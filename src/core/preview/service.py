from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Literal, Protocol, overload, runtime_checkable

from core.preview.artifacts import (
    PreviewArtifactIntegrityError,
    PreviewArtifactOwner,
    PreviewRuntimeArtifactStore,
    find_preview_artifact_metadata,
    preview_artifact_owner,
)
from core.preview.capacity import (
    PreviewCapacityUnavailable,
    PreviewGenerationScheduler,
    PreviewRequestedReservation,
)
from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    policy_from_tenant_config,
)
from core.preview.focus_metadata import (
    build_requested_focus_metadata_artifact,
    compose_package_artifacts,
    validate_requested_focus_metadata_artifact,
)
from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator
from core.preview.manifest_validation import validate_requested_manifest
from core.preview.mapping import (
    build_requested_preview_manifest,
    validate_preview_effective_columns,
)
from core.preview.models import (
    PreviewColumnProfile,
    PreviewDiagnostic,
    PreviewGrain,
    PreviewRequest,
    PreviewRequestStatus,
    validate_preview_request_snapshot,
)
from core.preview.persistence import (
    PreviewEffectiveColumnsMetadataMissingError,
    PreviewExpiredArtifact,
    PreviewRequestPage,
    PreviewStorageBackend,
)
from core.preview.spooling import PreviewGenerationSpoolLimitError
from core.storage.backend_provider import (  # noqa: TC001 - public runtime constructor annotation is resolvable
    TenantBackendProvider,
)

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.artifacts import (
        PreviewArchiveStream,
        PreviewVerifiedArtifactStream,
    )

logger = logging.getLogger(__name__)
_PREVIEW_LEASE_DURATION = timedelta(seconds=30)
_PREVIEW_HEARTBEAT_INTERVAL_SECONDS = 10.0


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_uuid() -> str:
    return str(uuid.uuid4())


@runtime_checkable
class PreviewExecutor(Protocol):
    def submit(self, task: Callable[[], None]) -> Future[None]: ...
    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None: ...


class PreviewWorkerUnavailable(RuntimeError):  # noqa: N818 - stable design/API name
    pass


class PreviewArtifactUnavailable(FileNotFoundError):  # noqa: N818 - stable design/API name
    pass


class PreviewRecoveryUnavailable(RuntimeError):  # noqa: N818 - stable design/API name
    pass


class _PreviewReadyTransitionError(RuntimeError):
    def __init__(self, storage_key: str) -> None:
        super().__init__("preview ready transition was rejected after artifact finalization")
        self.storage_key = storage_key


class PreviewRuntime:
    def __init__(
        self,
        *,
        artifact_store: PreviewRuntimeArtifactStore,
        backend_provider: TenantBackendProvider,
        max_workers: int,
        max_queued_generations: int = 8,
        max_running_generations_per_tenant: int = 1,
        max_queued_generations_per_tenant: int = 2,
        max_generation_spool_bytes: int = 2_147_483_648,
        max_csv_file_bytes: int | None = None,
        startup_at: datetime | None = None,
        configured_owners: tuple[PreviewArtifactOwner, ...] = (),
        clock: Callable[[], datetime] = utc_now,
        request_id_factory: Callable[[], str] = new_uuid,
        executor: PreviewExecutor | None = None,
        scheduler: PreviewGenerationScheduler | None = None,
        lease_owner_id: str | None = None,
        package_generator: PreviewPackageGenerator | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._backend_provider = backend_provider
        self._clock = clock
        self._max_csv_file_bytes = max_csv_file_bytes
        self._max_generation_spool_bytes = max_generation_spool_bytes
        self._package_generator = package_generator or PreviewPackageGenerator(
            max_csv_file_bytes=max_csv_file_bytes,
            max_generation_spool_bytes=max_generation_spool_bytes,
            clock=clock,
        )
        process_start = startup_at if startup_at is not None else utc_now()
        if process_start.tzinfo is None or process_start.utcoffset() is None:
            raise ValueError("startup_at must be timezone-aware")
        self._startup_at = process_start.astimezone(UTC).replace(microsecond=0)
        owner_keys = tuple(self._owner_key(owner) for owner in configured_owners)
        self._owner_recovery_pending = set(owner_keys)
        self._owner_recovery_locks: dict[tuple[str, str, str], threading.Lock] = {
            key: threading.Lock() for key in owner_keys
        }
        self._recovery_state_lock = threading.Lock()
        self._local_terminalization_pending: dict[
            str,
            tuple[PreviewArtifactOwner, PreviewDiagnostic],
        ] = {}
        self._request_id_factory = request_id_factory
        self._lease_owner_id = lease_owner_id or uuid.uuid4().hex
        if (
            not self._lease_owner_id
            or self._lease_owner_id in {".", ".."}
            or "/" in self._lease_owner_id
            or "\\" in self._lease_owner_id
        ):
            raise ValueError("lease_owner_id must be a safe nonblank identifier")
        self._lease_lock = threading.Lock()
        self._lease_targets: dict[str, tuple[str, TenantConfig]] = {}
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        if scheduler is not None and executor is not None:
            raise ValueError("executor cannot be supplied with a shared scheduler")
        self._owns_scheduler = scheduler is None
        if scheduler is None:
            owns_executor = executor is None
            scheduler_executor = executor or ThreadPoolExecutor(max_workers=max_workers)
            scheduler = PreviewGenerationScheduler(
                max_workers=max_workers,
                max_queued_generations=max_queued_generations,
                max_running_generations_per_tenant=max_running_generations_per_tenant,
                max_queued_generations_per_tenant=max_queued_generations_per_tenant,
                executor=scheduler_executor,
                shutdown_executor=owns_executor,
            )
        self._scheduler = scheduler
        self._closed = False

    @staticmethod
    def _owner_key(owner: PreviewArtifactOwner) -> tuple[str, str, str]:
        return (owner.ecosystem, owner.tenant_id, owner.storage_backend_fingerprint)

    def ensure_owner_recovered(
        self,
        *,
        backend: PreviewStorageBackend,
        owner: PreviewArtifactOwner,
    ) -> None:
        key = self._owner_key(owner)
        tenant_name = owner.tenant_name
        ecosystem = owner.ecosystem
        tenant_id = owner.tenant_id
        lock = self._owner_recovery_locks.setdefault(key, threading.Lock())
        with lock:
            startup_recovery_pending = key in self._owner_recovery_pending
            diagnostic = PreviewDiagnostic(
                "preview_generation_interrupted",
                "FOCUS Mapping Preview generation was interrupted before completion.",
                True,
            )
            try:
                self._artifact_store.cleanup_staging(owner)
                self._retry_local_terminalization(backend=backend, owner=owner)
                with backend.create_preview_write_unit_of_work() as uow:
                    now = self._clock().astimezone(UTC)
                    startup_recovery = (
                        uow.requests.fail_interrupted_before(
                            ecosystem=ecosystem,
                            tenant_id=tenant_id,
                            startup_at=self._startup_at,
                            lease_stale_at=now,
                            diagnostic=diagnostic,
                        )
                        if startup_recovery_pending
                        else None
                    )
                    stale_recovery = uow.requests.fail_stale_foreign_leases(
                        ecosystem=ecosystem,
                        tenant_id=tenant_id,
                        current_worker_id=self._lease_owner_id,
                        lease_stale_at=now,
                        limit=100,
                        diagnostic=diagnostic,
                    )
                    if startup_recovery_pending or stale_recovery.failed_count > 0:
                        uow.commit()
                if startup_recovery_pending:
                    self.reconcile_expiry(backend=backend, ecosystem=ecosystem, tenant_id=tenant_id)
                self._reconcile_finalized(backend=backend, owner=owner)
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview owner recovery failed tenant=%s ecosystem=%s tenant_id=%s error_type=%s",
                    tenant_name,
                    ecosystem,
                    tenant_id,
                    type(exc).__name__,
                )
                raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable") from None
            if startup_recovery_pending and startup_recovery is not None and startup_recovery.protected_count == 0:
                self._owner_recovery_pending.discard(key)

    def _retry_local_terminalization(
        self,
        *,
        backend: PreviewStorageBackend,
        owner: PreviewArtifactOwner,
    ) -> None:
        key = self._owner_key(owner)
        with self._recovery_state_lock:
            candidates = tuple(
                (request_id, diagnostic)
                for request_id, (pending_owner, diagnostic) in self._local_terminalization_pending.items()
                if self._owner_key(pending_owner) == key
            )
        with self._lease_lock:
            active_request_ids = frozenset(self._lease_targets)
        for request_id, diagnostic in candidates:
            if request_id in active_request_ids:
                continue
            if self._mark_failed(backend, request_id, diagnostic):
                with self._recovery_state_lock:
                    self._local_terminalization_pending.pop(request_id, None)
                continue
            with backend.create_preview_metadata_read_unit_of_work() as read_uow:
                request = read_uow.requests.get_for_owner(
                    request_id,
                    owner.ecosystem,
                    owner.tenant_id,
                )
            if request is not None and request.status in {
                PreviewRequestStatus.READY,
                PreviewRequestStatus.FAILED,
                PreviewRequestStatus.EXPIRED,
            }:
                with self._recovery_state_lock:
                    self._local_terminalization_pending.pop(request_id, None)
                continue
            raise RuntimeError("preview request terminalization remains pending")

    def _reconcile_finalized(
        self,
        *,
        backend: PreviewStorageBackend,
        owner: PreviewArtifactOwner,
    ) -> None:
        with backend.create_preview_metadata_read_unit_of_work() as read_uow:
            references = frozenset(
                read_uow.artifact_references.list_for_owner(
                    ecosystem=owner.ecosystem,
                    tenant_id=owner.tenant_id,
                )
            )

        def is_referenced(storage_key: str) -> bool:
            with backend.create_preview_metadata_read_unit_of_work() as read_uow:
                return read_uow.artifact_references.is_referenced(
                    ecosystem=owner.ecosystem,
                    tenant_id=owner.tenant_id,
                    storage_key=storage_key,
                )

        self._artifact_store.reconcile_finalized(
            owner=owner,
            referenced_storage_keys=references,
            is_referenced=is_referenced,
        )

    def _remember_terminalization(
        self,
        *,
        request_id: str,
        owner: PreviewArtifactOwner,
        diagnostic: PreviewDiagnostic,
    ) -> None:
        with self._recovery_state_lock:
            self._local_terminalization_pending[request_id] = (owner, diagnostic)
            self._owner_recovery_pending.add(self._owner_key(owner))

    def _lease_expiry(self) -> datetime:
        return self._clock().astimezone(UTC) + _PREVIEW_LEASE_DURATION

    def _track_request(
        self,
        request_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None:
        with self._lease_lock:
            self._lease_targets[request_id] = (tenant_name, tenant_config)
            if self._heartbeat_thread is None:
                self._heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    name="preview-lease-heartbeat",
                    daemon=True,
                )
                self._heartbeat_thread.start()

    def _untrack_request(self, request_id: str) -> None:
        with self._lease_lock:
            self._lease_targets.pop(request_id, None)
            if self._closed and not self._lease_targets:
                self._heartbeat_stop.set()

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(_PREVIEW_HEARTBEAT_INTERVAL_SECONDS):
            with self._lease_lock:
                targets = tuple(self._lease_targets.items())
            for request_id, (tenant_name, tenant_config) in targets:
                try:
                    with self._backend_provider.acquire_backend(tenant_name, tenant_config) as leased_backend:
                        if not isinstance(leased_backend, PreviewStorageBackend):
                            raise TypeError("tenant backend does not support Preview storage")
                        with leased_backend.create_preview_write_unit_of_work() as uow:
                            renewed = uow.requests.renew_lease(
                                request_id,
                                self._lease_owner_id,
                                self._lease_expiry(),
                            )
                            uow.commit()
                except Exception as exc:
                    logger.error(
                        "FOCUS Mapping Preview lease heartbeat failed request_id=%s error_type=%s",
                        request_id,
                        type(exc).__name__,
                    )
                    continue
                if not renewed:
                    self._untrack_request(request_id)

    def submit(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        start_date: date,
        end_date: date,
        grain: PreviewGrain,
        column_profile: PreviewColumnProfile,
        effective_columns: tuple[str, ...],
        reservation: PreviewRequestedReservation | None = None,
    ) -> PreviewRequest:
        if self._closed:
            raise PreviewWorkerUnavailable("preview runtime is closed")
        owner = preview_artifact_owner(tenant_name, tenant_config)
        requested_reservation = reservation or self.reserve_requested(owner=owner)
        created_at = self._clock().astimezone(UTC).replace(microsecond=0)
        policy = policy_from_tenant_config(tenant_config, created_at=created_at)
        validate_preview_effective_columns(column_profile, effective_columns)
        request = PreviewRequest(
            request_id=self._request_id_factory(),
            tenant_name=tenant_name,
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            grain=grain,
            start_date=start_date,
            end_date=end_date,
            column_profile=column_profile,
            status=PreviewRequestStatus.QUEUED,
            created_at=created_at,
            started_at=None,
            completed_at=None,
            expires_at=None,
            source_snapshot=None,
            diagnostic=None,
            storage_key=None,
            package=None,
            effective_columns=effective_columns,
        )
        validate_preview_request_snapshot(
            request=request,
            snapshot=request.source_snapshot,
            resulting_status=request.status,
            mode="strict_materialized",
        )
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                uow.requests.create_queued(
                    request,
                    worker_id=self._lease_owner_id,
                    lease_expires_at=self._lease_expiry(),
                )
                uow.commit()
        except BaseException:
            requested_reservation.cancel()
            raise
        self._track_request(request.request_id, tenant_name, tenant_config)
        try:
            requested_reservation.attach(
                work_id=request.request_id,
                run=lambda: self._run_worker(request, policy, tenant_config),
                on_cancel=lambda: self._cancel_waiting_request(request, tenant_config),
            )
        except Exception as exc:
            scheduling_error: BaseException = exc
            while scheduling_error.__cause__ is not None:
                scheduling_error = scheduling_error.__cause__
            logger.error(
                "FOCUS Mapping Preview worker scheduling failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(scheduling_error).__name__,
            )
            diagnostic = PreviewDiagnostic(
                "preview_worker_unavailable", "FOCUS Mapping Preview worker is unavailable.", True
            )
            if not self._mark_failed(backend, request.request_id, diagnostic):
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=preview_artifact_owner(tenant_name, tenant_config),
                    diagnostic=diagnostic,
                )
            self._untrack_request(request.request_id)
            raise PreviewWorkerUnavailable("FOCUS Mapping Preview worker is unavailable") from exc
        return request

    def _cancel_waiting_request(
        self,
        request: PreviewRequest,
        tenant_config: TenantConfig,
    ) -> None:
        diagnostic = PreviewDiagnostic(
            "preview_generation_interrupted",
            "FOCUS Mapping Preview generation was interrupted before completion.",
            True,
        )
        try:
            if not self._mark_failed_with_lease(request, tenant_config, diagnostic):
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=preview_artifact_owner(request.tenant_name, tenant_config),
                    diagnostic=diagnostic,
                )
        finally:
            self._untrack_request(request.request_id)

    def reserve_requested(
        self,
        *,
        owner: PreviewArtifactOwner,
    ) -> PreviewRequestedReservation:
        if self._closed:
            raise PreviewCapacityUnavailable
        return self._scheduler.reserve_requested(owner=owner)

    def _run_worker(
        self,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
        tenant_config: TenantConfig,
    ) -> None:
        owner = preview_artifact_owner(request.tenant_name, tenant_config)
        try:
            with self._backend_provider.acquire_backend(request.tenant_name, tenant_config) as leased_backend:
                if not isinstance(leased_backend, PreviewStorageBackend):
                    raise TypeError("tenant backend does not support Preview storage")
                self._run_worker_with_backend(leased_backend, request, policy, owner)
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview backend lease failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            marked_failed = self._mark_failed_with_lease(
                request,
                tenant_config,
                PreviewDiagnostic("preview_generation_failed", "FOCUS Mapping Preview generation failed.", True),
            )
            if not marked_failed:
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=owner,
                    diagnostic=PreviewDiagnostic(
                        "preview_generation_failed",
                        "FOCUS Mapping Preview generation failed.",
                        True,
                    ),
                )
        finally:
            self._untrack_request(request.request_id)

    def _run_worker_with_backend(
        self,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
        owner: PreviewArtifactOwner,
    ) -> None:
        stored = None
        draft = None
        ready_committed = False
        failure_diagnostic = PreviewDiagnostic(
            "preview_generation_failed",
            "FOCUS Mapping Preview generation failed.",
            True,
        )
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                running = uow.requests.mark_running(
                    request.request_id,
                    self._clock().astimezone(UTC).replace(microsecond=0),
                    worker_id=self._lease_owner_id,
                    lease_expires_at=self._lease_expiry(),
                )
                if running is None:
                    return
                uow.commit()
            try:
                with self._artifact_store.begin_generation(
                    owner=owner,
                    request_id=request.request_id,
                    max_spool_bytes=self._max_generation_spool_bytes,
                ) as generation:
                    snapshot, draft = self._package_generator.generate(
                        backend=backend,
                        request=running,
                        policy=policy,
                        workspace=generation.workspace,
                    )
                    data_files = tuple(draft.data_files)
                    generation.stage_data_files(data_files)
                    ready_at = self._clock().astimezone(UTC).replace(microsecond=0)
                    expires_at = ready_at + timedelta(days=7)
                    focus_metadata = build_requested_focus_metadata_artifact(
                        request=running,
                        snapshot=snapshot,
                        draft=draft,
                        data_files=data_files,
                        ready_at=ready_at,
                        expires_at=expires_at,
                    )
                    package_files = compose_package_artifacts(
                        data_files=data_files,
                        focus_metadata=focus_metadata,
                    )
                    generation.stage_metadata_file(focus_metadata)
                    staged_files = generation.files
                    validate_requested_focus_metadata_artifact(
                        request=running,
                        snapshot=snapshot,
                        draft=draft,
                        package_files=package_files,
                        staged_files=staged_files,
                        ready_at=ready_at,
                        expires_at=expires_at,
                    )
                    manifest_body = build_requested_preview_manifest(
                        request=running,
                        snapshot=snapshot,
                        draft=draft,
                        files=staged_files,
                        ready_at=ready_at,
                        expires_at=expires_at,
                    )
                    stored = generation.publish(manifest_body=manifest_body)
                    with backend.create_preview_write_unit_of_work() as uow:
                        if not uow.requests.mark_ready(
                            request.request_id,
                            ready_at,
                            expires_at,
                            snapshot,
                            stored,
                            worker_id=self._lease_owner_id,
                        ):
                            raise _PreviewReadyTransitionError(stored.storage_key)
                        uow.commit()
                        ready_committed = True
            except PreviewGenerationSpoolLimitError:
                raise PreviewGenerationError(
                    PreviewDiagnostic(
                        "preview_generation_spool_limit_exceeded",
                        "FOCUS Mapping Preview package exceeds the configured generation spool limit.",
                        False,
                    )
                ) from None
        except PreviewGenerationError as exc:
            if not self._mark_failed(backend, request.request_id, exc.diagnostic):
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=owner,
                    diagnostic=exc.diagnostic,
                )
        except _PreviewReadyTransitionError:
            logger.error(
                "FOCUS Mapping Preview ready transition rejected after artifact finalization request_id=%s",
                request.request_id,
            )
            if not self._mark_failed(backend, request.request_id, failure_diagnostic):
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=owner,
                    diagnostic=failure_diagnostic,
                )
        except Exception as exc:
            logger.error(
                "Unexpected FOCUS Mapping Preview worker failure tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            if ready_committed:
                with self._recovery_state_lock:
                    self._owner_recovery_pending.add(self._owner_key(owner))
            elif not self._mark_failed(backend, request.request_id, failure_diagnostic):
                self._remember_terminalization(
                    request_id=request.request_id,
                    owner=owner,
                    diagnostic=failure_diagnostic,
                )
        finally:
            if draft is not None:
                try:
                    draft.close()
                except OSError:
                    logger.exception(
                        "FOCUS Mapping Preview generation workspace cleanup failed request_id=%s",
                        request.request_id,
                    )
                    with self._recovery_state_lock:
                        self._owner_recovery_pending.add(self._owner_key(owner))
            if stored is not None and not ready_committed:
                with self._recovery_state_lock:
                    self._owner_recovery_pending.add(self._owner_key(owner))

    def _mark_failed_with_lease(
        self,
        request: PreviewRequest,
        tenant_config: TenantConfig,
        diagnostic: PreviewDiagnostic,
    ) -> bool:
        try:
            with self._backend_provider.acquire_backend(request.tenant_name, tenant_config) as leased_backend:
                if not isinstance(leased_backend, PreviewStorageBackend):
                    return False
                return self._mark_failed(leased_backend, request.request_id, diagnostic)
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview failure backend lease failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return False

    def _mark_failed(self, backend: PreviewStorageBackend, request_id: str, diagnostic: PreviewDiagnostic) -> bool:
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                if not uow.requests.mark_failed(
                    request_id,
                    self._clock().astimezone(UTC).replace(microsecond=0),
                    diagnostic,
                    worker_id=self._lease_owner_id,
                ):
                    logger.error(
                        "FOCUS Mapping Preview failure transition rejected request_id=%s diagnostic_code=%s",
                        request_id,
                        diagnostic.code,
                    )
                    return False
                uow.commit()
            return True
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview failure persistence failed request_id=%s error_type=%s",
                request_id,
                type(exc).__name__,
            )
            return False

    def get_request(
        self, *, backend: PreviewStorageBackend, request_id: str, ecosystem: str, tenant_id: str
    ) -> PreviewRequest | None:
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            return uow.requests.get_for_owner(request_id, ecosystem, tenant_id)

    def list_recent_requests(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        cursor_request_id: str | None,
    ) -> PreviewRequestPage:
        self.reconcile_expiry(backend=backend, ecosystem=ecosystem, tenant_id=tenant_id)
        with backend.create_preview_metadata_read_unit_of_work() as uow:
            return uow.requests.list_recent_for_owner(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                limit=limit,
                cursor_request_id=cursor_request_id,
            )

    def reconcile_expiry(
        self,
        *,
        backend: PreviewStorageBackend,
        ecosystem: str,
        tenant_id: str,
        request_id: str | None = None,
    ) -> None:
        now = self._clock().astimezone(UTC)
        artifacts: list[PreviewExpiredArtifact] = []
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                if request_id is None:
                    while batch := uow.requests.expire_ready_due(
                        ecosystem=ecosystem,
                        tenant_id=tenant_id,
                        now=now,
                        limit=100,
                    ):
                        artifacts.extend(batch)
                        if len(batch) < 100:
                            break
                    artifacts.extend(
                        uow.requests.list_expired_artifacts(
                            ecosystem=ecosystem,
                            tenant_id=tenant_id,
                            limit=100,
                        )
                    )
                else:
                    try:
                        artifact = uow.requests.expire_ready_request(
                            request_id=request_id,
                            ecosystem=ecosystem,
                            tenant_id=tenant_id,
                            now=now,
                        )
                    except PreviewEffectiveColumnsMetadataMissingError:
                        return
                    if artifact is not None:
                        artifacts.append(artifact)
                    else:
                        current = uow.requests.get_for_owner(request_id, ecosystem, tenant_id)
                        if (
                            current is not None
                            and current.status is PreviewRequestStatus.EXPIRED
                            and current.storage_key is not None
                        ):
                            artifacts.append(PreviewExpiredArtifact(current.request_id, current.storage_key))
                uow.commit()
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview expiry persistence failed ecosystem=%s tenant_id=%s "
                "request_id=%s stage=transition error_type=%s",
                ecosystem,
                tenant_id,
                request_id or "all",
                type(exc).__name__,
            )
            raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable") from None
        unique = {(item.request_id, item.storage_key): item for item in artifacts}
        for artifact in unique.values():
            try:
                self._artifact_store.delete_package(storage_key=artifact.storage_key)
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview expired artifact cleanup failed ecosystem=%s tenant_id=%s "
                    "request_id=%s error_type=%s",
                    ecosystem,
                    tenant_id,
                    artifact.request_id,
                    type(exc).__name__,
                )
                continue
            try:
                with backend.create_preview_write_unit_of_work() as uow:
                    uow.requests.clear_expired_storage_key(artifact.request_id, artifact.storage_key)
                    uow.commit()
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview expiry persistence failed ecosystem=%s tenant_id=%s "
                    "request_id=%s stage=clear_storage_key error_type=%s",
                    ecosystem,
                    tenant_id,
                    artifact.request_id,
                    type(exc).__name__,
                )
                raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable") from None

    def _open_verified_manifest(self, request: PreviewRequest) -> PreviewVerifiedArtifactStream:
        if request.storage_key is None or request.package is None:
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            stream = self._artifact_store.open_verified(request.storage_key, request.package.manifest)
            try:
                validate_requested_manifest(stream, request)
            except BaseException:
                stream.close()
                raise
            return stream
        except OSError:
            raise
        except ValueError as exc:
            raise PreviewArtifactIntegrityError("stored preview manifest is invalid") from exc

    def _verified_manifest_body(self, request: PreviewRequest) -> bytes:
        with self._open_verified_manifest(request) as stream:
            return b"".join(stream.iter_chunks())

    @overload
    def read_manifest_bytes(self, request: PreviewRequest) -> bytes: ...

    @overload
    def read_manifest_bytes(
        self,
        request: PreviewRequest,
        *,
        stream: Literal[True],
    ) -> PreviewVerifiedArtifactStream: ...

    def read_manifest_bytes(
        self,
        request: PreviewRequest,
        *,
        stream: bool = False,
    ) -> bytes | PreviewVerifiedArtifactStream:
        try:
            opened = self._open_verified_manifest(request)
            if stream:
                return opened
            with opened:
                return b"".join(opened.iter_chunks())
        except (OSError, ValueError) as exc:
            logger.error(
                "FOCUS Mapping Preview manifest read failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            raise PreviewArtifactUnavailable("preview package is unavailable") from None

    @overload
    def read_file_bytes(self, request: PreviewRequest, file_name: str) -> bytes: ...

    @overload
    def read_file_bytes(
        self,
        request: PreviewRequest,
        file_name: str,
        *,
        stream: Literal[True],
    ) -> PreviewVerifiedArtifactStream: ...

    def read_file_bytes(
        self,
        request: PreviewRequest,
        file_name: str,
        *,
        stream: bool = False,
    ) -> bytes | PreviewVerifiedArtifactStream:
        opened = self._open_file_stream(request, file_name)
        if stream:
            return opened
        with opened:
            return b"".join(opened.iter_chunks())

    def open_manifest_stream(self, request: PreviewRequest) -> PreviewVerifiedArtifactStream:
        return self.read_manifest_bytes(request, stream=True)

    def _open_file_stream(
        self,
        request: PreviewRequest,
        file_name: str,
    ) -> PreviewVerifiedArtifactStream:
        if (
            request.storage_key is None
            or request.package is None
            or find_preview_artifact_metadata(request.package.files, file_name) is None
        ):
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            with self._open_verified_manifest(request):
                pass
            metadata = find_preview_artifact_metadata(request.package.files, file_name)
            assert metadata is not None
            return self._artifact_store.open_verified(request.storage_key, metadata)
        except (OSError, ValueError) as exc:
            logger.error(
                "FOCUS Mapping Preview file read failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            raise PreviewArtifactUnavailable("preview package is unavailable") from None

    def open_file_stream(
        self,
        request: PreviewRequest,
        file_name: str,
    ) -> PreviewVerifiedArtifactStream:
        return self.read_file_bytes(request, file_name, stream=True)

    def open_archive(self, request: PreviewRequest) -> PreviewArchiveStream:
        if request.storage_key is None or request.package is None:
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            self._verified_manifest_body(request)
            return self._artifact_store.open_archive(
                storage_key=request.storage_key,
                manifest=request.package.manifest,
                files=request.package.files,
            )
        except (OSError, ValueError) as exc:
            logger.error(
                "FOCUS Mapping Preview archive build failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            raise PreviewArtifactUnavailable("preview package is unavailable") from None

    def close(self, *, wait: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        scheduler_error: BaseException | None = None
        try:
            if self._owns_scheduler:
                self._scheduler.close(wait=wait)
            elif wait:
                self._scheduler.wait_idle()
        except BaseException as exc:
            scheduler_error = exc
        finally:
            with self._lease_lock:
                if wait or not self._lease_targets:
                    self._heartbeat_stop.set()
            if wait and self._heartbeat_thread is not None:
                self._heartbeat_thread.join()
        if scheduler_error is not None:
            raise scheduler_error
