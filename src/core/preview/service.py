from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.preview.artifacts import PreviewArtifactIntegrityError
from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    policy_from_tenant_config,
)
from core.preview.generator import PreviewGenerationError, PreviewPackageGenerator
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
    PreviewExpiredArtifact,
    PreviewRequestPage,
    PreviewStorageBackend,
)

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.artifacts import PreviewArchiveStream, PreviewArtifactStore

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
        artifact_store: PreviewArtifactStore,
        max_workers: int,
        max_csv_file_bytes: int | None = None,
        startup_at: datetime | None = None,
        configured_owner_keys: tuple[tuple[str, str, str], ...] = (),
        clock: Callable[[], datetime] = utc_now,
        request_id_factory: Callable[[], str] = new_uuid,
        executor: PreviewExecutor | None = None,
        lease_owner_id: str | None = None,
        package_generator: PreviewPackageGenerator | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._clock = clock
        self._max_csv_file_bytes = max_csv_file_bytes
        self._package_generator = package_generator or PreviewPackageGenerator(
            max_csv_file_bytes=max_csv_file_bytes,
            clock=clock,
        )
        process_start = startup_at if startup_at is not None else utc_now()
        if process_start.tzinfo is None or process_start.utcoffset() is None:
            raise ValueError("startup_at must be timezone-aware")
        self._startup_at = process_start.astimezone(UTC).replace(microsecond=0)
        self._staging_recovery_pending = True
        self._staging_recovery_lock = threading.Lock()
        self._owner_recovery_pending = set(configured_owner_keys)
        self._owner_recovery_locks: dict[tuple[str, str, str], threading.Lock] = {
            key: threading.Lock() for key in configured_owner_keys
        }
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
        self._lease_targets: dict[str, PreviewStorageBackend] = {}
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._owns_executor = executor is None
        self._executor: PreviewExecutor = executor or ThreadPoolExecutor(max_workers=max_workers)
        self._closed = False

    def ensure_staging_recovered(self) -> None:
        if not self._staging_recovery_pending:
            return
        with self._staging_recovery_lock:
            if not self._staging_recovery_pending:
                return
            try:
                self._artifact_store.cleanup_staging()
            except Exception as exc:
                logger.error(
                    "FOCUS Mapping Preview staging recovery failed error_type=%s",
                    type(exc).__name__,
                )
                raise PreviewRecoveryUnavailable("FOCUS Mapping Preview recovery is unavailable") from None
            self._staging_recovery_pending = False

    def ensure_owner_recovered(
        self,
        *,
        backend: PreviewStorageBackend,
        tenant_name: str,
        ecosystem: str,
        tenant_id: str,
    ) -> None:
        self.ensure_staging_recovered()
        key = (tenant_name, ecosystem, tenant_id)
        lock = self._owner_recovery_locks.setdefault(key, threading.Lock())
        with lock:
            startup_recovery_pending = key in self._owner_recovery_pending
            diagnostic = PreviewDiagnostic(
                "preview_generation_interrupted",
                "FOCUS Mapping Preview generation was interrupted before completion.",
                True,
            )
            try:
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
                self._owner_recovery_pending.remove(key)

    def _lease_expiry(self) -> datetime:
        return self._clock().astimezone(UTC) + _PREVIEW_LEASE_DURATION

    def _track_request(self, request_id: str, backend: PreviewStorageBackend) -> None:
        with self._lease_lock:
            self._lease_targets[request_id] = backend
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

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(_PREVIEW_HEARTBEAT_INTERVAL_SECONDS):
            with self._lease_lock:
                targets = tuple(self._lease_targets.items())
            for request_id, backend in targets:
                try:
                    with backend.create_preview_write_unit_of_work() as uow:
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
    ) -> PreviewRequest:
        if self._closed:
            raise PreviewWorkerUnavailable("preview runtime is closed")
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
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(
                request,
                worker_id=self._lease_owner_id,
                lease_expires_at=self._lease_expiry(),
            )
            uow.commit()
        self._track_request(request.request_id, backend)
        try:
            self._executor.submit(lambda: self._run_worker(backend, request, policy))
        except Exception as exc:
            logger.error(
                "FOCUS Mapping Preview worker scheduling failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            diagnostic = PreviewDiagnostic(
                "preview_worker_unavailable", "FOCUS Mapping Preview worker is unavailable.", True
            )
            self._mark_failed(backend, request.request_id, diagnostic)
            self._untrack_request(request.request_id)
            raise PreviewWorkerUnavailable("FOCUS Mapping Preview worker is unavailable") from exc
        return request

    def _run_worker(
        self,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
    ) -> None:
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
            snapshot, draft = self._package_generator.generate(
                backend=backend,
                request=running,
                policy=policy,
            )
            with self._artifact_store.stage_data_files(
                request_id=request.request_id,
                data_files=draft.data_files,
            ) as staged:
                ready_at = self._clock().astimezone(UTC).replace(microsecond=0)
                expires_at = ready_at + timedelta(days=7)
                manifest_body = build_requested_preview_manifest(
                    request=running,
                    snapshot=snapshot,
                    draft=draft,
                    files=staged.files,
                    ready_at=ready_at,
                    expires_at=expires_at,
                )
                stored = staged.publish(manifest_body=manifest_body)
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
        except PreviewGenerationError as exc:
            self._mark_failed(backend, request.request_id, exc.diagnostic)
        except _PreviewReadyTransitionError:
            logger.error(
                "FOCUS Mapping Preview ready transition rejected after artifact finalization request_id=%s",
                request.request_id,
            )
            self._mark_failed(
                backend,
                request.request_id,
                PreviewDiagnostic("preview_generation_failed", "FOCUS Mapping Preview generation failed.", True),
            )
        except Exception as exc:
            logger.error(
                "Unexpected FOCUS Mapping Preview worker failure tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            self._mark_failed(
                backend,
                request.request_id,
                PreviewDiagnostic("preview_generation_failed", "FOCUS Mapping Preview generation failed.", True),
            )
        finally:
            self._untrack_request(request.request_id)

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
        with backend.create_preview_read_unit_of_work() as uow:
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
        with backend.create_preview_read_unit_of_work() as uow:
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
                    artifact = uow.requests.expire_ready_request(
                        request_id=request_id,
                        ecosystem=ecosystem,
                        tenant_id=tenant_id,
                        now=now,
                    )
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

    def _verified_manifest_body(self, request: PreviewRequest) -> bytes:
        if request.storage_key is None or request.package is None:
            raise PreviewArtifactUnavailable("preview package is unavailable")
        body = self._artifact_store.read_manifest(request.storage_key, request.package.manifest)
        try:
            manifest = json.loads(body)
        except UnicodeDecodeError, json.JSONDecodeError:
            raise PreviewArtifactIntegrityError("stored preview manifest is invalid") from None
        if not isinstance(manifest, dict) or manifest.get("request_id") != request.request_id:
            raise PreviewArtifactIntegrityError("stored preview manifest identity is invalid")
        declarations = manifest.get("files")
        if not isinstance(declarations, list):
            raise PreviewArtifactIntegrityError("stored preview manifest file declarations are invalid")
        actual = tuple(declaration for declaration in declarations if isinstance(declaration, dict))
        expected = tuple(
            {
                "name": item.name,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "order": item.order,
            }
            for item in request.package.files
        )
        if len(actual) != len(declarations) or actual != expected:
            raise PreviewArtifactIntegrityError("stored preview manifest metadata is inconsistent")
        return body

    def read_manifest_bytes(self, request: PreviewRequest) -> bytes:
        try:
            return self._verified_manifest_body(request)
        except (OSError, ValueError) as exc:
            logger.error(
                "FOCUS Mapping Preview manifest read failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            raise PreviewArtifactUnavailable("preview package is unavailable") from None

    def read_file_bytes(self, request: PreviewRequest, file_name: str) -> bytes:
        if (
            request.storage_key is None
            or request.package is None
            or file_name not in {item.name for item in request.package.files}
        ):
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            self._verified_manifest_body(request)
            metadata = next(item for item in request.package.files if item.name == file_name)
            return self._artifact_store.read_file(request.storage_key, metadata)
        except (OSError, ValueError) as exc:
            logger.error(
                "FOCUS Mapping Preview file read failed tenant=%s request_id=%s error_type=%s",
                request.tenant_name,
                request.request_id,
                type(exc).__name__,
            )
            raise PreviewArtifactUnavailable("preview package is unavailable") from None

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
        if self._owns_executor:
            self._executor.shutdown(wait=wait, cancel_futures=False)
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=None if wait else 0)
