from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from core.preview.evidence import PreviewEvidenceScope
from core.preview.mapping import (
    PreviewMappingError,
    PreviewReconciliationError,
    PreviewSourceSnapshotError,
    PreviewTracerScopeError,
    build_daily_full_package,
    source_through,
    validate_daily_full_mapping,
    validate_daily_full_source,
)
from core.preview.models import (
    PreviewDiagnostic,
    PreviewPackagePayload,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
)
from core.preview.persistence import (
    CompleteCalculationCoverage,
    NoUsableCalculationCoverage,
    PartialCalculationCoverage,
    PreviewStorageBackend,
)

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.artifacts import PreviewArtifactStore

logger = logging.getLogger(__name__)


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


class _PreviewFailureError(Exception):
    def __init__(self, diagnostic: PreviewDiagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


def _failure(code: str, message: str, retryable: bool = False) -> _PreviewFailureError:
    return _PreviewFailureError(PreviewDiagnostic(code, message, retryable))


def _mapping_failure(error: PreviewMappingError) -> _PreviewFailureError:
    if isinstance(error, PreviewSourceSnapshotError):
        return _failure("preview_source_snapshot_incomplete", "Persisted preview source evidence is incomplete.")
    if isinstance(error, PreviewReconciliationError):
        return _failure("preview_reconciliation_failed", "Persisted preview evidence does not reconcile.")
    assert isinstance(error, PreviewTracerScopeError)
    return _failure("daily_full_tracer_scope_unsupported", "Persisted evidence is outside the Daily Full tracer scope.")


class PreviewRuntime:
    def __init__(
        self,
        *,
        artifact_store: PreviewArtifactStore,
        max_workers: int,
        clock: Callable[[], datetime] = utc_now,
        request_id_factory: Callable[[], str] = new_uuid,
        executor: PreviewExecutor | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._clock = clock
        self._request_id_factory = request_id_factory
        self._owns_executor = executor is None
        self._executor: PreviewExecutor = executor or ThreadPoolExecutor(max_workers=max_workers)
        self._closed = False

    def submit(
        self,
        *,
        tenant_name: str,
        tenant_config: TenantConfig,
        backend: PreviewStorageBackend,
        start_date: date,
        end_date: date,
        grain: Literal["daily"],
        column_profile: Literal["full"],
    ) -> PreviewRequest:
        if self._closed:
            raise PreviewWorkerUnavailable("preview runtime is closed")
        created_at = self._clock()
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
            source_snapshot=None,
            diagnostic=None,
            storage_key=None,
            package=None,
        )
        with backend.create_preview_write_unit_of_work() as uow:
            uow.requests.create_queued(request)
            uow.commit()
        try:
            self._executor.submit(lambda: self._run_worker(backend, request))
        except Exception as exc:
            logger.exception("FOCUS Mapping Preview worker scheduling failed request_id=%s", request.request_id)
            diagnostic = PreviewDiagnostic(
                "preview_worker_unavailable", "FOCUS Mapping Preview worker is unavailable.", True
            )
            with backend.create_preview_write_unit_of_work() as uow:
                uow.requests.mark_failed(request.request_id, self._clock(), diagnostic)
                uow.commit()
            raise PreviewWorkerUnavailable("FOCUS Mapping Preview worker is unavailable") from exc
        return request

    def _run_worker(self, backend: PreviewStorageBackend, request: PreviewRequest) -> None:
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                if not uow.requests.mark_running(request.request_id, self._clock()):
                    return
                uow.commit()
            snapshot, package = self._generate(backend, request)
            stored = self._artifact_store.finalize_package(request_id=request.request_id, package=package)
            with backend.create_preview_write_unit_of_work() as uow:
                uow.requests.mark_ready(request.request_id, self._clock(), snapshot, stored)
                uow.commit()
        except _PreviewFailureError as exc:
            self._mark_failed(backend, request.request_id, exc.diagnostic)
        except Exception:
            logger.exception("Unexpected FOCUS Mapping Preview worker failure request_id=%s", request.request_id)
            self._mark_failed(
                backend,
                request.request_id,
                PreviewDiagnostic("preview_generation_failed", "FOCUS Mapping Preview generation failed.", True),
            )

    def _mark_failed(self, backend: PreviewStorageBackend, request_id: str, diagnostic: PreviewDiagnostic) -> None:
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                uow.requests.mark_failed(request_id, self._clock(), diagnostic)
                uow.commit()
        except Exception:
            logger.exception("FOCUS Mapping Preview failure persistence failed request_id=%s", request_id)
            raise

    def _generate(
        self, backend: PreviewStorageBackend, request: PreviewRequest
    ) -> tuple[PreviewSourceSnapshot, PreviewPackagePayload]:
        start = datetime.combine(request.start_date, datetime.min.time(), tzinfo=UTC)
        end = datetime.combine(request.end_date, datetime.min.time(), tzinfo=UTC)
        scope = PreviewEvidenceScope(request.ecosystem, request.tenant_id, start, end)
        with backend.create_preview_read_unit_of_work() as uow:
            coverage = uow.calculations.find_current_coverage(
                ecosystem=request.ecosystem,
                tenant_id=request.tenant_id,
                start_date=request.start_date,
                end_date=request.end_date,
            )
            incomplete = getattr(coverage, "incomplete_correlation_dates", ())
            if incomplete:
                raise _failure(
                    "calculation_metadata_unavailable", "One or more requested dates lack preview calculation metadata."
                )
            if isinstance(coverage, NoUsableCalculationCoverage):
                raise _failure(
                    "calculation_unavailable",
                    "No successful persisted calculation is available for the requested dates; "
                    "run the pipeline and retry.",
                    True,
                )
            if isinstance(coverage, PartialCalculationCoverage):
                raise _failure(
                    "calculation_coverage_incomplete",
                    "No successful persisted calculation covers every requested date; run the pipeline and retry.",
                    True,
                )
            assert isinstance(coverage, CompleteCalculationCoverage)
            sources = uow.cost_evidence.find_preview_source_candidates(scope)
            if len(sources) != 1:
                raise _failure(
                    "daily_full_tracer_scope_unsupported", "Persisted evidence is outside the Daily Full tracer scope."
                )
            source = sources[0]
            try:
                validate_daily_full_source(request_start=start, request_end=end, source=source)
            except PreviewMappingError as exc:
                raise _mapping_failure(exc) from exc
            aggregates = uow.cost_evidence.find_preview_aggregate_candidates(scope, source)
            allocations = uow.allocation_evidence.find_preview_allocation_candidates(scope, source)
            if len(aggregates) != 1 or len(allocations) != 1:
                raise _failure(
                    "daily_full_tracer_scope_unsupported", "Persisted evidence is outside the Daily Full tracer scope."
                )
            aggregate, allocation = aggregates[0], allocations[0]
            try:
                validate_daily_full_mapping(
                    request_start=start,
                    request_end=end,
                    source=source,
                    aggregate=aggregate,
                    allocation=allocation,
                )
            except PreviewMappingError as exc:
                raise _mapping_failure(exc) from exc
            assert source.resource_id is not None
            resource = uow.resources.get(request.ecosystem, request.tenant_id, source.resource_id)
            environment = (
                uow.resources.get(request.ecosystem, request.tenant_id, source.environment_id)
                if source.environment_id is not None
                else None
            )
            identity = uow.identities.get(request.ecosystem, request.tenant_id, allocation.allocation_target_id)
            if resource is None or identity is None:
                raise _failure("preview_source_snapshot_incomplete", "Persisted preview source evidence is incomplete.")
            try:
                snapshot = PreviewSourceSnapshot(
                    calculation_timestamp=max(entry.calculation_completed_at for entry in coverage.entries),
                    calculation_coverage=coverage.entries,
                    source_through=source_through(source),
                )
                package = build_daily_full_package(
                    request=request,
                    snapshot=snapshot,
                    source=source,
                    aggregate=aggregate,
                    allocation=allocation,
                    resource=resource,
                    identity=identity,
                    environment=environment,
                    generated_at=self._clock(),
                )
            except PreviewMappingError as exc:
                raise _mapping_failure(exc) from exc
        return snapshot, package

    def get_request(
        self, *, backend: PreviewStorageBackend, request_id: str, ecosystem: str, tenant_id: str
    ) -> PreviewRequest | None:
        with backend.create_preview_read_unit_of_work() as uow:
            return uow.requests.get_for_owner(request_id, ecosystem, tenant_id)

    def read_manifest_bytes(self, request: PreviewRequest) -> bytes:
        if request.storage_key is None or request.package is None:
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            return self._artifact_store.read_manifest(request.storage_key)
        except (OSError, ValueError) as exc:
            logger.exception("FOCUS Mapping Preview manifest read failed request_id=%s", request.request_id)
            raise PreviewArtifactUnavailable("preview package is unavailable") from exc

    def read_file_bytes(self, request: PreviewRequest, file_name: str) -> bytes:
        if (
            request.storage_key is None
            or request.package is None
            or file_name not in {item.name for item in request.package.files}
        ):
            raise PreviewArtifactUnavailable("preview package is unavailable")
        try:
            return self._artifact_store.read_file(request.storage_key, file_name)
        except (OSError, ValueError) as exc:
            logger.exception("FOCUS Mapping Preview file read failed request_id=%s", request.request_id)
            raise PreviewArtifactUnavailable("preview package is unavailable") from exc

    def close(self, *, wait: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_executor:
            self._executor.shutdown(wait=wait, cancel_futures=False)
