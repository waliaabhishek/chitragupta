from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    capped_correlations,
    policy_from_tenant_config,
    public_source_correlation_id,
    request_eligibility_diagnostic,
    source_issue_diagnostic,
)
from core.preview.evidence import PreviewAggregateEvidence, PreviewEvidenceScope, PreviewSourceEvidence
from core.preview.mapping import (
    PreviewMappingError,
    PreviewReconciliationError,
    PreviewSourceIssue,
    PreviewSourceSnapshotError,
    PreviewTracerScopeError,
    build_daily_full_package,
    classify_daily_full_source,
    source_through,
    validate_daily_full_mapping,
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


def _failure(
    code: str,
    message: str,
    retryable: bool = False,
    source_correlation_ids: tuple[str, ...] = (),
) -> _PreviewFailureError:
    return _PreviewFailureError(PreviewDiagnostic(code, message, retryable, source_correlation_ids))


def _mapping_failure(
    error: PreviewMappingError,
    source_correlation_ids: tuple[str, ...],
) -> _PreviewFailureError:
    if isinstance(error, PreviewSourceSnapshotError):
        return _failure(
            "preview_source_record_incomplete",
            "One or more source records lack required Preview evidence.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewReconciliationError):
        return _failure(
            "preview_source_reconciliation_failed",
            "Persisted source, aggregate, or allocation evidence does not reconcile.",
            source_correlation_ids=source_correlation_ids,
        )
    assert isinstance(error, PreviewTracerScopeError)
    if "unallocated" in str(error):
        return _failure(
            "preview_mapping_scope_unsupported",
            "The complete source set exceeds the current Daily Full mapping scope.",
            source_correlation_ids=source_correlation_ids,
        )
    return _failure(
        "preview_source_reconciliation_failed",
        "Persisted source, aggregate, or allocation evidence does not reconcile.",
        source_correlation_ids=source_correlation_ids,
    )


_ISSUE_PRECEDENCE = {
    PreviewSourceIssue.RECORD_MALFORMED: 0,
    PreviewSourceIssue.SCOPE_UNSUPPORTED: 1,
    PreviewSourceIssue.CHARGE_CLASSIFICATION_AMBIGUOUS: 2,
    PreviewSourceIssue.LINE_TYPE_UNKNOWN: 3,
    PreviewSourceIssue.MAPPING_UNAVAILABLE: 4,
    PreviewSourceIssue.LINE_TYPE_UNSUPPORTED: 5,
    PreviewSourceIssue.RECORD_INCOMPLETE: 6,
    PreviewSourceIssue.ECONOMICS_UNSUPPORTED: 7,
    PreviewSourceIssue.RECONCILIATION_FAILED: 8,
}


def _source_origin(source: PreviewSourceEvidence) -> tuple[datetime, str, str, str, str]:
    assert source.environment_id is not None
    assert source.resource_id is not None
    assert source.native_product is not None
    assert source.native_line_type is not None
    return (
        source.allocation_timestamp,
        source.environment_id,
        source.resource_id,
        source.native_product,
        source.native_line_type,
    )


def _aggregate_origin(aggregate: PreviewAggregateEvidence) -> tuple[datetime, str, str, str, str]:
    return (
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
    )


def _coverage_matches(
    source_rows: Iterator[PreviewSourceEvidence],
    aggregate_rows: Iterator[PreviewAggregateEvidence],
) -> bool:
    source_keys = (_source_origin(row) for row in source_rows)
    aggregate_keys = (_aggregate_origin(row) for row in aggregate_rows)
    source_key = next(source_keys, None)
    aggregate_key = next(aggregate_keys, None)
    matched_any = False
    matches = True
    while source_key is not None or aggregate_key is not None:
        if source_key is None:
            matches = False
            aggregate_key = next(aggregate_keys, None)
            continue
        if aggregate_key is None:
            matches = False
            source_key = next(source_keys, None)
            continue
        if source_key == aggregate_key:
            matched_any = True
            current = source_key
            while source_key == current:
                source_key = next(source_keys, None)
            while aggregate_key == current:
                aggregate_key = next(aggregate_keys, None)
            continue
        matches = False
        if source_key < aggregate_key:
            source_key = next(source_keys, None)
        else:
            aggregate_key = next(aggregate_keys, None)
    return matches and matched_any


def _calculation_failure(
    coverage: NoUsableCalculationCoverage | PartialCalculationCoverage,
    policy: PreviewEligibilityPolicy,
) -> _PreviewFailureError:
    if coverage.incomplete_correlation_dates:
        return _failure(
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
        )
    if any(value < policy.acquisition_start_date for value in coverage.missing_dates):
        return _failure(
            "calculation_before_acquisition_lookback",
            "Required retained calculation evidence is unavailable outside the current acquisition window.",
        )
    if any(value >= policy.acquisition_end_date for value in coverage.missing_dates):
        return _failure(
            "calculation_pending_cutoff_window",
            "One or more requested dates are still inside the configured acquisition cutoff window; "
            "wait for the dates to enter the acquisition window, run the pipeline, and retry.",
            True,
        )
    if isinstance(coverage, NoUsableCalculationCoverage):
        return _failure(
            "calculation_unavailable",
            "No successful persisted calculation is available for the requested dates; run the pipeline and retry.",
            True,
        )
    return _failure(
        "calculation_coverage_incomplete",
        "No successful persisted calculation covers every requested date; run the pipeline and retry.",
        True,
    )


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
        policy = policy_from_tenant_config(tenant_config, created_at=created_at)
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
            self._executor.submit(lambda: self._run_worker(backend, request, policy))
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

    def _run_worker(
        self,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
    ) -> None:
        try:
            with backend.create_preview_write_unit_of_work() as uow:
                if not uow.requests.mark_running(request.request_id, self._clock()):
                    return
                uow.commit()
            snapshot, package = self._generate(backend, request, policy)
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
        self,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
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
            if isinstance(coverage, (NoUsableCalculationCoverage, PartialCalculationCoverage)):
                raise _calculation_failure(coverage, policy)
            assert isinstance(coverage, CompleteCalculationCoverage)
            diagnostic = request_eligibility_diagnostic(request=request, policy=policy)
            if diagnostic is not None:
                raise _PreviewFailureError(diagnostic)

            valid_count = 0
            source: PreviewSourceEvidence | None = None
            winning_issue: PreviewSourceIssue | None = None
            issue_correlations: tuple[str, ...] = ()
            valid_correlations: tuple[str, ...] = ()
            for candidate in uow.cost_evidence.iter_preview_sources(scope):
                issue = classify_daily_full_source(request_start=start, request_end=end, source=candidate)
                correlation = public_source_correlation_id(
                    ecosystem=request.ecosystem,
                    tenant_id=request.tenant_id,
                    source_record_id=candidate.source_record_id,
                )
                if issue is not None:
                    if winning_issue is None or _ISSUE_PRECEDENCE[issue] < _ISSUE_PRECEDENCE[winning_issue]:
                        winning_issue = issue
                        issue_correlations = (correlation,)
                    elif issue is winning_issue:
                        issue_correlations = capped_correlations([*issue_correlations, correlation])
                    continue
                valid_count = min(2, valid_count + 1)
                source = candidate if valid_count == 1 else None
                valid_correlations = capped_correlations([*valid_correlations, correlation])
            if winning_issue is not None:
                raise _PreviewFailureError(source_issue_diagnostic(winning_issue, issue_correlations))

            coverage_matches = _coverage_matches(
                uow.cost_evidence.iter_preview_sources(scope),
                uow.cost_evidence.iter_preview_aggregates(scope),
            )
            if not coverage_matches:
                raise _failure(
                    "preview_source_coverage_incomplete",
                    "Persisted source evidence does not completely cover the calculated Preview scope.",
                    False,
                    valid_correlations,
                )
            if valid_count != 1 or source is None:
                raise _failure(
                    "preview_mapping_scope_unsupported",
                    "The complete source set exceeds the current Daily Full mapping scope.",
                    False,
                    valid_correlations,
                )
            source_correlation = public_source_correlation_id(
                ecosystem=request.ecosystem,
                tenant_id=request.tenant_id,
                source_record_id=source.source_record_id,
            )
            selected_correlations = (source_correlation,)
            aggregates = uow.cost_evidence.find_preview_aggregate_candidates(scope, source)
            allocations = uow.allocation_evidence.find_preview_allocation_candidates(scope, source)
            if len(aggregates) != 1 or len(allocations) != 1:
                raise _failure(
                    "preview_mapping_scope_unsupported",
                    "The complete source set exceeds the current Daily Full mapping scope.",
                    source_correlation_ids=selected_correlations,
                )
            aggregate, allocation = aggregates[0], allocations[0]
            if not aggregate.compatibility_currency:
                raise _failure(
                    "preview_billing_currency_unknown",
                    "Persisted billing currency evidence is unknown for one or more source records.",
                    False,
                    (source_correlation,),
                )
            if aggregate.compatibility_currency != "USD":
                raise _failure(
                    "preview_billing_currency_unsupported",
                    "FOCUS Mapping Preview currently supports only USD billing currency.",
                    False,
                    (source_correlation,),
                )
            try:
                validate_daily_full_mapping(
                    request_start=start,
                    request_end=end,
                    source=source,
                    aggregate=aggregate,
                    allocation=allocation,
                )
            except PreviewMappingError as exc:
                raise _mapping_failure(exc, selected_correlations) from exc
            assert source.resource_id is not None
            resource = uow.resources.get(request.ecosystem, request.tenant_id, source.resource_id)
            environment = (
                uow.resources.get(request.ecosystem, request.tenant_id, source.environment_id)
                if source.environment_id is not None
                else None
            )
            identity = uow.identities.get(request.ecosystem, request.tenant_id, allocation.allocation_target_id)
            if resource is None or identity is None:
                raise _failure(
                    "preview_source_record_incomplete",
                    "One or more source records lack required Preview evidence.",
                    source_correlation_ids=selected_correlations,
                )
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
                raise _mapping_failure(exc, selected_correlations) from exc
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
