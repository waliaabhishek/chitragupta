from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.preview.artifacts import PreviewArtifactIntegrityError
from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    capped_correlations,
    policy_from_tenant_config,
    public_source_correlation_id,
    request_eligibility_diagnostic,
    source_issue_diagnostic,
)
from core.preview.evidence import (
    PreviewAllocationEvidenceDecodeError,
    PreviewEvidenceScope,
)
from core.preview.mapping import (
    FOCUS_1_4_NATIVE_LINE_READINESS_V1,
    FOCUS_1_4_SERVICE_RULES_V1,
    AcceptedPreviewSource,
    PreparedPreviewPackageRow,
    PreviewAllocationLineageError,
    PreviewBillingAccountConflictError,
    PreviewBillingAccountUnavailableError,
    PreviewBillingCurrencyUnknownError,
    PreviewBillingCurrencyUnsupportedError,
    PreviewCsvFileSizeError,
    PreviewDataPackageDraft,
    PreviewFinancialReconciliationError,
    PreviewFinancialUnsupportedError,
    PreviewLineageReadiness,
    PreviewMappingError,
    PreviewMappingScopeError,
    PreviewPackageReconciliation,
    PreviewProviderContext,
    PreviewProviderContextIncompleteError,
    PreviewResourceShape,
    PreviewRowValidationError,
    PreviewSourceAggregateReconciliationError,
    PreviewSourceCoverageError,
    PreviewSourceEvidenceError,
    PreviewSourceIssue,
    SelectedPreviewEvidence,
    SelectedSourceProjection,
    build_preview_data_package,
    build_preview_manifest,
    classify_daily_full_source,
    preview_sum_decimals,
    project_allocated_financials,
    project_daily_portion_full_row,
    project_financials,
    reconcile_allocation_lineage_stream,
    reconcile_source_aggregate_stream,
    resolve_provider_resource_context_from_mapping,
    source_through,
    validate_preview_effective_columns,
)
from core.preview.models import (
    PreviewColumnProfile,
    PreviewDiagnostic,
    PreviewGrain,
    PreviewMonthlyStatus,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
    validate_preview_request_snapshot,
)
from core.preview.monthly import PreviewMonthlyAggregationError, aggregate_monthly_full_rows
from core.preview.persistence import (
    CompleteCalculationCoverage,
    NoUsableCalculationCoverage,
    PartialCalculationCoverage,
    PreviewExpiredArtifact,
    PreviewRequestPage,
    PreviewStorageBackend,
)
from core.preview.request import PreviewEvidencePendingError, resolve_preview_evidence_interval

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.models.entity_tag import EntityTag
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


class _PreviewFailureError(Exception):
    def __init__(self, diagnostic: PreviewDiagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


class _PreviewReadyTransitionError(RuntimeError):
    def __init__(self, storage_key: str) -> None:
        super().__init__("preview ready transition was rejected after artifact finalization")
        self.storage_key = storage_key


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
    if isinstance(error, PreviewSourceEvidenceError):
        return _failure(
            "preview_source_record_incomplete",
            "One or more source records lack required Preview evidence.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewCsvFileSizeError):
        return _failure(
            "preview_csv_row_exceeds_file_size_limit",
            "A Preview CSV header or row exceeds the configured file-size limit.",
        )
    if isinstance(error, PreviewFinancialUnsupportedError):
        return _failure(
            "preview_source_economics_unsupported",
            "One or more source records have unsupported monetary or quantity values.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewFinancialReconciliationError):
        return _failure(
            "preview_source_reconciliation_failed",
            "Persisted source, aggregate, or allocation evidence does not reconcile.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewMappingScopeError):
        return _failure(
            "preview_mapping_scope_unsupported",
            "The complete source set exceeds the current Daily Full mapping scope.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewBillingAccountUnavailableError):
        return _failure(
            "preview_billing_account_unavailable",
            "Authoritative Confluent Cloud organization evidence is unavailable for this tenant.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewBillingAccountConflictError):
        return _failure(
            "preview_billing_account_conflicting",
            "Persisted Confluent Cloud organization evidence conflicts for this tenant.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, PreviewProviderContextIncompleteError):
        return _failure(
            "preview_provider_context_incomplete",
            "Authoritative provider resource context is unavailable for one or more source records.",
            source_correlation_ids=source_correlation_ids,
        )
    if isinstance(error, (PreviewRowValidationError, PreviewMonthlyAggregationError)):
        return _failure(
            "preview_mapping_validation_failed",
            "The generated row does not satisfy the Daily Full mapping profile.",
            source_correlation_ids=source_correlation_ids,
        )
    raise TypeError(f"Unhandled Preview mapping error type: {type(error).__name__}")


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


def _tags_json(tags: Sequence[EntityTag]) -> str:
    values = {tag.tag_key: tag.tag_value for tag in tags}
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def _source_correlations(request: PreviewRequest, source_record_ids: Sequence[str]) -> tuple[str, ...]:
    return capped_correlations(
        [
            public_source_correlation_id(
                ecosystem=request.ecosystem,
                tenant_id=request.tenant_id,
                source_record_id=source_record_id,
            )
            for source_record_id in source_record_ids
        ]
    )


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
        max_csv_file_bytes: int | None = None,
        startup_at: datetime | None = None,
        configured_owner_keys: tuple[tuple[str, str, str], ...] = (),
        clock: Callable[[], datetime] = utc_now,
        request_id_factory: Callable[[], str] = new_uuid,
        executor: PreviewExecutor | None = None,
        lease_owner_id: str | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._clock = clock
        self._max_csv_file_bytes = max_csv_file_bytes
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
            snapshot, draft = self._generate(backend, running, policy)
            with self._artifact_store.stage_data_files(
                request_id=request.request_id,
                data_files=draft.data_files,
            ) as staged:
                ready_at = self._clock().astimezone(UTC).replace(microsecond=0)
                expires_at = ready_at + timedelta(days=7)
                manifest_body = build_preview_manifest(
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
        except _PreviewFailureError as exc:
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

    def _generate(
        self,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
    ) -> tuple[PreviewSourceSnapshot, PreviewDataPackageDraft]:
        try:
            evidence_interval = resolve_preview_evidence_interval(request=request, policy=policy)
        except PreviewEvidencePendingError:
            raise _failure(
                "calculation_pending_cutoff_window",
                "One or more requested dates are still inside the configured acquisition cutoff window; "
                "wait for the dates to enter the acquisition window, run the pipeline, and retry.",
                True,
            ) from None
        monthly_status: PreviewMonthlyStatus | None = (
            None
            if evidence_interval.monthly_stage is None
            else "settled"
            if evidence_interval.monthly_stage == "settlement_candidate"
            else "provisional"
        )
        if evidence_interval.start_date == evidence_interval.end_date:
            diagnostic = request_eligibility_diagnostic(request=request, policy=policy)
            if diagnostic is not None:
                raise _PreviewFailureError(diagnostic)
            snapshot = PreviewSourceSnapshot(
                calculation_timestamp=None,
                calculation_coverage=(),
                source_through=None,
                effective_coverage_start_date=evidence_interval.start_date,
                effective_coverage_end_date=evidence_interval.end_date,
                availability_cutoff_end_date=(policy.acquisition_end_date if request.grain == "monthly" else None),
                monthly_status=monthly_status,
            )
            package = build_preview_data_package(
                request=request,
                snapshot=snapshot,
                full_rows=(),
                reconciliation=PreviewPackageReconciliation(0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
                max_csv_file_bytes=self._max_csv_file_bytes,
            )
            return snapshot, package
        start = datetime.combine(evidence_interval.start_date, datetime.min.time(), tzinfo=UTC)
        end = datetime.combine(evidence_interval.end_date, datetime.min.time(), tzinfo=UTC)
        scope = PreviewEvidenceScope(request.ecosystem, request.tenant_id, start, end)
        with backend.create_preview_read_unit_of_work() as uow:
            coverage = uow.calculations.find_current_coverage(
                ecosystem=request.ecosystem,
                tenant_id=request.tenant_id,
                start_date=evidence_interval.start_date,
                end_date=evidence_interval.end_date,
            )
            if isinstance(coverage, (NoUsableCalculationCoverage, PartialCalculationCoverage)):
                raise _calculation_failure(coverage, policy)
            assert isinstance(coverage, CompleteCalculationCoverage)
            diagnostic = request_eligibility_diagnostic(request=request, policy=policy)
            if diagnostic is not None:
                raise _PreviewFailureError(diagnostic)
            accepted: list[SelectedSourceProjection] = []
            winning_issue: PreviewSourceIssue | None = None
            issue_correlations: tuple[str, ...] = ()
            valid_correlations: tuple[str, ...] = ()
            unsupported_provider_context = False
            for candidate in uow.cost_evidence.iter_preview_sources(scope):
                issue: PreviewSourceIssue | None
                classification = classify_daily_full_source(request_start=start, request_end=end, source=candidate)
                correlation = public_source_correlation_id(
                    ecosystem=request.ecosystem,
                    tenant_id=request.tenant_id,
                    source_record_id=candidate.source_record_id,
                )
                if isinstance(classification, AcceptedPreviewSource):
                    assert candidate.amount is not None
                    try:
                        financials = project_financials(
                            source=candidate,
                            semantics=classification.semantics,
                            billed_share=candidate.amount,
                        )
                    except PreviewFinancialUnsupportedError:
                        issue = PreviewSourceIssue.ECONOMICS_UNSUPPORTED
                    except PreviewFinancialReconciliationError:
                        issue = PreviewSourceIssue.RECONCILIATION_FAILED
                    else:
                        issue = None
                else:
                    issue = classification.issue
                if issue is not None:
                    if winning_issue is None or _ISSUE_PRECEDENCE[issue] < _ISSUE_PRECEDENCE[winning_issue]:
                        winning_issue = issue
                        issue_correlations = (correlation,)
                    elif issue is winning_issue:
                        issue_correlations = capped_correlations([*issue_correlations, correlation])
                    continue
                assert isinstance(classification, AcceptedPreviewSource)
                assert candidate.native_line_type is not None
                if FOCUS_1_4_NATIVE_LINE_READINESS_V1[candidate.native_line_type] is not PreviewLineageReadiness.READY:
                    raise PreviewMappingScopeError("native line type lacks allocation lineage readiness")
                rule = FOCUS_1_4_SERVICE_RULES_V1[classification.semantics.service_rule_key]
                unsupported_provider_context = (
                    unsupported_provider_context or rule.context_strategy == "unsupported_provider_context"
                )
                accepted.append(SelectedSourceProjection(candidate, classification.semantics, financials))
                valid_correlations = capped_correlations([*valid_correlations, correlation])
            if winning_issue is not None:
                raise _PreviewFailureError(source_issue_diagnostic(winning_issue, issue_correlations))
            if unsupported_provider_context:
                raise _mapping_failure(
                    PreviewProviderContextIncompleteError("TABLEFLOW provider context is unavailable"),
                    valid_correlations,
                )

            try:
                selected_by_origin, aggregate_by_origin = reconcile_source_aggregate_stream(
                    selected_sources=accepted,
                    aggregates=uow.cost_evidence.iter_preview_aggregates(scope),
                )
            except PreviewSourceCoverageError:
                raise _failure(
                    "preview_source_coverage_incomplete",
                    "Persisted source evidence does not completely cover the calculated Preview scope.",
                    False,
                    valid_correlations,
                ) from None
            except PreviewMappingScopeError as exc:
                raise _mapping_failure(exc, valid_correlations) from None
            except PreviewBillingCurrencyUnknownError as exc:
                raise _failure(
                    "preview_billing_currency_unknown",
                    "Persisted billing currency evidence is unknown for one or more source records.",
                    source_correlation_ids=_source_correlations(request, exc.source_record_ids),
                ) from None
            except PreviewBillingCurrencyUnsupportedError as exc:
                raise _failure(
                    "preview_billing_currency_unsupported",
                    "FOCUS Mapping Preview currently supports only USD billing currency.",
                    source_correlation_ids=_source_correlations(request, exc.source_record_ids),
                ) from None
            except PreviewSourceAggregateReconciliationError as exc:
                raise _failure(
                    "preview_source_reconciliation_failed",
                    "Persisted source, aggregate, or allocation evidence does not reconcile.",
                    source_correlation_ids=_source_correlations(request, exc.source_record_ids),
                ) from None

            calculation_ids = tuple(entry.calculation_id for entry in coverage.entries)
            expected_completion_by_run = {
                (entry.tracking_date, entry.calculation_id): entry.calculation_completed_at
                for entry in coverage.entries
            }
            try:
                allocations_by_origin = reconcile_allocation_lineage_stream(
                    aggregates_by_origin=aggregate_by_origin,
                    expected_completion_by_run=expected_completion_by_run,
                    runs=uow.allocation_evidence.iter_preview_allocation_runs(scope, calculation_ids),
                    allocations=uow.allocation_evidence.iter_preview_allocations(scope, calculation_ids),
                )
            except PreviewAllocationEvidenceDecodeError, PreviewAllocationLineageError:
                raise _failure(
                    "preview_allocation_lineage_incomplete",
                    "Persisted allocation lineage is incomplete for one or more billing origins.",
                    source_correlation_ids=valid_correlations,
                ) from None
            except PreviewFinancialReconciliationError:
                raise _failure(
                    "preview_source_reconciliation_failed",
                    "Persisted source, aggregate, or allocation evidence does not reconcile.",
                    source_correlation_ids=valid_correlations,
                ) from None

            current_time = self._clock()
            snapshot = PreviewSourceSnapshot(
                calculation_timestamp=max(entry.calculation_completed_at for entry in coverage.entries),
                calculation_coverage=coverage.entries,
                source_through=(
                    max((source_through(item.source) for item in selected_by_origin.values()), default=None)
                ),
                effective_coverage_start_date=evidence_interval.start_date,
                effective_coverage_end_date=evidence_interval.end_date,
                availability_cutoff_end_date=(policy.acquisition_end_date if request.grain == "monthly" else None),
                monthly_status=monthly_status,
            )
            if not selected_by_origin:
                package = build_preview_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=(),
                    reconciliation=PreviewPackageReconciliation(0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
                    max_csv_file_bytes=self._max_csv_file_bytes,
                )
                return snapshot, package
            organizations, _ = uow.resources.find_active_at(
                request.ecosystem,
                request.tenant_id,
                current_time,
                resource_type="organization",
                limit=2,
                count=False,
            )
            if not organizations:
                raise _mapping_failure(PreviewBillingAccountUnavailableError(), valid_correlations)
            if len(organizations) != 1 or organizations[0].metadata.get("organization_binding_state") != "bound":
                raise _mapping_failure(PreviewBillingAccountConflictError(), valid_correlations)
            organization = organizations[0]
            if not organization.resource_id.strip():
                raise _mapping_failure(PreviewBillingAccountUnavailableError(), valid_correlations)
            provider_context = PreviewProviderContext(organization.resource_id, organization.display_name)

            resource_tag_ids = {
                item.source.resource_id for item in selected_by_origin.values() if item.source.resource_id is not None
            }
            resource_tag_ids.update(
                portion.target_id
                for portions in allocations_by_origin.values()
                for portion in portions
                if portion.target_kind == "resource" and portion.target_id is not None
            )
            identity_tag_ids = {
                portion.target_id
                for portions in allocations_by_origin.values()
                for portion in portions
                if portion.target_kind == "identity" and portion.target_id is not None
            }
            resource_lookup_ids = set(resource_tag_ids)
            resource_lookup_ids.update(
                item.source.environment_id
                for item in selected_by_origin.values()
                if item.source.environment_id is not None
            )
            resource_by_id = uow.resources.get_many(
                request.ecosystem,
                request.tenant_id,
                sorted(resource_lookup_ids),
            )
            auxiliary_resource_ids: set[str] = set()
            for selected in selected_by_origin.values():
                source_resource_id = selected.source.resource_id
                origin_resource = resource_by_id.get(source_resource_id) if source_resource_id is not None else None
                if origin_resource is None:
                    continue
                if origin_resource.parent_id is not None:
                    auxiliary_resource_ids.add(origin_resource.parent_id)
                for metadata_key in ("kafka_cluster_id", "compute_pool_id"):
                    metadata_value = origin_resource.metadata.get(metadata_key)
                    if isinstance(metadata_value, str):
                        auxiliary_resource_ids.add(metadata_value)
            missing_auxiliary_ids = auxiliary_resource_ids.difference(resource_by_id)
            if missing_auxiliary_ids:
                resource_by_id.update(
                    uow.resources.get_many(
                        request.ecosystem,
                        request.tenant_id,
                        sorted(missing_auxiliary_ids),
                    )
                )
            identity_by_id = uow.identities.get_many(
                request.ecosystem,
                request.tenant_id,
                sorted(identity_tag_ids),
            )
            resource_tags = uow.tags.find_tags_for_entities(request.tenant_id, "resource", sorted(resource_tag_ids))
            identity_tags = uow.tags.find_tags_for_entities(request.tenant_id, "identity", sorted(identity_tag_ids))

            try:

                def package_rows() -> Iterator[PreparedPreviewPackageRow]:
                    for key in sorted(allocations_by_origin):
                        selected = selected_by_origin[key]
                        source = selected.source
                        origin_resource = (
                            resource_by_id.get(source.resource_id) if source.resource_id is not None else None
                        )
                        environment = (
                            resource_by_id.get(source.environment_id) if source.environment_id is not None else None
                        )
                        rule = FOCUS_1_4_SERVICE_RULES_V1[selected.semantics.service_rule_key]
                        if rule.resource_shape is not PreviewResourceShape.ORGANIZATION_WIDE and (
                            source.environment_id is None
                            or environment is None
                            or environment.resource_type != "environment"
                        ):
                            raise PreviewProviderContextIncompleteError("source environment authority is incompatible")
                        resource_context = resolve_provider_resource_context_from_mapping(
                            source=source,
                            semantics=selected.semantics,
                            origin_resource=origin_resource,
                            resources=uow.resources,
                            resource_by_id=resource_by_id,
                        )
                        origin_tags = _tags_json(
                            resource_tags.get(source.resource_id, []) if source.resource_id is not None else []
                        )
                        for allocation in allocations_by_origin[key]:
                            target_id = allocation.target_id
                            allocated_entity = (
                                resource_by_id.get(target_id)
                                if allocation.target_kind == "resource" and target_id is not None
                                else identity_by_id.get(target_id)
                                if allocation.target_kind == "identity" and target_id is not None
                                else None
                            )
                            allocated_tags = (
                                None
                                if allocation.target_kind == "unallocated"
                                else _tags_json(resource_tags.get(target_id, []))
                                if allocation.target_kind == "resource" and target_id is not None
                                else _tags_json(identity_tags.get(target_id, []))
                                if target_id is not None
                                else "{}"
                            )
                            financials = project_allocated_financials(
                                selected=selected,
                                allocation=allocation,
                            )
                            yield PreparedPreviewPackageRow(
                                evidence=SelectedPreviewEvidence(
                                    SelectedSourceProjection(source, selected.semantics, financials),
                                    aggregate_by_origin[key],
                                    allocation,
                                ),
                                resource_context=resource_context,
                                allocated_entity=allocated_entity,
                                environment=environment,
                                origin_tags_json=origin_tags,
                                allocated_tags_json=allocated_tags,
                            )

                full_rows = tuple(
                    project_daily_portion_full_row(
                        prepared=prepared,
                        provider_context=provider_context,
                    )
                    for prepared in package_rows()
                )
                if request.grain == "monthly":
                    full_rows = aggregate_monthly_full_rows(
                        rows=full_rows,
                        month_start=datetime.combine(request.start_date, datetime.min.time(), tzinfo=UTC),
                        month_end=datetime.combine(request.end_date, datetime.min.time(), tzinfo=UTC),
                    )
                source_cost = preview_sum_decimals(
                    item.source.amount or Decimal(0) for item in selected_by_origin.values()
                )
                allocated_cost = preview_sum_decimals(
                    allocation.allocated_cost
                    for allocations in allocations_by_origin.values()
                    for allocation in allocations
                )
                source_quantity = preview_sum_decimals(
                    item.source.quantity or Decimal(0) for item in selected_by_origin.values()
                )
                allocated_quantity = preview_sum_decimals(
                    allocation.allocated_quantity
                    for allocations in allocations_by_origin.values()
                    for allocation in allocations
                )
                package = build_preview_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=full_rows,
                    reconciliation=PreviewPackageReconciliation(
                        len(selected_by_origin),
                        source_cost,
                        allocated_cost,
                        source_quantity,
                        allocated_quantity,
                    ),
                    max_csv_file_bytes=self._max_csv_file_bytes,
                )
            except PreviewMappingError as exc:
                raise _mapping_failure(exc, valid_correlations) from exc
        return snapshot, package

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
