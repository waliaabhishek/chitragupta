from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    capped_correlations,
    public_source_correlation_id,
    request_eligibility_diagnostic,
    source_issue_diagnostic,
)
from core.preview.evidence import PreviewAllocationEvidenceDecodeError, PreviewEvidenceScope
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
    PreviewFullRow,
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
    classify_daily_full_source,
    preview_sum_decimals,
    project_allocated_financials,
    project_daily_portion_full_row,
    project_financials,
    reconcile_allocation_lineage_stream,
    reconcile_source_aggregate_stream,
    resolve_provider_resource_context_from_mapping,
    source_through,
)
from core.preview.models import (
    PreviewDiagnostic,
    PreviewMonthlyStatus,
    PreviewRequest,
    PreviewSourceSnapshot,
)
from core.preview.monthly import PreviewMonthlyAggregationError, aggregate_monthly_full_rows
from core.preview.persistence import (
    CompleteCalculationCoverage,
    NoUsableCalculationCoverage,
    PartialCalculationCoverage,
    PreviewStorageBackend,
)
from core.preview.request import PreviewEvidencePendingError, resolve_preview_evidence_interval

if TYPE_CHECKING:
    from core.models.entity_tag import EntityTag

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(UTC)


class PreviewGenerationError(Exception):
    def __init__(self, diagnostic: PreviewDiagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


def _failure(
    code: str,
    message: str,
    retryable: bool = False,
    source_correlation_ids: tuple[str, ...] = (),
) -> PreviewGenerationError:
    return PreviewGenerationError(PreviewDiagnostic(code, message, retryable, source_correlation_ids))


def _mapping_failure(
    error: PreviewMappingError,
    source_correlation_ids: tuple[str, ...],
) -> PreviewGenerationError:
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
) -> PreviewGenerationError:
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


class PreviewPackageGenerator:
    def __init__(
        self,
        *,
        max_csv_file_bytes: int | None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._max_csv_file_bytes = max_csv_file_bytes
        self._clock = clock

    def generate(
        self,
        *,
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
                raise PreviewGenerationError(diagnostic)
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
                raise PreviewGenerationError(diagnostic)
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
                raise PreviewGenerationError(source_issue_diagnostic(winning_issue, issue_correlations))
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
                source_through=max((source_through(item.source) for item in selected_by_origin.values()), default=None),
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
                            financials = project_allocated_financials(selected=selected, allocation=allocation)
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

                full_rows: Iterable[PreviewFullRow] = (
                    project_daily_portion_full_row(prepared=prepared, provider_context=provider_context)
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
