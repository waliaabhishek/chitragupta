from __future__ import annotations

import json
import logging
import pickle
import sys
from bisect import bisect_right
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, localcontext
from itertools import batched, chain
from typing import TYPE_CHECKING

from core.preview.eligibility import (
    PreviewEligibilityPolicy,
    capped_correlations,
    public_source_correlation_id,
    request_eligibility_diagnostic,
    source_issue_diagnostic,
)
from core.preview.evidence import (
    AllocationLineageRunStatus,
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewAllocationEvidenceDecodeError,
    PreviewAllocationRunEvidence,
    PreviewEvidenceScope,
    PreviewSourceAttempt,
    PreviewSourceAuthoritySlice,
    PreviewSourceReadiness,
    SourceAttemptStatus,
)
from core.preview.mapping import (
    FOCUS_1_4_SERVICE_RULES_V1,
    PREVIEW_DECIMAL_CONTEXT,
    AcceptedPreviewSource,
    PreparedPreviewPackageRow,
    PreviewAllocationLineageError,
    PreviewBillingAccountConflictError,
    PreviewBillingAccountUnavailableError,
    PreviewCsvFileSizeError,
    PreviewDataPackageDraft,
    PreviewFinancialReconciliationError,
    PreviewFinancialUnsupportedError,
    PreviewFullRow,
    PreviewMappingError,
    PreviewMappingScopeError,
    PreviewPackageReconciliation,
    PreviewProviderContext,
    PreviewProviderContextIncompleteError,
    PreviewResourceShape,
    PreviewRowValidationError,
    PreviewSourceCoverageError,
    PreviewSourceEvidenceError,
    PreviewSourceIssue,
    SelectedPreviewEvidence,
    SelectedSourceProjection,
    build_bounded_preview_data_package,
    build_preview_data_package,
    classify_daily_full_source,
    preview_decimal_text,
    preview_sum_decimals,
    preview_utc_text,
    project_allocated_financials,
    project_daily_portion_full_row,
    project_financials,
    reconcile_source_aggregate_evidence,
    resolve_provider_resource_context_from_mapping,
    source_through,
    validate_allocation_lineage_portion,
)
from core.preview.models import (
    PreviewDiagnostic,
    PreviewMonthlyStatus,
    PreviewRequest,
    PreviewSourceSnapshot,
)
from core.preview.monthly import (
    PreviewMonthlyAggregationError,
    aggregate_monthly_full_rows,
    aggregate_monthly_full_rows_bounded,
)
from core.preview.organization_authority import OrganizationAuthorityAttemptStatus
from core.preview.persistence import (
    CompleteCalculationCoverage,
    NoUsableCalculationCoverage,
    PartialCalculationCoverage,
    PreviewEvidenceStorageBackend,
    PreviewSourceReadinessReader,
    PreviewStorageBackend,
)
from core.preview.request import PreviewEvidencePendingError, resolve_preview_evidence_interval
from core.preview.spooling import (
    SQLITE_BATCH_SIZE,
    PreviewGenerationSpoolLimitError,
    PreviewGenerationWorkspace,
)
from core.preview.storage_availability import PreviewEvidenceUnavailableError

if TYPE_CHECKING:
    from core.models.entity_tag import EntityTag
    from core.models.identity import Identity
    from core.models.resource import Resource
    from core.preview.persistence import PreviewGenerationReadUnitOfWork

logger = logging.getLogger(__name__)
_LEGACY_BUILD_PREVIEW_DATA_PACKAGE = build_preview_data_package
_LEGACY_AGGREGATE_MONTHLY_FULL_ROWS = aggregate_monthly_full_rows


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


def _evidence_storage_unavailable() -> PreviewGenerationError:
    return _failure(
        "preview_evidence_storage_unavailable",
        "FOCUS Mapping Preview evidence storage is unavailable; repair the enabled tenant database "
        "schema and rerun the pipeline.",
        False,
    )


def _require_complete_source_authority(authority: PreviewSourceAttempt | None) -> None:
    if authority is not None and authority.status is not SourceAttemptStatus.COMPLETE:
        raise _failure(
            "preview_source_evidence_unavailable",
            "Native Confluent Cloud source evidence is unavailable for the requested scope; "
            "run the pipeline and retry.",
            True,
        )


def _resolve_source_authority(
    source_readiness: PreviewSourceReadinessReader,
    ecosystem: str,
    tenant_id: str,
    start: datetime,
    end: datetime,
) -> tuple[PreviewSourceAuthoritySlice, ...]:
    resolver = getattr(source_readiness, "resolve_authority", None)
    if callable(resolver):
        resolved = resolver(ecosystem, tenant_id, start, end)
        if isinstance(resolved, tuple) and all(isinstance(item, PreviewSourceAuthoritySlice) for item in resolved):
            return resolved
    authority = source_readiness.get_current_authority(ecosystem, tenant_id)
    return (PreviewSourceAuthoritySlice(start, end, authority),)


def _readiness_covering_slice(
    authority_slice: PreviewSourceAuthoritySlice,
    rows: tuple[PreviewSourceReadiness, ...],
    starts: tuple[datetime, ...],
) -> tuple[PreviewSourceReadiness, ...] | None:
    cursor = authority_slice.start
    position = max(0, bisect_right(starts, cursor) - 1)
    used: list[PreviewSourceReadiness] = []
    while position < len(rows):
        item = rows[position]
        if item.window_start > cursor or item.window_start >= authority_slice.end:
            break
        if item.window_end > cursor:
            used.append(item)
            cursor = item.window_end
            if cursor >= authority_slice.end:
                return tuple(used)
        position += 1
    return None


def _authority_for_window(
    authority_slices: tuple[PreviewSourceAuthoritySlice, ...],
    starts: tuple[datetime, ...],
    window_start: datetime,
    window_end: datetime,
) -> PreviewSourceAttempt | None:
    position = bisect_right(starts, window_start) - 1
    if position < 0:
        return None
    authority_slice = authority_slices[position]
    if authority_slice.start <= window_start and window_end <= authority_slice.end:
        return authority_slice.attempt
    return None


@contextmanager
def _generation_read_unit_of_work(
    backend: PreviewEvidenceStorageBackend,
) -> Iterator[PreviewGenerationReadUnitOfWork]:
    try:
        with backend.create_preview_generation_read_unit_of_work() as uow:
            yield uow
    except PreviewEvidenceUnavailableError:
        raise _evidence_storage_unavailable() from None


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
    PreviewSourceIssue.RECORD_INCOMPLETE: 6,
    PreviewSourceIssue.ECONOMICS_UNSUPPORTED: 7,
    PreviewSourceIssue.RECONCILIATION_FAILED: 8,
}


def _tags_json(tags: Sequence[EntityTag]) -> str:
    values = {tag.tag_key: tag.tag_value for tag in tags}
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def _source_correlations(request: PreviewRequest, source_record_ids: Iterable[str]) -> tuple[str, ...]:
    smallest: list[str] = []
    for source_record_id in source_record_ids:
        correlation = public_source_correlation_id(
            ecosystem=request.ecosystem,
            tenant_id=request.tenant_id,
            source_record_id=source_record_id,
        )
        position = bisect_right(smallest, correlation)
        if position < 20 and correlation not in smallest[max(0, position - 1) : position + 1]:
            smallest.insert(position, correlation)
            del smallest[20:]
    return tuple(smallest)


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


def _origin_key(
    timestamp: datetime,
    environment_id: str,
    resource_id: str,
    native_product: str,
    native_line_type: str,
    source_record_id: str | None = None,
    evidence_scope_start: datetime | None = None,
    evidence_scope_end: datetime | None = None,
) -> str:
    values: list[str] = [
        preview_utc_text(timestamp),
        environment_id,
        resource_id,
        native_product,
        native_line_type,
    ]
    if source_record_id is not None and evidence_scope_start is not None and evidence_scope_end is not None:
        values.extend(
            (
                source_record_id,
                preview_utc_text(evidence_scope_start),
                preview_utc_text(evidence_scope_end),
            )
        )
    return json.dumps(values, separators=(",", ":"))


def _selected_origin(selected: SelectedSourceProjection) -> str | None:
    source = selected.source
    values = (
        source.billing_timestamp,
        source.billing_env_id,
        source.billing_resource_id,
        source.billing_product_category,
        source.billing_product_type,
    )
    if any(value is None for value in values):
        return None
    timestamp, environment_id, resource_id, native_product, native_line_type = values
    assert isinstance(timestamp, datetime)
    assert isinstance(environment_id, str)
    assert isinstance(resource_id, str)
    assert isinstance(native_product, str)
    assert isinstance(native_line_type, str)
    return _origin_key(
        timestamp,
        environment_id,
        resource_id,
        native_product,
        native_line_type,
        source.source_record_id,
        source.evidence_scope_start,
        source.evidence_scope_end,
    )


def _selected_compatibility_origin(selected: SelectedSourceProjection) -> str | None:
    source = selected.source
    values = (
        source.billing_timestamp,
        source.billing_env_id,
        source.billing_resource_id,
        source.billing_product_category,
        source.billing_product_type,
    )
    if any(value is None for value in values):
        return None
    timestamp, environment_id, resource_id, native_product, native_line_type = values
    assert isinstance(timestamp, datetime)
    assert isinstance(environment_id, str)
    assert isinstance(resource_id, str)
    assert isinstance(native_product, str)
    assert isinstance(native_line_type, str)
    return _origin_key(timestamp, environment_id, resource_id, native_product, native_line_type)


def _aggregate_origin(aggregate: PreviewAggregateEvidence) -> str:
    return _origin_key(
        aggregate.timestamp,
        aggregate.environment_id,
        aggregate.resource_id,
        aggregate.native_product,
        aggregate.native_line_type,
        aggregate.source_record_id or None,
        aggregate.evidence_scope_start,
        aggregate.evidence_scope_end,
    )


def _allocation_origin(allocation: PreviewAllocationEvidence) -> str:
    return _origin_key(
        allocation.timestamp,
        allocation.environment_id,
        allocation.resource_id,
        allocation.native_product,
        allocation.native_line_type,
        allocation.source_record_id or None,
        allocation.evidence_scope_start,
        allocation.evidence_scope_end,
    )


def _allocation_compatibility_origin(allocation: PreviewAllocationEvidence) -> str:
    return _origin_key(
        allocation.timestamp,
        allocation.environment_id,
        allocation.resource_id,
        allocation.native_product,
        allocation.native_line_type,
    )


def _legacy_key(
    timestamp: datetime,
    environment_id: str,
    resource_id: str | None,
    native_product: str,
    native_line_type: str,
    amount: Decimal | None,
    quantity: Decimal | None,
    price: Decimal | None,
) -> str:
    return json.dumps(
        [
            preview_utc_text(timestamp),
            environment_id,
            resource_id,
            native_product,
            native_line_type,
            None if amount is None else preview_decimal_text(amount),
            None if quantity is None else preview_decimal_text(quantity),
            None if price is None else preview_decimal_text(price),
        ],
        separators=(",", ":"),
    )


def _selected_legacy_key(selected: SelectedSourceProjection) -> str:
    source = selected.source
    quantity = source.quantity
    price = source.price
    if source.native_line_type == "PROMO_CREDIT":
        quantity = Decimal(0) if quantity is None else quantity
        price = Decimal(0) if price is None else price
    return _legacy_key(
        source.allocation_timestamp,
        source.environment_id or "",
        source.resource_id,
        source.native_product or "",
        source.native_line_type or "",
        source.amount,
        quantity,
        price,
    )


@dataclass
class _CompatibilityTotals:
    source_costs: list[Decimal]
    source_quantities: list[Decimal]
    compatibility_cost: Decimal
    compatibility_quantity: Decimal
    source_ids: list[str] = field(default_factory=list)
    expected_mismatch: bool = False


class _PreviewEvidenceSpool:
    """Generation-owned external reconciliation store."""

    def __init__(
        self,
        *,
        limit_bytes: int,
        workspace: PreviewGenerationWorkspace | None = None,
    ) -> None:
        self._closed = True
        self._transferred = False
        self.workspace = workspace or PreviewGenerationWorkspace(limit_bytes)
        self.path = self.workspace.root / "evidence.sqlite"
        self._connection_context = self.workspace.sqlite_connection(self.path)
        try:
            self._connection = self._connection_context.__enter__()
            self._connection.create_collation(
                "preview_decimal",
                lambda left, right: (Decimal(left) > Decimal(right)) - (Decimal(left) < Decimal(right)),
            )
            self._connection.executescript(
                """
            CREATE TABLE selected_sources (
                source_order INTEGER PRIMARY KEY,
                origin_key TEXT,
                compatibility_origin_key TEXT,
                legacy_key TEXT,
                source_record_id TEXT NOT NULL,
                source_cost TEXT NOT NULL,
                source_quantity TEXT NOT NULL,
                source_through TEXT NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE INDEX ix_selected_origin ON selected_sources (origin_key);
            CREATE INDEX ix_selected_legacy ON selected_sources (legacy_key);
            CREATE TABLE aggregates (
                aggregate_order INTEGER PRIMARY KEY,
                origin_key TEXT NOT NULL,
                compatibility_currency TEXT NOT NULL,
                payload BLOB NOT NULL
            );
            CREATE INDEX ix_aggregate_origin ON aggregates (origin_key);
            CREATE TABLE aggregate_candidates (
                candidate_key TEXT NOT NULL,
                origin_key TEXT NOT NULL
            );
            CREATE INDEX ix_aggregate_candidate ON aggregate_candidates (candidate_key);
            CREATE TABLE allocations (
                allocation_order INTEGER PRIMARY KEY,
                origin_key TEXT NOT NULL,
                compatibility_origin_key TEXT NOT NULL,
                portion_ordinal INTEGER NOT NULL,
                target_kind TEXT NOT NULL,
                target_id TEXT,
                allocated_cost TEXT NOT NULL,
                allocated_quantity TEXT NOT NULL,
                compatibility_allocated_cost TEXT,
                compatibility_allocated_quantity TEXT,
                payload BLOB NOT NULL
            );
            CREATE INDEX ix_allocation_origin ON allocations (origin_key, portion_ordinal);
            CREATE INDEX ix_allocation_compatibility_column
                ON allocations (compatibility_origin_key, portion_ordinal, allocation_order);
            CREATE INDEX ix_selected_source_cost
                ON selected_sources (source_cost COLLATE preview_decimal);
            CREATE INDEX ix_selected_source_quantity
                ON selected_sources (source_quantity COLLATE preview_decimal);
            CREATE INDEX ix_allocation_cost
                ON allocations (allocated_cost COLLATE preview_decimal);
            CREATE INDEX ix_allocation_quantity
                ON allocations (allocated_quantity COLLATE preview_decimal);
                """
            )
        except BaseException:
            self._connection_context.__exit__(*sys.exc_info())
            self.workspace.close()
            raise
        self._closed = False
        self._selected_count = 0
        self._aggregate_count = 0
        self._allocation_count = 0
        self._pending_writes = 0

    def _commit_and_enforce(self, *, force: bool = False) -> None:
        self._pending_writes += 1
        if not force and self._pending_writes < SQLITE_BATCH_SIZE:
            return
        self._connection.commit()
        self._pending_writes = 0
        try:
            self.workspace.enforce_limit()
        except PreviewGenerationSpoolLimitError:
            raise _failure(
                "preview_generation_spool_limit_exceeded",
                "FOCUS Mapping Preview package exceeds the configured generation spool limit.",
                False,
            ) from None

    def add_selected(self, selected: SelectedSourceProjection) -> None:
        self._selected_count += 1
        origin = _selected_origin(selected)
        source = selected.source
        payload = pickle.dumps(selected, protocol=pickle.HIGHEST_PROTOCOL)
        self.workspace.preflight_write(len(payload))
        self._connection.execute(
            """
            INSERT INTO selected_sources (
                source_order, origin_key, compatibility_origin_key, legacy_key, source_record_id,
                source_cost, source_quantity, source_through, payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._selected_count,
                origin,
                _selected_compatibility_origin(selected),
                _selected_legacy_key(selected) if origin is None else None,
                source.source_record_id,
                preview_decimal_text(source.amount or Decimal(0)),
                preview_decimal_text(source.quantity or Decimal(0)),
                preview_utc_text(source_through(source)),
                payload,
            ),
        )
        self._commit_and_enforce()

    def add_aggregate(self, aggregate: PreviewAggregateEvidence) -> None:
        self._aggregate_count += 1
        origin = _aggregate_origin(aggregate)
        payload = pickle.dumps(aggregate, protocol=pickle.HIGHEST_PROTOCOL)
        self.workspace.preflight_write(len(payload))
        self._connection.execute(
            """
            INSERT INTO aggregates (aggregate_order, origin_key, compatibility_currency, payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                self._aggregate_count,
                origin,
                aggregate.compatibility_currency,
                payload,
            ),
        )
        common = (
            aggregate.timestamp,
            aggregate.environment_id,
            aggregate.native_product,
            aggregate.native_line_type,
            aggregate.total_cost,
            aggregate.quantity,
            aggregate.unit_price,
        )
        self._connection.executemany(
            "INSERT INTO aggregate_candidates (candidate_key, origin_key) VALUES (?, ?)",
            (
                (
                    _legacy_key(
                        common[0],
                        common[1],
                        resource_id,
                        common[2],
                        common[3],
                        common[4],
                        common[5],
                        common[6],
                    ),
                    origin,
                )
                for resource_id in (aggregate.resource_id, None)
            ),
        )
        self._commit_and_enforce()

    def reconcile_sources(self, request: PreviewRequest) -> None:
        self._commit_and_enforce(force=True)
        self._connection.execute(
            """
            UPDATE selected_sources AS source
            SET origin_key = source.compatibility_origin_key
            WHERE source.origin_key IS NOT NULL
              AND source.compatibility_origin_key IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM aggregates AS exact
                  WHERE exact.origin_key = source.origin_key
              )
              AND (
                  SELECT COUNT(*) FROM aggregates AS compatibility
                  WHERE compatibility.origin_key = source.compatibility_origin_key
              ) = 1
            """
        )
        self._connection.execute(
            """
            UPDATE selected_sources AS source
            SET origin_key = (
                SELECT MIN(candidate.origin_key)
                FROM aggregate_candidates AS candidate
                WHERE candidate.candidate_key = source.legacy_key
            )
            WHERE source.origin_key IS NULL
              AND (
                  SELECT COUNT(DISTINCT candidate.origin_key)
                  FROM aggregate_candidates AS candidate
                  WHERE candidate.candidate_key = source.legacy_key
              ) = 1
            """
        )
        self._commit_and_enforce(force=True)

        source_count_row = self._connection.execute("SELECT COUNT(*) FROM selected_sources").fetchone()
        aggregate_count_row = self._connection.execute("SELECT COUNT(*) FROM aggregates").fetchone()
        assert source_count_row is not None
        assert aggregate_count_row is not None
        source_count = int(source_count_row[0])
        aggregate_count = int(aggregate_count_row[0])
        duplicate_source = self._connection.execute(
            """
            SELECT 1 FROM selected_sources
            WHERE origin_key IS NOT NULL
            GROUP BY origin_key HAVING COUNT(*) != 1 LIMIT 1
            """
        ).fetchone()
        duplicate_aggregate = self._connection.execute(
            "SELECT 1 FROM aggregates GROUP BY origin_key HAVING COUNT(*) != 1 LIMIT 1"
        ).fetchone()
        missing_source_origin = self._connection.execute(
            "SELECT 1 FROM selected_sources WHERE origin_key IS NULL LIMIT 1"
        ).fetchone()
        unmatched = self._connection.execute(
            """
            SELECT 1
            FROM aggregates AS a
            LEFT JOIN selected_sources AS s ON s.origin_key = a.origin_key
            WHERE s.origin_key IS NULL
            LIMIT 1
            """
        ).fetchone()
        if not source_count and not aggregate_count:
            return
        if duplicate_source is not None:
            raise PreviewMappingScopeError("billing origin has multiple accepted sources")
        if (
            not source_count
            or source_count != aggregate_count
            or duplicate_aggregate is not None
            or missing_source_origin is not None
            or unmatched is not None
        ):
            raise PreviewSourceCoverageError("source and aggregate coverage is incomplete")

        unknown = self._connection.execute(
            """
            SELECT s.source_record_id
            FROM aggregates AS a
            JOIN selected_sources AS s ON s.origin_key = a.origin_key
            WHERE a.compatibility_currency = ''
            ORDER BY a.aggregate_order
            """
        )
        first_unknown = unknown.fetchone()
        if first_unknown is not None:
            raise _failure(
                "preview_billing_currency_unknown",
                "Persisted billing currency evidence is unknown for one or more source records.",
                source_correlation_ids=_source_correlations(
                    request,
                    chain((str(first_unknown[0]),), (str(row[0]) for row in unknown)),
                ),
            )
        unsupported = self._connection.execute(
            """
            SELECT s.source_record_id
            FROM aggregates AS a
            JOIN selected_sources AS s ON s.origin_key = a.origin_key
            WHERE a.compatibility_currency != 'USD'
            ORDER BY a.aggregate_order
            """
        )
        first_unsupported = unsupported.fetchone()
        if first_unsupported is not None:
            raise _failure(
                "preview_billing_currency_unsupported",
                "FOCUS Mapping Preview currently supports only USD billing currency.",
                source_correlation_ids=_source_correlations(
                    request,
                    chain((str(first_unsupported[0]),), (str(row[0]) for row in unsupported)),
                ),
            )

        mismatched_correlations: list[str] = []
        compatibility_totals: dict[str, _CompatibilityTotals] = {}

        def add_mismatch(source_record_id: str) -> None:
            correlation = public_source_correlation_id(
                ecosystem=request.ecosystem,
                tenant_id=request.tenant_id,
                source_record_id=source_record_id,
            )
            position = bisect_right(mismatched_correlations, correlation)
            nearby = mismatched_correlations[max(0, position - 1) : position + 1]
            if position < 20 and correlation not in nearby:
                mismatched_correlations.insert(position, correlation)
                del mismatched_correlations[20:]

        pairs = self._connection.execute(
            """
            SELECT s.source_record_id, s.payload, a.payload
            FROM selected_sources AS s
            JOIN aggregates AS a ON a.origin_key = s.origin_key
            ORDER BY s.source_order
            """
        )
        for source_record_id, selected_payload, aggregate_payload in pairs:
            selected = pickle.loads(bytes(selected_payload))
            aggregate = pickle.loads(bytes(aggregate_payload))
            if not isinstance(selected, SelectedSourceProjection) or not isinstance(
                aggregate, PreviewAggregateEvidence
            ):
                raise PreviewSourceCoverageError("source and aggregate spool is invalid")
            try:
                reconcile_source_aggregate_evidence(selected=selected, aggregate=aggregate)
            except PreviewFinancialReconciliationError:
                add_mismatch(str(source_record_id))
            if aggregate.compatibility_total_cost is None or aggregate.compatibility_quantity is None:
                continue
            compatibility_origin = _origin_key(
                aggregate.timestamp,
                aggregate.environment_id,
                aggregate.resource_id,
                aggregate.native_product,
                aggregate.native_line_type,
            )
            current = compatibility_totals.get(compatibility_origin)
            if current is None:
                compatibility_totals[compatibility_origin] = _CompatibilityTotals(
                    source_costs=[aggregate.total_cost],
                    source_quantities=[aggregate.quantity],
                    compatibility_cost=aggregate.compatibility_total_cost,
                    compatibility_quantity=aggregate.compatibility_quantity,
                    source_ids=[str(source_record_id)],
                )
            else:
                current.source_costs.append(aggregate.total_cost)
                current.source_quantities.append(aggregate.quantity)
                current.source_ids.append(str(source_record_id))
                if (
                    aggregate.compatibility_total_cost != current.compatibility_cost
                    or aggregate.compatibility_quantity != current.compatibility_quantity
                ):
                    current.expected_mismatch = True
        for totals in compatibility_totals.values():
            if (
                totals.expected_mismatch
                or preview_sum_decimals(totals.source_costs) != totals.compatibility_cost
                or preview_sum_decimals(totals.source_quantities) != totals.compatibility_quantity
            ):
                for source_record_id in totals.source_ids:
                    add_mismatch(source_record_id)
        if mismatched_correlations:
            raise _failure(
                "preview_source_reconciliation_failed",
                "Persisted source, aggregate, or allocation evidence does not reconcile.",
                source_correlation_ids=tuple(mismatched_correlations),
            )

    def add_and_reconcile_allocations(
        self,
        *,
        expected_completion_by_run: dict[tuple[date, str], datetime],
        runs: Iterable[PreviewAllocationRunEvidence],
        allocations: Iterable[PreviewAllocationEvidence],
    ) -> None:
        runs_by_identity: dict[tuple[date, str], PreviewAllocationRunEvidence] = {}
        duplicate_run = False
        for run in runs:
            identity = run.tracking_date, run.calculation_id
            duplicate_run = duplicate_run or identity in runs_by_identity
            runs_by_identity[identity] = run
        expected_calculation_by_date = {
            tracking_date: calculation_id for tracking_date, calculation_id in expected_completion_by_run
        }
        allocation_count_by_run: dict[tuple[date, str], int] = {}
        invalid_scope = False

        for allocation in allocations:
            origin = _allocation_origin(allocation)
            run_identity = allocation.timestamp.date(), allocation.calculation_id
            allocation_count_by_run[run_identity] = allocation_count_by_run.get(run_identity, 0) + 1
            invalid_scope = invalid_scope or (
                allocation.calculation_id != expected_calculation_by_date.get(allocation.timestamp.date())
            )
            self._allocation_count += 1
            payload = pickle.dumps(allocation, protocol=pickle.HIGHEST_PROTOCOL)
            self.workspace.preflight_write(len(payload))
            self._connection.execute(
                """
                INSERT INTO allocations (
                    allocation_order, origin_key, compatibility_origin_key, portion_ordinal,
                    target_kind, target_id, allocated_cost, allocated_quantity,
                    compatibility_allocated_cost, compatibility_allocated_quantity, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._allocation_count,
                    origin,
                    _allocation_compatibility_origin(allocation),
                    allocation.portion_ordinal,
                    allocation.target_kind,
                    allocation.target_id,
                    preview_decimal_text(allocation.allocated_cost),
                    preview_decimal_text(allocation.allocated_quantity),
                    (
                        None
                        if allocation.compatibility_allocated_cost is None
                        else preview_decimal_text(allocation.compatibility_allocated_cost)
                    ),
                    (
                        None
                        if allocation.compatibility_allocated_quantity is None
                        else preview_decimal_text(allocation.compatibility_allocated_quantity)
                    ),
                    payload,
                ),
            )
            self._commit_and_enforce()

        self._commit_and_enforce(force=True)
        invalid_runs = (
            duplicate_run
            or set(runs_by_identity) != set(expected_completion_by_run)
            or any(
                run.capture_status is not AllocationLineageRunStatus.COMPLETE
                or run.capture_reason is not None
                or run.calculation_completed_at != expected_completion_by_run.get(identity)
                or (run.preview_portion_count if run.preview_portion_count is not None else run.portion_count)
                != allocation_count_by_run.get(identity, 0)
                for identity, run in runs_by_identity.items()
            )
        )
        missing_aggregate = self._connection.execute(
            """
            SELECT 1
            FROM (SELECT DISTINCT origin_key FROM allocations) AS portions
            LEFT JOIN aggregates AS aggregate_rows USING (origin_key)
            WHERE aggregate_rows.origin_key IS NULL
            LIMIT 1
            """
        ).fetchone()
        missing_allocations = self._connection.execute(
            """
            SELECT 1
            FROM aggregates AS aggregate_rows
            LEFT JOIN (SELECT DISTINCT origin_key FROM allocations) AS portions USING (origin_key)
            WHERE portions.origin_key IS NULL
            LIMIT 1
            """
        ).fetchone()
        if invalid_runs or invalid_scope or missing_aggregate is not None or missing_allocations is not None:
            raise PreviewAllocationLineageError("allocation lineage stream is incomplete")

        partial_compatibility_column = self._connection.execute(
            """
            SELECT 1 FROM allocations
            WHERE (compatibility_allocated_cost IS NULL) !=
                  (compatibility_allocated_quantity IS NULL)
            LIMIT 1
            """
        ).fetchone()
        if partial_compatibility_column is not None:
            raise PreviewAllocationLineageError("allocation lineage stream is incomplete")

        current_column: tuple[str, int] | None = None
        column_costs: list[Decimal] = []
        column_quantities: list[Decimal] = []
        expected_column_cost: Decimal | None = None
        expected_column_quantity: Decimal | None = None
        expected_column_mismatch = False

        def finish_compatibility_column() -> None:
            if current_column is None:
                return
            if (
                expected_column_cost is None
                or expected_column_quantity is None
                or expected_column_mismatch
                or preview_sum_decimals(column_costs) != expected_column_cost
                or preview_sum_decimals(column_quantities) != expected_column_quantity
            ):
                raise PreviewFinancialReconciliationError("exact source columns do not reconcile with compatibility")

        compatibility_rows = self._connection.execute(
            """
            SELECT compatibility_origin_key, portion_ordinal,
                   allocated_cost, allocated_quantity,
                   compatibility_allocated_cost, compatibility_allocated_quantity
            FROM allocations
            WHERE compatibility_allocated_cost IS NOT NULL
              AND compatibility_allocated_quantity IS NOT NULL
            ORDER BY compatibility_origin_key, portion_ordinal, allocation_order
            """
        )
        for (
            compatibility_origin,
            portion_ordinal,
            allocated_cost_text,
            allocated_quantity_text,
            compatibility_cost_text,
            compatibility_quantity_text,
        ) in compatibility_rows:
            column = str(compatibility_origin), int(portion_ordinal)
            compatibility_cost = Decimal(str(compatibility_cost_text))
            compatibility_quantity = Decimal(str(compatibility_quantity_text))
            if column != current_column:
                finish_compatibility_column()
                current_column = column
                column_costs = []
                column_quantities = []
                expected_column_cost = compatibility_cost
                expected_column_quantity = compatibility_quantity
                expected_column_mismatch = False
            elif compatibility_cost != expected_column_cost or compatibility_quantity != expected_column_quantity:
                expected_column_mismatch = True
            column_costs.append(Decimal(str(allocated_cost_text)))
            column_quantities.append(Decimal(str(allocated_quantity_text)))
        finish_compatibility_column()

        current_origin: str | None = None
        current_aggregate: PreviewAggregateEvidence | None = None
        expected_ordinal = 0
        structure_rows = self._connection.execute(
            "SELECT origin_key, payload FROM allocations ORDER BY origin_key, portion_ordinal, allocation_order"
        )
        for origin_value, payload in structure_rows:
            origin = str(origin_value)
            allocation = pickle.loads(bytes(payload))
            if not isinstance(allocation, PreviewAllocationEvidence):
                raise PreviewAllocationLineageError("allocation spool is invalid")
            if origin != current_origin:
                if current_origin is not None and expected_ordinal == 0:
                    raise PreviewAllocationLineageError("allocation lineage has no portions")
                aggregate_row = self._connection.execute(
                    "SELECT payload FROM aggregates WHERE origin_key = ?",
                    (origin,),
                ).fetchone()
                assert aggregate_row is not None
                decoded = pickle.loads(bytes(aggregate_row[0]))
                if not isinstance(decoded, PreviewAggregateEvidence):
                    raise PreviewAllocationLineageError("aggregate spool is invalid")
                current_aggregate = decoded
                current_origin = origin
                expected_ordinal = 0
            if allocation.portion_ordinal != expected_ordinal:
                raise PreviewAllocationLineageError("allocation lineage ordinals are invalid")
            expected_ordinal += 1
            assert current_aggregate is not None
            validate_allocation_lineage_portion(aggregate=current_aggregate, allocation=allocation)

        current_origin = None
        current_aggregate = None
        allocated_cost = Decimal(0)
        allocated_quantity = Decimal(0)
        allocated_original_cost = Decimal(0)
        origin_original_cost: Decimal | None = None

        def finish_totals() -> None:
            nonlocal allocated_cost, allocated_original_cost, allocated_quantity, origin_original_cost
            if current_aggregate is not None and (
                allocated_cost != current_aggregate.total_cost
                or allocated_quantity != current_aggregate.quantity
                or origin_original_cost is None
                or allocated_original_cost != origin_original_cost
            ):
                raise PreviewFinancialReconciliationError("allocation lineage totals do not reconcile")
            allocated_cost = Decimal(0)
            allocated_quantity = Decimal(0)
            allocated_original_cost = Decimal(0)
            origin_original_cost = None

        total_rows = self._connection.execute(
            "SELECT origin_key, payload FROM allocations ORDER BY origin_key, portion_ordinal, allocation_order"
        )
        for origin_value, payload in total_rows:
            origin = str(origin_value)
            allocation = pickle.loads(bytes(payload))
            assert isinstance(allocation, PreviewAllocationEvidence)
            if origin != current_origin:
                finish_totals()
                aggregate_row = self._connection.execute(
                    "SELECT payload FROM aggregates WHERE origin_key = ?",
                    (origin,),
                ).fetchone()
                assert aggregate_row is not None
                decoded = pickle.loads(bytes(aggregate_row[0]))
                assert isinstance(decoded, PreviewAggregateEvidence)
                current_aggregate = decoded
                current_origin = origin
            with localcontext(PREVIEW_DECIMAL_CONTEXT):
                allocated_cost += allocation.allocated_cost
                allocated_quantity += allocation.allocated_quantity
                allocated_original_cost += allocation.allocated_original_cost
            if origin_original_cost is None:
                origin_original_cost = allocation.origin_original_cost
            elif origin_original_cost != allocation.origin_original_cost:
                raise PreviewFinancialReconciliationError("allocation lineage totals do not reconcile")
        finish_totals()

    def iter_origins(
        self,
    ) -> Iterator[tuple[str, SelectedSourceProjection, PreviewAggregateEvidence]]:
        rows = self._connection.execute(
            """
            SELECT s.origin_key, s.payload, a.payload
            FROM selected_sources AS s
            JOIN aggregates AS a ON a.origin_key = s.origin_key
            ORDER BY s.origin_key
            """
        )
        for origin, selected_payload, aggregate_payload in rows:
            selected = pickle.loads(bytes(selected_payload))
            aggregate = pickle.loads(bytes(aggregate_payload))
            if not isinstance(selected, SelectedSourceProjection) or not isinstance(
                aggregate, PreviewAggregateEvidence
            ):
                raise PreviewSourceCoverageError("source and aggregate spool is invalid")
            yield str(origin), selected, aggregate

    def iter_origin_batches(
        self,
        *,
        batch_size: int = SQLITE_BATCH_SIZE,
    ) -> Iterator[tuple[tuple[str, SelectedSourceProjection, PreviewAggregateEvidence], ...]]:
        if batch_size <= 0:
            raise ValueError("origin batch size must be positive")
        rows = self._connection.execute(
            """
            SELECT s.origin_key, s.payload, a.payload
            FROM selected_sources AS s
            JOIN aggregates AS a ON a.origin_key = s.origin_key
            ORDER BY s.origin_key
            """
        )
        while raw_batch := rows.fetchmany(batch_size):
            batch: list[tuple[str, SelectedSourceProjection, PreviewAggregateEvidence]] = []
            for origin, selected_payload, aggregate_payload in raw_batch:
                selected = pickle.loads(bytes(selected_payload))
                aggregate = pickle.loads(bytes(aggregate_payload))
                if not isinstance(selected, SelectedSourceProjection) or not isinstance(
                    aggregate, PreviewAggregateEvidence
                ):
                    raise PreviewSourceCoverageError("source and aggregate spool is invalid")
                batch.append((str(origin), selected, aggregate))
            yield tuple(batch)

    def iter_allocations(self, origin: str) -> Iterator[PreviewAllocationEvidence]:
        rows = self._connection.execute(
            "SELECT payload FROM allocations WHERE origin_key = ? ORDER BY portion_ordinal",
            (origin,),
        )
        for (payload,) in rows:
            allocation = pickle.loads(bytes(payload))
            if not isinstance(allocation, PreviewAllocationEvidence):
                raise PreviewAllocationLineageError("allocation spool is invalid")
            yield allocation

    def iter_allocation_batches(
        self,
        origin: str,
        *,
        batch_size: int = SQLITE_BATCH_SIZE,
    ) -> Iterator[tuple[PreviewAllocationEvidence, ...]]:
        if batch_size <= 0:
            raise ValueError("allocation batch size must be positive")
        rows = self._connection.execute(
            "SELECT payload FROM allocations WHERE origin_key = ? ORDER BY portion_ordinal",
            (origin,),
        )
        while raw_batch := rows.fetchmany(batch_size):
            batch: list[PreviewAllocationEvidence] = []
            for (payload,) in raw_batch:
                allocation = pickle.loads(bytes(payload))
                if not isinstance(allocation, PreviewAllocationEvidence):
                    raise PreviewAllocationLineageError("allocation spool is invalid")
                batch.append(allocation)
            yield tuple(batch)

    def target_ids_for_origins(
        self,
        origins: tuple[str, ...],
        *,
        limit: int = SQLITE_BATCH_SIZE,
    ) -> tuple[set[str], set[str]] | None:
        if not origins:
            return set(), set()
        placeholders = ", ".join("?" for _ in origins)
        rows = self._connection.execute(
            f"""
            SELECT target_kind, target_id
            FROM allocations
            WHERE origin_key IN ({placeholders}) AND target_id IS NOT NULL
            ORDER BY origin_key, portion_ordinal
            """,  # noqa: S608 - placeholders are generated solely from bounded origin count
            origins,
        )
        resource_ids: set[str] = set()
        identity_ids: set[str] = set()
        for target_kind, target_id in rows:
            if target_kind == "resource":
                resource_ids.add(str(target_id))
            elif target_kind == "identity":
                identity_ids.add(str(target_id))
            if len(resource_ids) + len(identity_ids) > limit:
                return None
        return resource_ids, identity_ids

    def context_ids(
        self,
        *,
        limit: int,
    ) -> tuple[set[str], set[str], set[str]] | None:
        """Return small context-ID sets or signal that bounded batching is required."""

        resource_tag_ids: set[str] = set()
        identity_tag_ids: set[str] = set()
        resource_lookup_ids: set[str] = set()
        selected_rows = self._connection.execute("SELECT payload FROM selected_sources ORDER BY source_order")
        for (payload,) in selected_rows:
            selected = pickle.loads(bytes(payload))
            if not isinstance(selected, SelectedSourceProjection):
                raise PreviewSourceCoverageError("source spool is invalid")
            source = selected.source
            if source.resource_id is not None:
                resource_tag_ids.add(source.resource_id)
                resource_lookup_ids.add(source.resource_id)
            if source.environment_id is not None:
                resource_lookup_ids.add(source.environment_id)
            if len(resource_tag_ids | identity_tag_ids | resource_lookup_ids) > limit:
                return None
        allocation_rows = self._connection.execute("SELECT payload FROM allocations ORDER BY allocation_order")
        for (payload,) in allocation_rows:
            allocation = pickle.loads(bytes(payload))
            if not isinstance(allocation, PreviewAllocationEvidence):
                raise PreviewAllocationLineageError("allocation spool is invalid")
            if allocation.target_id is not None and allocation.target_kind == "resource":
                resource_tag_ids.add(allocation.target_id)
                resource_lookup_ids.add(allocation.target_id)
            elif allocation.target_id is not None and allocation.target_kind == "identity":
                identity_tag_ids.add(allocation.target_id)
            if len(resource_tag_ids | identity_tag_ids | resource_lookup_ids) > limit:
                return None
        return resource_tag_ids, identity_tag_ids, resource_lookup_ids

    def _sum_decimal_column(self, table: str, column: str) -> Decimal:
        rows = self._connection.execute(
            f"SELECT {column} FROM {table} ORDER BY {column} COLLATE preview_decimal"  # noqa: S608
        )
        with localcontext(PREVIEW_DECIMAL_CONTEXT):
            return sum((Decimal(str(row[0])) for row in rows), Decimal(0))

    @property
    def reconciliation(self) -> PreviewPackageReconciliation:
        return PreviewPackageReconciliation(
            self._selected_count,
            self._sum_decimal_column("selected_sources", "source_cost"),
            self._sum_decimal_column("allocations", "allocated_cost"),
            self._sum_decimal_column("selected_sources", "source_quantity"),
            self._sum_decimal_column("allocations", "allocated_quantity"),
        )

    @property
    def source_through(self) -> datetime | None:
        row = self._connection.execute("SELECT MAX(source_through) FROM selected_sources").fetchone()
        if row is None or row[0] is None:
            return None
        return datetime.strptime(str(row[0]), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)

    @property
    def source_count(self) -> int:
        return self._selected_count

    def finish(self) -> PreviewGenerationWorkspace:
        if not self._closed:
            self._commit_and_enforce(force=True)
            self._connection_context.__exit__(None, None, None)
            self._closed = True
        self.path.unlink(missing_ok=True)
        self.workspace.enforce_limit()
        self._transferred = True
        return self.workspace

    def close(self) -> None:
        if not getattr(self, "_closed", True):
            self._connection_context.__exit__(None, None, None)
            self._closed = True
        if not self._transferred:
            self.workspace.close()

    def __del__(self) -> None:
        self.close()


class PreviewPackageGenerator:
    def __init__(
        self,
        *,
        max_csv_file_bytes: int | None,
        max_generation_spool_bytes: int = 2_147_483_648,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._max_csv_file_bytes = max_csv_file_bytes
        self._max_generation_spool_bytes = max_generation_spool_bytes
        self._clock = clock

    @property
    def max_generation_spool_bytes(self) -> int:
        return self._max_generation_spool_bytes

    def _build_data_package(
        self,
        *,
        request: PreviewRequest,
        snapshot: PreviewSourceSnapshot,
        full_rows: Iterable[PreviewFullRow],
        reconciliation: PreviewPackageReconciliation,
        workspace: PreviewGenerationWorkspace | None = None,
    ) -> PreviewDataPackageDraft:
        try:
            if build_preview_data_package is not _LEGACY_BUILD_PREVIEW_DATA_PACKAGE:
                draft = build_preview_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=full_rows,
                    reconciliation=reconciliation,
                    max_csv_file_bytes=self._max_csv_file_bytes,
                )
                return replace(draft, _workspace=workspace) if workspace is not None else draft
            return build_bounded_preview_data_package(
                request=request,
                snapshot=snapshot,
                full_rows=full_rows,
                reconciliation=reconciliation,
                max_csv_file_bytes=self._max_csv_file_bytes,
                max_generation_spool_bytes=self._max_generation_spool_bytes,
                workspace=workspace,
            )
        except PreviewGenerationSpoolLimitError:
            raise _failure(
                "preview_generation_spool_limit_exceeded",
                "FOCUS Mapping Preview package exceeds the configured generation spool limit.",
                False,
            ) from None

    def generate(
        self,
        *,
        backend: PreviewStorageBackend,
        request: PreviewRequest,
        policy: PreviewEligibilityPolicy,
        workspace: PreviewGenerationWorkspace | None = None,
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
        if not isinstance(backend, PreviewEvidenceStorageBackend):
            raise _evidence_storage_unavailable()
        if evidence_interval.start_date == evidence_interval.end_date:
            with _generation_read_unit_of_work(backend) as uow:
                point = datetime.combine(evidence_interval.start_date, datetime.min.time(), tzinfo=UTC)
                authority_slices = _resolve_source_authority(
                    uow.source_readiness,
                    request.ecosystem,
                    request.tenant_id,
                    point,
                    point,
                )
                source_authority = authority_slices[0].attempt if authority_slices else None
                _require_complete_source_authority(source_authority)
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
                package = self._build_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=(),
                    reconciliation=PreviewPackageReconciliation(0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
                )
                return snapshot, package
        start = datetime.combine(evidence_interval.start_date, datetime.min.time(), tzinfo=UTC)
        end = datetime.combine(evidence_interval.end_date, datetime.min.time(), tzinfo=UTC)
        scope = PreviewEvidenceScope(request.ecosystem, request.tenant_id, start, end)
        with _generation_read_unit_of_work(backend) as uow:
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
            authority_slices = _resolve_source_authority(
                uow.source_readiness,
                request.ecosystem,
                request.tenant_id,
                scope.start,
                scope.end,
            )
            readiness = uow.source_readiness.list_covering(
                request.ecosystem,
                request.tenant_id,
                scope.start,
                scope.end,
            )
            readiness_by_window = {
                (item.attempt_sequence, item.window_start, item.window_end): item for item in readiness
            }
            readiness_by_attempt: dict[int, list[PreviewSourceReadiness]] = {}
            for item in readiness:
                readiness_by_attempt.setdefault(item.attempt_sequence, []).append(item)
            readiness_index = {
                attempt_sequence: (
                    tuple(items),
                    tuple(item.window_start for item in items),
                )
                for attempt_sequence, items in readiness_by_attempt.items()
            }
            effective_readiness: list[PreviewSourceReadiness] = []
            for authority_slice in authority_slices:
                _require_complete_source_authority(authority_slice.attempt)
                if authority_slice.attempt is None:
                    continue
                indexed = readiness_index.get(authority_slice.attempt.attempt_sequence)
                covering = (
                    None
                    if indexed is None
                    else _readiness_covering_slice(
                        authority_slice,
                        indexed[0],
                        indexed[1],
                    )
                )
                if covering is None:
                    raise _failure(
                        "preview_source_evidence_unavailable",
                        "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                        "run the pipeline and retry.",
                        True,
                    )
                effective_readiness.extend(covering)
            authority_starts = tuple(item.start for item in authority_slices)
            try:
                evidence_spool = _PreviewEvidenceSpool(
                    limit_bytes=self._max_generation_spool_bytes,
                    workspace=workspace,
                )
            except PreviewGenerationSpoolLimitError:
                raise _failure(
                    "preview_generation_spool_limit_exceeded",
                    "FOCUS Mapping Preview package exceeds the configured generation spool limit.",
                    False,
                ) from None
            winning_issue: PreviewSourceIssue | None = None
            issue_correlations: tuple[str, ...] = ()
            valid_correlations: tuple[str, ...] = ()
            unsupported_provider_context = False
            for candidate in uow.cost_evidence.iter_preview_sources(scope):
                if (
                    candidate.collection_window_start.tzinfo is None
                    or candidate.collection_window_start.utcoffset() is None
                    or candidate.collection_window_end.tzinfo is None
                    or candidate.collection_window_end.utcoffset() is None
                    or candidate.collection_window_start >= candidate.collection_window_end
                ):
                    raise _failure(
                        "preview_source_evidence_unavailable",
                        "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                        "run the pipeline and retry.",
                        True,
                    )
                matching_authority = _authority_for_window(
                    authority_slices,
                    authority_starts,
                    max(candidate.collection_window_start, scope.start),
                    min(candidate.collection_window_end, scope.end),
                )
                if matching_authority is None:
                    raise _failure(
                        "preview_source_evidence_unavailable",
                        "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                        "run the pipeline and retry.",
                        True,
                    )
                window_readiness = readiness_by_window.get(
                    (
                        matching_authority.attempt_sequence,
                        candidate.collection_window_start,
                        candidate.collection_window_end,
                    )
                )
                if window_readiness is None or candidate.capture_id != window_readiness.capture_id:
                    raise _failure(
                        "preview_source_evidence_unavailable",
                        "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                        "run the pipeline and retry.",
                        True,
                    )
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
                rule = FOCUS_1_4_SERVICE_RULES_V1[classification.semantics.service_rule_key]
                unsupported_provider_context = (
                    unsupported_provider_context or rule.context_strategy == "unsupported_provider_context"
                )
                evidence_spool.add_selected(SelectedSourceProjection(candidate, classification.semantics, financials))
                valid_correlations = capped_correlations([*valid_correlations, correlation])
            if winning_issue is not None:
                raise PreviewGenerationError(source_issue_diagnostic(winning_issue, issue_correlations))
            if any(item.attempt is None for item in authority_slices) and evidence_spool.source_count:
                raise _failure(
                    "preview_source_evidence_unavailable",
                    "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                    "run the pipeline and retry.",
                    True,
                )
            if unsupported_provider_context:
                raise _mapping_failure(
                    PreviewProviderContextIncompleteError("TABLEFLOW provider context is unavailable"),
                    valid_correlations,
                )

            try:
                for aggregate in uow.cost_evidence.iter_preview_aggregates(scope):
                    evidence_spool.add_aggregate(aggregate)
                evidence_spool.reconcile_sources(request)
            except PreviewSourceCoverageError:
                raise _failure(
                    "preview_source_coverage_incomplete",
                    "Persisted source evidence does not completely cover the calculated Preview scope.",
                    False,
                    valid_correlations,
                ) from None
            except PreviewMappingScopeError as exc:
                raise _mapping_failure(exc, valid_correlations) from None
            calculation_ids = tuple(entry.calculation_id for entry in coverage.entries)
            expected_completion_by_run = {
                (entry.tracking_date, entry.calculation_id): entry.calculation_completed_at
                for entry in coverage.entries
            }
            try:

                def lineage_runs() -> Iterator[PreviewAllocationRunEvidence]:
                    for item in uow.allocation_evidence.iter_preview_allocation_runs(scope, calculation_ids):
                        if item.capture_status is AllocationLineageRunStatus.UNAVAILABLE:
                            raise _failure(
                                "preview_allocation_lineage_unavailable",
                                "Allocation lineage capture is unavailable for one or more requested calculations.",
                                True,
                                valid_correlations,
                            )
                        yield item

                evidence_spool.add_and_reconcile_allocations(
                    expected_completion_by_run=expected_completion_by_run,
                    runs=lineage_runs(),
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
                source_through=evidence_spool.source_through,
                effective_coverage_start_date=evidence_interval.start_date,
                effective_coverage_end_date=evidence_interval.end_date,
                availability_cutoff_end_date=(policy.acquisition_end_date if request.grain == "monthly" else None),
                monthly_status=monthly_status,
            )
            if not evidence_spool.source_count:
                if any(item.source_count for item in effective_readiness):
                    raise _failure(
                        "preview_source_evidence_unavailable",
                        "Native Confluent Cloud source evidence is unavailable for the requested scope; "
                        "run the pipeline and retry.",
                        True,
                    )
                package = self._build_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=(),
                    reconciliation=PreviewPackageReconciliation(0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
                    workspace=evidence_spool.workspace,
                )
                transferred_workspace = evidence_spool.finish()
                if package._workspace is None:
                    transferred_workspace.close()
                return snapshot, package
            organization_authority = uow.organization_authority.get_latest(request.ecosystem, request.tenant_id)
            if organization_authority is not None and organization_authority.status in {
                OrganizationAuthorityAttemptStatus.PENDING,
                OrganizationAuthorityAttemptStatus.UNAVAILABLE,
            }:
                raise _mapping_failure(PreviewBillingAccountUnavailableError(), valid_correlations)
            if (
                organization_authority is not None
                and organization_authority.status is OrganizationAuthorityAttemptStatus.CONFLICTING
            ):
                raise _mapping_failure(PreviewBillingAccountConflictError(), valid_correlations)
            organizations, _ = uow.resources.find_active_at(
                request.ecosystem,
                request.tenant_id,
                current_time,
                resource_type="organization",
                limit=3,
                count=False,
            )
            if not organizations:
                error = (
                    PreviewBillingAccountConflictError()
                    if organization_authority is not None
                    and organization_authority.status is OrganizationAuthorityAttemptStatus.AVAILABLE
                    else PreviewBillingAccountUnavailableError()
                )
                raise _mapping_failure(error, valid_correlations)
            if len(organizations) != 1 or organizations[0].metadata.get("organization_binding_state") != "bound":
                raise _mapping_failure(PreviewBillingAccountConflictError(), valid_correlations)
            organization = organizations[0]
            if not organization.resource_id.strip():
                error = (
                    PreviewBillingAccountUnavailableError()
                    if organization_authority is None
                    else PreviewBillingAccountConflictError()
                )
                raise _mapping_failure(error, valid_correlations)
            if (
                organization_authority is not None
                and organization.resource_id != organization_authority.organization_id
            ):
                raise _mapping_failure(PreviewBillingAccountConflictError(), valid_correlations)
            provider_context = PreviewProviderContext(organization.resource_id, organization.display_name)
            try:

                def package_rows() -> Iterator[PreparedPreviewPackageRow]:
                    def load_resources(resource_ids: set[str]) -> dict[str, Resource]:
                        loaded: dict[str, Resource] = {}
                        for id_batch in batched(sorted(resource_ids), SQLITE_BATCH_SIZE, strict=False):
                            loaded.update(
                                uow.resources.get_many(
                                    request.ecosystem,
                                    request.tenant_id,
                                    list(id_batch),
                                )
                            )
                        return loaded

                    def load_identities(identity_ids: set[str]) -> dict[str, Identity]:
                        loaded: dict[str, Identity] = {}
                        for id_batch in batched(sorted(identity_ids), SQLITE_BATCH_SIZE, strict=False):
                            loaded.update(
                                uow.identities.get_many(
                                    request.ecosystem,
                                    request.tenant_id,
                                    list(id_batch),
                                )
                            )
                        return loaded

                    def load_tags(entity_type: str, entity_ids: set[str]) -> dict[str, list[EntityTag]]:
                        loaded: dict[str, list[EntityTag]] = {}
                        for id_batch in batched(sorted(entity_ids), SQLITE_BATCH_SIZE, strict=False):
                            loaded.update(
                                uow.tags.find_tags_for_entities(
                                    request.tenant_id,
                                    entity_type,
                                    list(id_batch),
                                )
                            )
                        return loaded

                    for origin_batch in evidence_spool.iter_origin_batches():
                        base_resource_ids = {
                            resource_id
                            for _origin, selected, _aggregate in origin_batch
                            for resource_id in (selected.source.resource_id, selected.source.environment_id)
                            if resource_id is not None
                        }
                        resource_by_id = load_resources(base_resource_ids)
                        auxiliary_resource_ids: set[str] = set()
                        for _origin, selected, _aggregate in origin_batch:
                            source_resource_id = selected.source.resource_id
                            origin_resource = (
                                resource_by_id.get(source_resource_id) if source_resource_id is not None else None
                            )
                            if origin_resource is None:
                                continue
                            if origin_resource.parent_id is not None:
                                auxiliary_resource_ids.add(origin_resource.parent_id)
                            for metadata_key in ("kafka_cluster_id", "compute_pool_id"):
                                metadata_value = origin_resource.metadata.get(metadata_key)
                                if isinstance(metadata_value, str):
                                    auxiliary_resource_ids.add(metadata_value)
                        resource_by_id.update(load_resources(auxiliary_resource_ids.difference(resource_by_id)))
                        origin_tag_ids = {
                            selected.source.resource_id
                            for _origin, selected, _aggregate in origin_batch
                            if selected.source.resource_id is not None
                        }
                        target_context = evidence_spool.target_ids_for_origins(
                            tuple(origin for origin, _selected, _aggregate in origin_batch)
                        )
                        prefetched_target_resources: dict[str, Resource] = {}
                        prefetched_target_identities: dict[str, Identity] = {}
                        prefetched_identity_tags: dict[str, list[EntityTag]] = {}
                        if target_context is not None:
                            resource_target_ids, identity_target_ids = target_context
                            origin_tag_ids.update(resource_target_ids)
                            prefetched_target_resources = load_resources(resource_target_ids.difference(resource_by_id))
                            prefetched_target_identities = load_identities(identity_target_ids)
                        origin_tags_by_id = load_tags("resource", origin_tag_ids)
                        if target_context is not None:
                            prefetched_identity_tags = load_tags("identity", identity_target_ids)

                        for origin, selected, aggregate in origin_batch:
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
                                raise PreviewProviderContextIncompleteError(
                                    "source environment authority is incompatible"
                                )
                            resource_context = resolve_provider_resource_context_from_mapping(
                                source=source,
                                semantics=selected.semantics,
                                origin_resource=origin_resource,
                                resources=uow.resources,
                                resource_by_id=resource_by_id,
                            )
                            origin_tags_json = _tags_json(
                                origin_tags_by_id.get(source.resource_id, []) if source.resource_id is not None else []
                            )
                            for allocation_batch in evidence_spool.iter_allocation_batches(origin):
                                resource_target_ids = {
                                    item.target_id
                                    for item in allocation_batch
                                    if item.target_id is not None and item.target_kind == "resource"
                                }
                                identity_target_ids = {
                                    item.target_id
                                    for item in allocation_batch
                                    if item.target_id is not None and item.target_kind == "identity"
                                }
                                target_resources = (
                                    prefetched_target_resources
                                    if target_context is not None
                                    else load_resources(resource_target_ids.difference(resource_by_id))
                                )
                                target_identities = (
                                    prefetched_target_identities
                                    if target_context is not None
                                    else load_identities(identity_target_ids)
                                )
                                resource_tags = (
                                    origin_tags_by_id
                                    if target_context is not None
                                    else {
                                        **{
                                            target_id: origin_tags_by_id[target_id]
                                            for target_id in resource_target_ids
                                            if target_id in origin_tags_by_id
                                        },
                                        **load_tags(
                                            "resource",
                                            resource_target_ids.difference(origin_tags_by_id),
                                        ),
                                    }
                                )
                                identity_tags = (
                                    prefetched_identity_tags
                                    if target_context is not None
                                    else load_tags("identity", identity_target_ids)
                                )
                                for allocation in allocation_batch:
                                    target_id = allocation.target_id
                                    allocated_entity: Identity | Resource | None = None
                                    allocated_tags = None
                                    if target_id is not None and allocation.target_kind == "resource":
                                        allocated_entity = resource_by_id.get(target_id) or target_resources.get(
                                            target_id
                                        )
                                        allocated_tags = _tags_json(resource_tags.get(target_id, []))
                                    elif target_id is not None and allocation.target_kind == "identity":
                                        allocated_entity = target_identities.get(target_id)
                                        allocated_tags = _tags_json(identity_tags.get(target_id, []))
                                    allocated_tags = (
                                        None
                                        if allocation.target_kind == "unallocated"
                                        else allocated_tags
                                        if target_id is not None
                                        else "{}"
                                    )
                                    financials = project_allocated_financials(
                                        selected=selected,
                                        allocation=allocation,
                                    )
                                    yield PreparedPreviewPackageRow(
                                        evidence=SelectedPreviewEvidence(
                                            SelectedSourceProjection(
                                                source,
                                                selected.semantics,
                                                financials,
                                            ),
                                            aggregate,
                                            allocation,
                                        ),
                                        resource_context=resource_context,
                                        allocated_entity=allocated_entity,
                                        environment=environment,
                                        origin_tags_json=origin_tags_json,
                                        allocated_tags_json=allocated_tags,
                                    )

                full_rows: Iterable[PreviewFullRow] = (
                    project_daily_portion_full_row(prepared=prepared, provider_context=provider_context)
                    for prepared in package_rows()
                )
                if request.grain == "monthly":
                    month_start = datetime.combine(request.start_date, datetime.min.time(), tzinfo=UTC)
                    month_end = datetime.combine(request.end_date, datetime.min.time(), tzinfo=UTC)
                    full_rows = (
                        aggregate_monthly_full_rows(
                            rows=full_rows,
                            month_start=month_start,
                            month_end=month_end,
                        )
                        if aggregate_monthly_full_rows is not _LEGACY_AGGREGATE_MONTHLY_FULL_ROWS
                        else aggregate_monthly_full_rows_bounded(
                            rows=full_rows,
                            month_start=month_start,
                            month_end=month_end,
                            workspace=evidence_spool.workspace,
                        )
                    )
                package = self._build_data_package(
                    request=request,
                    snapshot=snapshot,
                    full_rows=full_rows,
                    reconciliation=evidence_spool.reconciliation,
                    workspace=evidence_spool.workspace,
                )
                transferred_workspace = evidence_spool.finish()
                if package._workspace is None:
                    transferred_workspace.close()
            except PreviewMappingError as exc:
                evidence_spool.close()
                raise _mapping_failure(exc, valid_correlations) from exc
            except BaseException:
                evidence_spool.close()
                raise
        return snapshot, package
