from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Context, Decimal, localcontext
from typing import TYPE_CHECKING, Literal, cast

from sqlalchemy import case, delete, exists, func, or_, update
from sqlmodel import Session, col, select

from core.preview.evidence import (
    AllocationLineageRun,
    AllocationLineageRunStatus,
    AllocationLineageUnavailableRun,
    PreviewSourceAttempt,
    PreviewSourceAttemptOrigin,
    PreviewSourceAuthoritySlice,
    PreviewSourceEvidence,
    PreviewSourceReadiness,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
    normalize_preview_source_economics,
    source_attempt_origin,
)
from core.preview.evidence_capture import (
    NativeSourceEvidenceCapture,
    NativeSourceWindow,
    PreviewEvidenceBootstrapConflictError,
    SourceAttemptBeginFailure,
    SourceWindowWriteResult,
)
from core.preview.models import PreviewDiagnostic
from core.preview.organization_authority import (
    OrganizationAuthorityAttempt,
    OrganizationAuthorityAttemptStatus,
    OrganizationAuthorityFailureReason,
    OrganizationAuthorityFinalStatus,
    PreviewOrganizationAuthorityConflictError,
    PreviewOrganizationAuthorityDecodeError,
)
from core.preview.repair import (
    PreviewRepair,
    PreviewRepairDate,
    PreviewRepairDateStatus,
    PreviewRepairFailureStage,
    PreviewRepairHistoryUnresolved,
    PreviewRepairProgress,
    PreviewRepairStatus,
)
from core.preview.retention import (
    PreviewRetentionCleanupKind,
    PreviewRetentionDiagnostic,
    PreviewRetentionOutcome,
    PreviewRetentionOutcomeSet,
    PreviewRetentionOutcomeStatus,
)
from core.preview.storage_availability import PreviewEvidenceSchemaError
from core.storage.backends.sqlmodel.tables import PipelineStateTable
from core.storage.backends.sqlmodel.timestamps import (
    canonical_utc_second,
    exclusive_utc_second_upper_bound,
)
from plugins.confluent_cloud.storage.preview_tables import (
    CCloudAllocationLineagePortionTable,
    CCloudAllocationLineageRunTable,
    CCloudCostSourceRecordTable,
    CCloudFocusPreviewRepairDateTable,
    CCloudFocusPreviewRepairHeadTable,
    CCloudFocusPreviewRepairTable,
    CCloudFocusPreviewRetentionOutcomeTable,
    CCloudOrganizationAuthorityAttemptTable,
    CCloudPreviewSourceAllocationLineagePortionTable,
    CCloudSourceCaptureReadinessHistoryTable,
    CCloudSourceCaptureReadinessTable,
    CCloudSourceEvidenceAttemptTable,
)
from plugins.confluent_cloud.storage.repositories import (
    CCloudBillingRepository,
    CCloudChargebackRepository,
    _source_table_to_preview,
)
from plugins.confluent_cloud.storage.tables import CCloudBillingTable

logger = logging.getLogger(__name__)

_APPORTION_CONTEXT = Context(prec=64)
_PREVIEW_RATIO_CONTEXT = Context(prec=38)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from core.preview.persistence import LineageDeletionCount
    from core.storage.interface import AllocationLineageRunCapture


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return canonical_utc_second(value)


def _require_aware(value: datetime, name: str) -> datetime:
    return canonical_utc_second(value, field=name)


def _decimal_text(value: Decimal) -> str:
    result = format(value, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return str(Decimal(result or "0"))


def _quantum(values: Sequence[Decimal], *, minimum_places: int = 0) -> Decimal:
    exponent = min(
        (value.as_tuple().exponent for value in values if value.is_finite() and value != 0),
        default=0,
    )
    return Decimal(1).scaleb(min(cast("int", exponent), -minimum_places))


def _integer_units(values: Sequence[Decimal], quantum: Decimal) -> tuple[int, ...]:
    units: list[int] = []
    with localcontext(_APPORTION_CONTEXT):
        for value in values:
            if not value.is_finite():
                raise ValueError("allocation margin is nonfinite")
            integral = value / quantum
            if integral != integral.to_integral_value():
                raise ValueError("allocation margin is not representable at the selected quantum")
            units.append(int(integral))
    return tuple(units)


def _largest_remainder(total: int, weights: Sequence[int], ordinals: Sequence[int]) -> tuple[int, ...]:
    if total < 0 or len(weights) != len(ordinals) or any(weight < 0 for weight in weights):
        raise ValueError("invalid largest-remainder inputs")
    if not weights:
        if total:
            raise ValueError("cannot apportion a nonzero total without columns")
        return ()
    weight_total = sum(weights)
    effective = tuple(weights) if weight_total else tuple(1 for _ in weights)
    denominator = sum(effective)
    bases = [total * weight // denominator for weight in effective]
    remainder_units = total - sum(bases)
    ranked = sorted(
        range(len(effective)),
        key=lambda index: (-(total * effective[index] % denominator), ordinals[index]),
    )
    for index in ranked[:remainder_units]:
        bases[index] += 1
    return tuple(bases)


def _transport_unsigned(
    row_margins: Sequence[int],
    column_margins: Sequence[int],
    ordinals: Sequence[int],
) -> tuple[tuple[int, ...], ...]:
    if (
        len(column_margins) != len(ordinals)
        or any(value < 0 for value in (*row_margins, *column_margins))
        or sum(row_margins) != sum(column_margins)
    ):
        raise ValueError("invalid transportation margins")
    if not row_margins:
        return ()
    capacities = list(column_margins)
    remaining_total = sum(capacities)
    matrix: list[tuple[int, ...]] = []
    for row_index, row_total in enumerate(row_margins):
        if row_index == len(row_margins) - 1:
            matrix.append(tuple(capacities))
            break
        if row_total == 0:
            matrix.append(tuple(0 for _ in capacities))
            continue
        if remaining_total <= 0:
            raise ValueError("transportation capacity is exhausted")
        bases = [row_total * capacity // remaining_total for capacity in capacities]
        leftover = row_total - sum(bases)
        ranked = sorted(
            range(len(capacities)),
            key=lambda index: (
                -(row_total * capacities[index] % remaining_total),
                ordinals[index],
            ),
        )
        for index in ranked:
            if leftover == 0:
                break
            if bases[index] < capacities[index]:
                bases[index] += 1
                leftover -= 1
        if leftover:
            raise ValueError("transportation remainder exceeds capacity")
        for index, value in enumerate(bases):
            capacities[index] -= value
        remaining_total -= row_total
        matrix.append(tuple(bases))
    return tuple(matrix)


def _apportion_signed(
    source_margins: Sequence[Decimal],
    generic_margins: Sequence[Decimal],
    portion_ordinals: Sequence[int],
    *,
    minimum_places: int = 0,
) -> tuple[tuple[Decimal, ...], ...]:
    if len(generic_margins) != len(portion_ordinals):
        raise ValueError("allocation columns and ordinals differ")
    with localcontext(_APPORTION_CONTEXT):
        if sum(source_margins, Decimal(0)) != sum(generic_margins, Decimal(0)):
            raise ValueError("source and generic allocation margins differ")
    quantum = _quantum((*source_margins, *generic_margins), minimum_places=minimum_places)
    source_units = _integer_units(source_margins, quantum)
    generic_units = _integer_units(generic_margins, quantum)
    total = sum(source_units)
    if not source_units:
        return ()
    if total == 0:
        if any(source_units) or any(generic_units):
            raise ValueError("zero-net signed margins require the quantity bridge")
        return tuple(tuple(Decimal(0) for _ in generic_units) for _ in source_units)
    if any(value and (value > 0) != (total > 0) for value in generic_units):
        raise ValueError("generic allocation columns have incompatible signs")

    positive_indexes = [index for index, value in enumerate(source_units) if value > 0]
    negative_indexes = [index for index, value in enumerate(source_units) if value < 0]
    positive_rows = [source_units[index] for index in positive_indexes]
    negative_rows = [-source_units[index] for index in negative_indexes]
    generic_magnitudes = [abs(value) for value in generic_units]
    if total > 0:
        negative_columns = _largest_remainder(sum(negative_rows), generic_magnitudes, portion_ordinals)
        positive_columns = tuple(
            generic_magnitudes[index] + negative_columns[index] for index in range(len(generic_units))
        )
    else:
        positive_columns = _largest_remainder(sum(positive_rows), generic_magnitudes, portion_ordinals)
        negative_columns = tuple(
            generic_magnitudes[index] + positive_columns[index] for index in range(len(generic_units))
        )
    positive_matrix = _transport_unsigned(positive_rows, positive_columns, portion_ordinals)
    negative_matrix = _transport_unsigned(negative_rows, negative_columns, portion_ordinals)
    positive_by_index = dict(zip(positive_indexes, positive_matrix, strict=True))
    negative_by_index = dict(zip(negative_indexes, negative_matrix, strict=True))
    result: list[tuple[Decimal, ...]] = []
    for index, margin in enumerate(source_units):
        units = (
            positive_by_index[index]
            if margin > 0
            else tuple(-value for value in negative_by_index[index])
            if margin < 0
            else tuple(0 for _ in generic_units)
        )
        result.append(tuple(Decimal(value) * quantum for value in units))
    return tuple(result)


def _apportion_zero_net_quantity(
    *,
    source_quantities: Sequence[Decimal],
    generic_allocated_costs: Sequence[Decimal],
    portion_ordinals: Sequence[int],
) -> tuple[tuple[Decimal, ...], ...]:
    if len(generic_allocated_costs) != len(portion_ordinals):
        raise ValueError("quantity bridge columns and ordinals differ")
    quantum = _quantum(source_quantities)
    source_units = _integer_units(source_quantities, quantum)
    if sum(source_units) != 0:
        raise ValueError("quantity bridge requires a zero-net source total")
    if not any(source_units):
        return tuple(tuple(Decimal(0) for _ in generic_allocated_costs) for _ in source_units)
    positive_indexes = [index for index, value in enumerate(source_units) if value > 0]
    negative_indexes = [index for index, value in enumerate(source_units) if value < 0]
    positive_rows = [source_units[index] for index in positive_indexes]
    negative_rows = [-source_units[index] for index in negative_indexes]
    if sum(positive_rows) != sum(negative_rows):
        raise ValueError("quantity bridge signs do not cancel")
    cost_quantum = _quantum(generic_allocated_costs)
    weights = [abs(value) for value in _integer_units(generic_allocated_costs, cost_quantum)]
    bridge = _largest_remainder(sum(positive_rows), weights, portion_ordinals)
    positive_matrix = _transport_unsigned(positive_rows, bridge, portion_ordinals)
    negative_matrix = _transport_unsigned(negative_rows, bridge, portion_ordinals)
    positive_by_index = dict(zip(positive_indexes, positive_matrix, strict=True))
    negative_by_index = dict(zip(negative_indexes, negative_matrix, strict=True))
    result: list[tuple[Decimal, ...]] = []
    for index, margin in enumerate(source_units):
        units = (
            positive_by_index[index]
            if margin > 0
            else tuple(-value for value in negative_by_index[index])
            if margin < 0
            else tuple(0 for _ in generic_allocated_costs)
        )
        result.append(tuple(Decimal(value) * quantum for value in units))
    return tuple(result)


def _apportion_original_cost(
    original: Decimal,
    billed_cells: Sequence[Decimal],
    portion_ordinals: Sequence[int],
) -> tuple[Decimal, ...]:
    if not original.is_finite() or len(billed_cells) != len(portion_ordinals):
        raise ValueError("invalid original-cost apportionment")
    quantum = _quantum((original, *billed_cells), minimum_places=2)
    total_units = abs(_integer_units((original,), quantum)[0])
    billed_quantum = _quantum(billed_cells)
    weights = [abs(value) for value in _integer_units(billed_cells, billed_quantum)]
    apportioned = _largest_remainder(total_units, weights, portion_ordinals)
    sign = -1 if original < 0 else 1
    return tuple(Decimal(sign * value) * quantum for value in apportioned)


def _source_attempt(row: CCloudSourceEvidenceAttemptTable) -> PreviewSourceAttempt:
    if row.attempt_sequence is None:
        raise ValueError("source attempt sequence was not assigned")
    return PreviewSourceAttempt(
        attempt_sequence=row.attempt_sequence,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        refresh_token=row.refresh_token,
        refresh_start=_utc(row.refresh_start),
        refresh_end=_utc(row.refresh_end),
        status=SourceAttemptStatus(row.status),
        started_at=_utc(row.started_at),
        completed_at=None if row.completed_at is None else _utc(row.completed_at),
        failure_reason=None if row.failure_reason is None else SourceAttemptFailureReason(row.failure_reason),
    )


def _readiness(
    row: CCloudSourceCaptureReadinessTable | CCloudSourceCaptureReadinessHistoryTable,
) -> PreviewSourceReadiness:
    return PreviewSourceReadiness(
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        window_start=_utc(row.window_start),
        window_end=_utc(row.window_end),
        capture_id=row.capture_id,
        captured_at=_utc(row.captured_at),
        source_count=row.source_count,
        attempt_sequence=row.attempt_sequence,
    )


def _diagnostic(
    code: str | None,
    message: str | None,
    retryable: bool | None,
    source_correlation_ids_json: str | None = None,
) -> PreviewDiagnostic | None:
    if code is None and message is None and retryable is None:
        return None
    if code is None or message is None or retryable is None:
        raise ValueError("incomplete persisted repair diagnostic")
    correlations: tuple[str, ...] = ()
    if source_correlation_ids_json is not None:
        decoded = json.loads(source_correlation_ids_json)
        if not isinstance(decoded, list) or not all(isinstance(item, str) for item in decoded):
            raise ValueError("invalid persisted repair diagnostic correlations")
        correlations = tuple(decoded)
    return PreviewDiagnostic(
        code=code,
        message=message,
        retryable=retryable,
        source_correlation_ids=correlations,
    )


def _repair_date(row: CCloudFocusPreviewRepairDateTable) -> PreviewRepairDate:
    return PreviewRepairDate(
        repair_id=row.repair_id,
        tracking_date=row.tracking_date,
        status=PreviewRepairDateStatus(row.status),
        started_at=None if row.started_at is None else _utc(row.started_at),
        completed_at=None if row.completed_at is None else _utc(row.completed_at),
        calculation_id=row.calculation_id,
        calculation_completed_at=(None if row.calculation_completed_at is None else _utc(row.calculation_completed_at)),
        rows_written=row.rows_written,
        failure_stage=None if row.failure_stage is None else PreviewRepairFailureStage(row.failure_stage),
        diagnostic=_diagnostic(
            row.diagnostic_code,
            row.diagnostic_message,
            row.diagnostic_retryable,
            row.source_correlation_ids_json,
        ),
    )


def _retention_outcome(
    row: CCloudFocusPreviewRetentionOutcomeTable,
) -> PreviewRetentionOutcome:
    diagnostic_values = (
        row.diagnostic_code,
        row.diagnostic_message,
        row.diagnostic_error_type,
    )
    if all(value is None for value in diagnostic_values):
        diagnostic = None
    elif all(value is not None for value in diagnostic_values):
        diagnostic = PreviewRetentionDiagnostic(
            code=cast("str", row.diagnostic_code),
            message=cast("str", row.diagnostic_message),
            error_type=cast("str", row.diagnostic_error_type),
        )
    else:
        raise ValueError("incomplete persisted retention diagnostic")
    return PreviewRetentionOutcome(
        owner=row.tenant_id,
        cleanup_kind=PreviewRetentionCleanupKind(row.cleanup_kind),
        attempted_at=_utc(row.attempted_at),
        status=PreviewRetentionOutcomeStatus(row.status),
        diagnostic=diagnostic,
    )


class SQLModelPreviewRetentionOutcomeRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert_latest(
        self,
        ecosystem: str,
        tenant_id: str,
        outcome: PreviewRetentionOutcome,
    ) -> None:
        if not ecosystem.strip() or not tenant_id.strip():
            raise ValueError("retention outcome storage owner must not be blank")
        if outcome.owner != tenant_id:
            raise ValueError("retention outcome owner must match tenant_id")
        values = {
            "attempted_at": _require_aware(outcome.attempted_at, "attempted_at"),
            "status": outcome.status.value,
            "diagnostic_code": None if outcome.diagnostic is None else outcome.diagnostic.code,
            "diagnostic_message": None if outcome.diagnostic is None else outcome.diagnostic.message,
            "diagnostic_error_type": (None if outcome.diagnostic is None else outcome.diagnostic.error_type),
        }
        result = self._session.exec(
            update(CCloudFocusPreviewRetentionOutcomeTable)
            .where(
                col(CCloudFocusPreviewRetentionOutcomeTable.ecosystem) == ecosystem,
                col(CCloudFocusPreviewRetentionOutcomeTable.tenant_id) == tenant_id,
                col(CCloudFocusPreviewRetentionOutcomeTable.cleanup_kind) == outcome.cleanup_kind.value,
            )
            .values(**values)
        )
        if int(getattr(result, "rowcount", 0)) == 0:
            self._session.add(
                CCloudFocusPreviewRetentionOutcomeTable(
                    ecosystem=ecosystem,
                    tenant_id=tenant_id,
                    cleanup_kind=outcome.cleanup_kind.value,
                    **values,
                )
            )
            self._session.flush()

    def get_latest_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRetentionOutcomeSet:
        try:
            rows = self._session.exec(
                select(CCloudFocusPreviewRetentionOutcomeTable).where(
                    col(CCloudFocusPreviewRetentionOutcomeTable.ecosystem) == ecosystem,
                    col(CCloudFocusPreviewRetentionOutcomeTable.tenant_id) == tenant_id,
                )
            ).all()
            by_kind: dict[PreviewRetentionCleanupKind, PreviewRetentionOutcome] = {}
            for row in rows:
                outcome = _retention_outcome(row)
                if outcome.cleanup_kind in by_kind:
                    raise ValueError("duplicate persisted retention cleanup kind")
                by_kind[outcome.cleanup_kind] = outcome
            return PreviewRetentionOutcomeSet(
                ordinary=by_kind.get(PreviewRetentionCleanupKind.ORDINARY),
                preview_evidence=by_kind.get(PreviewRetentionCleanupKind.PREVIEW_EVIDENCE),
            )
        except PreviewEvidenceSchemaError:
            raise
        except (TypeError, ValueError) as exc:
            raise PreviewEvidenceSchemaError("persisted retention outcome is invalid") from exc


class SQLModelPreviewRepairRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def _get(self, repair_id: str) -> PreviewRepair | None:
        row = self._session.get(CCloudFocusPreviewRepairTable, repair_id)
        if row is None:
            return None
        date_rows = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable)
            .where(col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id)
            .order_by(col(CCloudFocusPreviewRepairDateTable.tracking_date))
        ).all()
        return PreviewRepair(
            repair_id=row.repair_id,
            tenant_name=row.tenant_name,
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            start_date=row.start_date,
            end_date=row.end_date,
            status=PreviewRepairStatus(row.status),
            created_at=_utc(row.created_at),
            started_at=None if row.started_at is None else _utc(row.started_at),
            completed_at=None if row.completed_at is None else _utc(row.completed_at),
            diagnostic=_diagnostic(
                row.diagnostic_code,
                row.diagnostic_message,
                row.diagnostic_retryable,
            ),
            dates=tuple(_repair_date(item) for item in date_rows),
        )

    def create_queued(self, repair: PreviewRepair) -> PreviewRepair:
        if repair.status is not PreviewRepairStatus.QUEUED:
            raise ValueError("repair must be queued")
        self._session.add(
            CCloudFocusPreviewRepairTable(
                repair_id=repair.repair_id,
                tenant_name=repair.tenant_name,
                ecosystem=repair.ecosystem,
                tenant_id=repair.tenant_id,
                start_date=repair.start_date,
                end_date=repair.end_date,
                status=repair.status.value,
                created_at=repair.created_at,
            )
        )
        self._session.add_all(
            [
                CCloudFocusPreviewRepairDateTable(
                    repair_id=item.repair_id,
                    tracking_date=item.tracking_date,
                    status=item.status.value,
                )
                for item in repair.dates
            ]
        )
        result = self._session.exec(
            update(CCloudFocusPreviewRepairHeadTable)
            .where(
                col(CCloudFocusPreviewRepairHeadTable.ecosystem) == repair.ecosystem,
                col(CCloudFocusPreviewRepairHeadTable.tenant_id) == repair.tenant_id,
            )
            .values(repair_id=repair.repair_id)
        )
        if int(getattr(result, "rowcount", 0)) == 0:
            self._session.add(
                CCloudFocusPreviewRepairHeadTable(
                    ecosystem=repair.ecosystem,
                    tenant_id=repair.tenant_id,
                    repair_id=repair.repair_id,
                )
            )
        self._session.flush()
        created = self._get(repair.repair_id)
        if created is None:
            raise RuntimeError("queued repair was not persisted")
        return created

    def get_for_owner(
        self,
        repair_id: str,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRepair | None:
        value = self._get(repair_id)
        if value is None or value.ecosystem != ecosystem or value.tenant_id != tenant_id:
            return None
        return value

    def find_active_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRepair | None:
        row = self._session.exec(
            select(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.ecosystem) == ecosystem,
                col(CCloudFocusPreviewRepairTable.tenant_id) == tenant_id,
                col(CCloudFocusPreviewRepairTable.status).in_(
                    (PreviewRepairStatus.QUEUED.value, PreviewRepairStatus.RUNNING.value)
                ),
            )
            .order_by(col(CCloudFocusPreviewRepairTable.created_at))
            .limit(1)
        ).first()
        return None if row is None else self._get(row.repair_id)

    def get_current_progress_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRepairProgress | PreviewRepairHistoryUnresolved | None:
        head = self._session.get(
            CCloudFocusPreviewRepairHeadTable,
            (ecosystem, tenant_id),
        )
        if head is None:
            has_history = self._session.exec(
                select(
                    exists().where(
                        col(CCloudFocusPreviewRepairTable.ecosystem) == ecosystem,
                        col(CCloudFocusPreviewRepairTable.tenant_id) == tenant_id,
                    )
                )
            ).one()
            if has_history:
                raise PreviewEvidenceSchemaError("repair history is missing its current head")
            return None
        if head.repair_id is None:
            return PreviewRepairHistoryUnresolved()
        parent = self._session.get(CCloudFocusPreviewRepairTable, head.repair_id)
        if parent is None:
            raise PreviewEvidenceSchemaError("repair head names a missing repair")
        if parent.ecosystem != ecosystem or parent.tenant_id != tenant_id:
            raise PreviewEvidenceSchemaError("repair head owner does not match repair owner")
        row = self._session.exec(
            select(
                func.count(col(CCloudFocusPreviewRepairDateTable.tracking_date)),
                func.sum(
                    case(
                        (
                            col(CCloudFocusPreviewRepairDateTable.status).in_(
                                (
                                    PreviewRepairDateStatus.SUCCEEDED.value,
                                    PreviewRepairDateStatus.FAILED.value,
                                )
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ),
            ).where(col(CCloudFocusPreviewRepairDateTable.repair_id) == head.repair_id)
        ).one()
        total_dates, completed_dates = row
        return PreviewRepairProgress(
            status=PreviewRepairStatus(parent.status),
            completed_dates=int(completed_dates or 0),
            total_dates=int(total_dates),
        )

    def mark_running(self, repair_id: str, *, started_at: datetime) -> PreviewRepair | None:
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == PreviewRepairStatus.QUEUED.value,
            )
            .values(status=PreviewRepairStatus.RUNNING.value, started_at=_require_aware(started_at, "started_at"))
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        return self._get(repair_id)

    def mark_date_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        started_at: datetime,
    ) -> PreviewRepairDate | None:
        parent = self._session.get(CCloudFocusPreviewRepairTable, repair_id)
        if parent is None or parent.status != PreviewRepairStatus.RUNNING.value:
            return None
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.QUEUED.value,
            )
            .values(
                status=PreviewRepairDateStatus.RUNNING.value,
                started_at=_require_aware(started_at, "started_at"),
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def _mark_date_result(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        status: PreviewRepairDateStatus,
        completed_at: datetime | None,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        if not calculation_id.strip() or rows_written < 0:
            raise ValueError("invalid repair calculation result")
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.RUNNING.value,
            )
            .values(
                status=status.value,
                completed_at=None if completed_at is None else _require_aware(completed_at, "completed_at"),
                calculation_id=calculation_id,
                calculation_completed_at=_require_aware(
                    calculation_completed_at,
                    "calculation_completed_at",
                ),
                rows_written=rows_written,
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def mark_date_daily_validated(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        return self._mark_date_result(
            repair_id,
            tracking_date,
            status=PreviewRepairDateStatus.DAILY_VALIDATED,
            completed_at=None,
            calculation_id=calculation_id,
            calculation_completed_at=calculation_completed_at,
            rows_written=rows_written,
        )

    def mark_date_succeeded_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        calculation_id: str,
        calculation_completed_at: datetime,
        rows_written: int,
    ) -> PreviewRepairDate | None:
        return self._mark_date_result(
            repair_id,
            tracking_date,
            status=PreviewRepairDateStatus.SUCCEEDED,
            completed_at=completed_at,
            calculation_id=calculation_id,
            calculation_completed_at=calculation_completed_at,
            rows_written=rows_written,
        )

    def mark_date_failed_from_running(
        self,
        repair_id: str,
        tracking_date: date,
        *,
        completed_at: datetime,
        stage: PreviewRepairFailureStage,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepairDate | None:
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date) == tracking_date,
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.RUNNING.value,
            )
            .values(
                status=PreviewRepairDateStatus.FAILED.value,
                completed_at=_require_aware(completed_at, "completed_at"),
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=stage.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        row = self._session.get(CCloudFocusPreviewRepairDateTable, (repair_id, tracking_date))
        return None if row is None else _repair_date(row)

    def finalize_month_dates(
        self,
        repair_id: str,
        tracking_dates: tuple[date, ...],
        *,
        terminal_status: Literal[
            PreviewRepairDateStatus.SUCCEEDED,
            PreviewRepairDateStatus.FAILED,
        ],
        completed_at: datetime,
        stage: PreviewRepairFailureStage | None,
        diagnostic: PreviewDiagnostic | None,
    ) -> tuple[PreviewRepairDate, ...] | None:
        if (
            not tracking_dates
            or tuple(sorted(set(tracking_dates))) != tracking_dates
            or terminal_status not in {PreviewRepairDateStatus.SUCCEEDED, PreviewRepairDateStatus.FAILED}
            or (terminal_status is PreviewRepairDateStatus.SUCCEEDED and (stage is not None or diagnostic is not None))
            or (terminal_status is PreviewRepairDateStatus.FAILED and (stage is None or diagnostic is None))
        ):
            raise ValueError("invalid monthly repair finalization")
        rows = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable).where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
            )
        ).all()
        if len(rows) != len(tracking_dates) or any(
            row.status != PreviewRepairDateStatus.DAILY_VALIDATED.value for row in rows
        ):
            return None
        values: dict[str, object] = {
            "status": terminal_status.value,
            "completed_at": _require_aware(completed_at, "completed_at"),
        }
        if terminal_status is PreviewRepairDateStatus.FAILED:
            assert stage is not None and diagnostic is not None
            values.update(
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=stage.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
                col(CCloudFocusPreviewRepairDateTable.status) == PreviewRepairDateStatus.DAILY_VALIDATED.value,
            )
            .values(**values)
        )
        if int(getattr(result, "rowcount", 0)) != len(tracking_dates):
            self._session.rollback()
            return None
        self._session.flush()
        updated = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.tracking_date).in_(tracking_dates),
            )
            .order_by(col(CCloudFocusPreviewRepairDateTable.tracking_date))
        ).all()
        return tuple(_repair_date(item) for item in updated)

    def _fail(
        self,
        repair_id: str,
        *,
        expected_status: PreviewRepairStatus,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        completed = _require_aware(completed_at, "completed_at")
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == expected_status.value,
            )
            .values(
                status=PreviewRepairStatus.FAILED.value,
                completed_at=completed,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
            )
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        nonterminal = (
            (PreviewRepairDateStatus.QUEUED.value,)
            if expected_status is PreviewRepairStatus.QUEUED
            else (
                PreviewRepairDateStatus.QUEUED.value,
                PreviewRepairDateStatus.RUNNING.value,
                PreviewRepairDateStatus.DAILY_VALIDATED.value,
            )
        )
        expected_children = len(
            self._session.exec(
                select(CCloudFocusPreviewRepairDateTable).where(
                    col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                    col(CCloudFocusPreviewRepairDateTable.status).in_(nonterminal),
                )
            ).all()
        )
        child_result = self._session.exec(
            update(CCloudFocusPreviewRepairDateTable)
            .where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairDateTable.status).in_(nonterminal),
            )
            .values(
                status=PreviewRepairDateStatus.FAILED.value,
                completed_at=completed,
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=PreviewRepairFailureStage.WORKER.value,
                diagnostic_code=diagnostic.code,
                diagnostic_message=diagnostic.message,
                diagnostic_retryable=diagnostic.retryable,
                source_correlation_ids_json=json.dumps(list(diagnostic.source_correlation_ids)),
            )
        )
        if int(getattr(child_result, "rowcount", 0)) != expected_children:
            self._session.rollback()
            return None
        self._session.flush()
        return self._get(repair_id)

    def fail_queued_before_execution(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        return self._fail(
            repair_id,
            expected_status=PreviewRepairStatus.QUEUED,
            completed_at=completed_at,
            diagnostic=diagnostic,
        )

    def fail_running_worker(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewRepair | None:
        return self._fail(
            repair_id,
            expected_status=PreviewRepairStatus.RUNNING,
            completed_at=completed_at,
            diagnostic=diagnostic,
        )

    def _finalize(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
        with_failures: bool,
    ) -> PreviewRepair | None:
        children = self._session.exec(
            select(CCloudFocusPreviewRepairDateTable).where(
                col(CCloudFocusPreviewRepairDateTable.repair_id) == repair_id
            )
        ).all()
        statuses = {item.status for item in children}
        if not children or not statuses <= {
            PreviewRepairDateStatus.SUCCEEDED.value,
            PreviewRepairDateStatus.FAILED.value,
        }:
            return None
        if with_failures != (PreviewRepairDateStatus.FAILED.value in statuses):
            return None
        final_status = PreviewRepairStatus.COMPLETED_WITH_FAILURES if with_failures else PreviewRepairStatus.COMPLETED
        result = self._session.exec(
            update(CCloudFocusPreviewRepairTable)
            .where(
                col(CCloudFocusPreviewRepairTable.repair_id) == repair_id,
                col(CCloudFocusPreviewRepairTable.status) == PreviewRepairStatus.RUNNING.value,
            )
            .values(status=final_status.value, completed_at=_require_aware(completed_at, "completed_at"))
        )
        if int(getattr(result, "rowcount", 0)) != 1:
            return None
        self._session.flush()
        return self._get(repair_id)

    def finalize_completed(self, repair_id: str, *, completed_at: datetime) -> PreviewRepair | None:
        return self._finalize(repair_id, completed_at=completed_at, with_failures=False)

    def finalize_completed_with_failures(
        self,
        repair_id: str,
        *,
        completed_at: datetime,
    ) -> PreviewRepair | None:
        return self._finalize(repair_id, completed_at=completed_at, with_failures=True)

    def fail_interrupted_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
        *,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> int:
        rows = self._session.exec(
            select(CCloudFocusPreviewRepairTable).where(
                col(CCloudFocusPreviewRepairTable.ecosystem) == ecosystem,
                col(CCloudFocusPreviewRepairTable.tenant_id) == tenant_id,
                col(CCloudFocusPreviewRepairTable.status).in_(
                    (PreviewRepairStatus.QUEUED.value, PreviewRepairStatus.RUNNING.value)
                ),
            )
        ).all()
        changed = 0
        for row in rows:
            result = self._fail(
                row.repair_id,
                expected_status=PreviewRepairStatus(row.status),
                completed_at=completed_at,
                diagnostic=diagnostic,
            )
            changed += result is not None
        return changed


class SQLModelPreviewSourceWindowRepository:
    """Evidence-only source writer and legacy bootstrap repository."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._sources = CCloudBillingRepository(session)

    def replace_capture(
        self,
        capture: NativeSourceEvidenceCapture,
        *,
        attempt_sequence: int,
        captured_at: datetime,
    ) -> SourceWindowWriteResult:
        from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture

        if not isinstance(capture, CCloudNativeSourceEvidenceCapture):
            raise TypeError("Confluent source writer requires a Confluent native capture")
        if attempt_sequence <= 0:
            raise ValueError("attempt_sequence must be positive")
        _require_aware(captured_at, "captured_at")
        result = self._sources.replace_source_window(
            capture.ecosystem,
            capture.tenant_id,
            capture.refresh_start,
            capture.refresh_end,
            capture.records,
        )
        for window_count in result.window_counts:
            window = window_count.window
            capture_id = capture.capture_id(window)
            self._session.execute(
                update(CCloudCostSourceRecordTable)
                .where(
                    col(CCloudCostSourceRecordTable.ecosystem) == capture.ecosystem,
                    col(CCloudCostSourceRecordTable.tenant_id) == capture.tenant_id,
                    col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                    col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                )
                .values(capture_id=capture_id)
            )
        self._session.flush()
        return result

    def list_unassociated_windows(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> tuple[NativeSourceWindow, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        rows = self._session.exec(
            select(
                CCloudCostSourceRecordTable.collection_window_start,
                CCloudCostSourceRecordTable.collection_window_end,
            )
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
                col(CCloudCostSourceRecordTable.collection_window_start) < end,
                col(CCloudCostSourceRecordTable.collection_window_end) > start,
            )
            .distinct()
            .order_by(
                col(CCloudCostSourceRecordTable.collection_window_start),
                col(CCloudCostSourceRecordTable.collection_window_end),
            )
        ).yield_per(256)
        return tuple(NativeSourceWindow(_utc(row[0]), _utc(row[1])) for row in rows)

    def iter_unassociated_window(
        self, ecosystem: str, tenant_id: str, window: NativeSourceWindow
    ) -> Iterator[PreviewSourceEvidence]:
        rows = self._session.exec(
            select(CCloudCostSourceRecordTable)
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
            )
            .order_by(
                col(CCloudCostSourceRecordTable.evidence_scope_start),
                col(CCloudCostSourceRecordTable.evidence_scope_end),
                col(CCloudCostSourceRecordTable.source_record_id),
                col(CCloudCostSourceRecordTable.identity_scheme),
            )
        )
        for row in rows:
            yield _source_table_to_preview(row)

    def associate_legacy_window(
        self,
        ecosystem: str,
        tenant_id: str,
        window: NativeSourceWindow,
        *,
        capture_id: str,
        expected_source_count: int,
    ) -> int:
        result = self._session.execute(
            update(CCloudCostSourceRecordTable)
            .where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.collection_window_start) == window.start,
                col(CCloudCostSourceRecordTable.collection_window_end) == window.end,
                col(CCloudCostSourceRecordTable.capture_id).is_(None),
            )
            .values(capture_id=capture_id)
        )
        changed = int(getattr(result, "rowcount", 0))
        if changed != expected_source_count:
            raise PreviewEvidenceBootstrapConflictError("legacy source association changed concurrently")
        return changed

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        cutoff = exclusive_utc_second_upper_bound(before, field="before")
        result = self._session.execute(
            delete(CCloudCostSourceRecordTable).where(
                col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
                col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
                col(CCloudCostSourceRecordTable.retention_timestamp) < cutoff,
            )
        )
        return int(getattr(result, "rowcount", 0))


class SQLModelPreviewAllocationLineageRepository:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._lineage = CCloudChargebackRepository(session)

    def replace_calculation_lineage(
        self,
        capture: AllocationLineageRunCapture,
        *,
        calculation_completed_at: datetime,
    ) -> AllocationLineageRun:
        complete_origins = [
            (
                _require_aware(origin.origin_timestamp, "origin_timestamp"),
                origin.origin_env_id,
                origin.origin_resource_id,
                origin.origin_product_type,
                origin.origin_product_category,
            )
            for origin in capture.captures
            if origin.status.value == "complete"
        ]
        if len(complete_origins) != len(set(complete_origins)):
            raise ValueError("exact Preview source association is ambiguous")
        self._lineage.replace_calculation_lineage(capture, calculation_completed_at=calculation_completed_at)
        self._associate_ordinary_bootstrapped_sources(capture)
        return self._replace_exact_calculation_lineage(
            capture,
            calculation_completed_at=calculation_completed_at,
            retained_sources=None,
        )

    def _associate_ordinary_bootstrapped_sources(
        self,
        capture: AllocationLineageRunCapture,
    ) -> None:
        day_start = datetime.combine(capture.tracking_date, time.min, tzinfo=UTC)
        day_end = day_start + timedelta(days=1)
        sources = tuple(
            self._session.exec(
                select(CCloudCostSourceRecordTable)
                .where(
                    col(CCloudCostSourceRecordTable.ecosystem) == capture.ecosystem,
                    col(CCloudCostSourceRecordTable.tenant_id) == capture.tenant_id,
                    col(CCloudCostSourceRecordTable.allocation_timestamp) >= day_start,
                    col(CCloudCostSourceRecordTable.allocation_timestamp) < day_end,
                    or_(
                        col(CCloudCostSourceRecordTable.capture_id).like("legacy:v1:%"),
                        col(CCloudCostSourceRecordTable.billing_timestamp).is_(None),
                        col(CCloudCostSourceRecordTable.billing_env_id).is_(None),
                        col(CCloudCostSourceRecordTable.billing_resource_id).is_(None),
                        col(CCloudCostSourceRecordTable.billing_product_type).is_(None),
                        col(CCloudCostSourceRecordTable.billing_product_category).is_(None),
                    ),
                )
                .order_by(
                    col(CCloudCostSourceRecordTable.capture_id),
                    col(CCloudCostSourceRecordTable.source_record_id),
                    col(CCloudCostSourceRecordTable.evidence_scope_start),
                    col(CCloudCostSourceRecordTable.evidence_scope_end),
                )
            ).all()
        )
        if sources:
            self._associate_bootstrapped_sources(sources, capture)

    def refresh_bootstrapped_lineage(
        self,
        capture_ids: tuple[str, ...],
    ) -> None:
        if not capture_ids or any(not capture_id.strip() for capture_id in capture_ids):
            raise ValueError("bootstrap capture IDs must be nonblank")
        if len(capture_ids) != len(set(capture_ids)):
            raise ValueError("bootstrap capture IDs must be unique")
        retained_sources = tuple(
            self._session.exec(
                select(CCloudCostSourceRecordTable)
                .where(col(CCloudCostSourceRecordTable.capture_id).in_(capture_ids))
                .order_by(
                    col(CCloudCostSourceRecordTable.ecosystem),
                    col(CCloudCostSourceRecordTable.tenant_id),
                    col(CCloudCostSourceRecordTable.capture_id),
                    col(CCloudCostSourceRecordTable.allocation_timestamp),
                    col(CCloudCostSourceRecordTable.source_record_id),
                    col(CCloudCostSourceRecordTable.evidence_scope_start),
                    col(CCloudCostSourceRecordTable.evidence_scope_end),
                )
            ).all()
        )
        found_capture_ids = {source.capture_id for source in retained_sources}
        if found_capture_ids != set(capture_ids):
            raise ValueError("bootstrap capture selection is incomplete")
        owners = {(source.ecosystem, source.tenant_id) for source in retained_sources}
        if len(owners) != 1:
            raise ValueError("bootstrap capture selection spans multiple owners")
        ecosystem, tenant_id = next(iter(owners))
        tracking_dates = tuple(sorted({_utc(source.allocation_timestamp).date() for source in retained_sources}))
        for tracking_date in tracking_dates:
            state = self._session.get(
                PipelineStateTable,
                (ecosystem, tenant_id, tracking_date),
            )
            if state is None or not state.chargeback_calculated:
                continue
            if not state.calculation_id or state.calculation_completed_at is None:
                raise ValueError("current calculated pipeline state is incomplete")
            run = self._load_generic_lineage_capture(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                tracking_date=tracking_date,
                calculation_id=state.calculation_id,
                calculation_completed_at=state.calculation_completed_at,
            )
            date_sources = tuple(
                source for source in retained_sources if _utc(source.allocation_timestamp).date() == tracking_date
            )
            selected_origins = self._associate_bootstrapped_sources(
                date_sources,
                run,
            )
            selected_capture = replace(
                run,
                captures=tuple(
                    origin
                    for origin in run.captures
                    if (
                        _require_aware(origin.origin_timestamp, "origin_timestamp"),
                        origin.origin_env_id,
                        origin.origin_resource_id,
                        origin.origin_product_type,
                        origin.origin_product_category,
                    )
                    in selected_origins
                ),
            )
            self._replace_exact_calculation_lineage(
                selected_capture,
                calculation_completed_at=state.calculation_completed_at,
                retained_sources=date_sources,
            )

    def _load_generic_lineage_capture(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        tracking_date: date,
        calculation_id: str,
        calculation_completed_at: datetime,
    ) -> AllocationLineageRunCapture:
        from core.storage.interface import (
            AllocationLineageCapture,
            AllocationLineageFact,
            AllocationLineageRunCapture,
            AllocationTargetKind,
            LineageCaptureStatus,
        )

        run = self._session.get(
            CCloudAllocationLineageRunTable,
            (ecosystem, tenant_id, tracking_date),
        )
        if (
            run is None
            or run.calculation_id != calculation_id
            or run.capture_status != LineageCaptureStatus.COMPLETE.value
            or _utc(run.calculation_completed_at) != _utc(calculation_completed_at)
        ):
            raise ValueError("matching complete generic allocation lineage is unavailable")
        portions = tuple(
            self._session.exec(
                select(CCloudAllocationLineagePortionTable)
                .where(
                    col(CCloudAllocationLineagePortionTable.ecosystem) == ecosystem,
                    col(CCloudAllocationLineagePortionTable.tenant_id) == tenant_id,
                    col(CCloudAllocationLineagePortionTable.tracking_date) == tracking_date,
                    col(CCloudAllocationLineagePortionTable.calculation_id) == calculation_id,
                )
                .order_by(
                    col(CCloudAllocationLineagePortionTable.origin_timestamp),
                    col(CCloudAllocationLineagePortionTable.origin_env_id),
                    col(CCloudAllocationLineagePortionTable.origin_resource_id),
                    col(CCloudAllocationLineagePortionTable.origin_product_type),
                    col(CCloudAllocationLineagePortionTable.origin_product_category),
                    col(CCloudAllocationLineagePortionTable.portion_ordinal),
                )
            ).all()
        )
        if len(portions) != run.portion_count:
            raise ValueError("generic allocation lineage portion count is incomplete")
        grouped: dict[
            tuple[datetime, str, str, str, str],
            list[CCloudAllocationLineagePortionTable],
        ] = {}
        for portion in portions:
            origin_key = (
                _utc(portion.origin_timestamp),
                portion.origin_env_id,
                portion.origin_resource_id,
                portion.origin_product_type,
                portion.origin_product_category,
            )
            grouped.setdefault(origin_key, []).append(portion)
        captures: list[AllocationLineageCapture] = []
        for origin_key, origin_portions in grouped.items():
            ordinals = tuple(portion.portion_ordinal for portion in origin_portions)
            if ordinals != tuple(range(len(origin_portions))):
                raise ValueError("generic allocation lineage ordinals are incomplete")
            try:
                facts = tuple(
                    AllocationLineageFact(
                        portion_ordinal=portion.portion_ordinal,
                        target_kind=AllocationTargetKind(portion.target_kind),
                        target_id=portion.target_id,
                        allocated_cost=Decimal(portion.allocated_cost),
                        allocated_quantity=Decimal(portion.allocated_quantity),
                        allocation_ratio=Decimal(portion.allocation_ratio),
                        method_id=portion.method_id,
                        method_version=portion.method_version,
                        method_details_json=portion.method_details_json,
                    )
                    for portion in origin_portions
                )
            except (ArithmeticError, TypeError, ValueError) as exc:
                raise ValueError("generic allocation lineage is invalid") from exc
            captures.append(
                AllocationLineageCapture(
                    origin_timestamp=origin_key[0],
                    origin_env_id=origin_key[1],
                    origin_resource_id=origin_key[2],
                    origin_product_type=origin_key[3],
                    origin_product_category=origin_key[4],
                    status=LineageCaptureStatus.COMPLETE,
                    reason=None,
                    facts=facts,
                )
            )
        return AllocationLineageRunCapture(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            tracking_date=tracking_date,
            calculation_id=calculation_id,
            captures=tuple(captures),
        )

    def _associate_bootstrapped_sources(
        self,
        retained_sources: tuple[CCloudCostSourceRecordTable, ...],
        capture: AllocationLineageRunCapture,
    ) -> set[tuple[datetime, str, str, str, str]]:
        complete_origins = {
            (
                _require_aware(origin.origin_timestamp, "origin_timestamp"),
                origin.origin_env_id,
                origin.origin_resource_id,
                origin.origin_product_type,
                origin.origin_product_category,
            )
            for origin in capture.captures
            if origin.status.value == "complete"
        }
        selected: set[tuple[datetime, str, str, str, str]] = set()
        for source in retained_sources:
            if source.capture_id is None or not source.capture_id.startswith("legacy:v1:"):
                raise ValueError("bootstrap source capture is not legacy evidence")
            association = (
                source.billing_timestamp,
                source.billing_env_id,
                source.billing_resource_id,
                source.billing_product_type,
                source.billing_product_category,
            )
            if any(value is None for value in association) and not all(value is None for value in association):
                raise ValueError("bootstrap source billing association is partial")
            if source.resource_id is None or source.line_type is None:
                raise ValueError("bootstrap source mapped identity is incomplete")
            derived = (
                _utc(source.allocation_timestamp),
                source.environment_id or "",
                source.resource_id,
                source.line_type,
                source.product or "",
            )
            if all(value is None for value in association):
                if derived not in complete_origins:
                    raise ValueError("bootstrap source has no matching generic origin")
                source.billing_timestamp = derived[0]
                source.billing_env_id = derived[1]
                source.billing_resource_id = derived[2]
                source.billing_product_type = derived[3]
                source.billing_product_category = derived[4]
                self._session.add(source)
            else:
                assert source.billing_timestamp is not None
                assert source.billing_env_id is not None
                assert source.billing_resource_id is not None
                assert source.billing_product_type is not None
                assert source.billing_product_category is not None
                persisted = (
                    _utc(source.billing_timestamp),
                    source.billing_env_id,
                    source.billing_resource_id,
                    source.billing_product_type,
                    source.billing_product_category,
                )
                if persisted != derived or persisted not in complete_origins:
                    raise ValueError("bootstrap source billing association conflicts")
            selected.add(derived)
        self._session.flush()
        return selected

    def _replace_exact_calculation_lineage(
        self,
        capture: AllocationLineageRunCapture,
        *,
        calculation_completed_at: datetime,
        retained_sources: tuple[CCloudCostSourceRecordTable, ...] | None,
    ) -> AllocationLineageRun:
        selected_refresh_sources = retained_sources is not None
        if retained_sources is None:
            self._session.execute(
                delete(CCloudPreviewSourceAllocationLineagePortionTable).where(
                    col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem) == capture.ecosystem,
                    col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id) == capture.tenant_id,
                    col(CCloudPreviewSourceAllocationLineagePortionTable.tracking_date) == capture.tracking_date,
                )
            )
        else:
            for source in retained_sources:
                self._session.execute(
                    delete(CCloudPreviewSourceAllocationLineagePortionTable).where(
                        col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem) == source.ecosystem,
                        col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id) == source.tenant_id,
                        col(CCloudPreviewSourceAllocationLineagePortionTable.source_record_id)
                        == source.source_record_id,
                        col(CCloudPreviewSourceAllocationLineagePortionTable.evidence_scope_start)
                        == source.evidence_scope_start,
                        col(CCloudPreviewSourceAllocationLineagePortionTable.evidence_scope_end)
                        == source.evidence_scope_end,
                    )
                )
        preview_rows: list[CCloudPreviewSourceAllocationLineagePortionTable] = []
        used_source_keys: set[tuple[str, datetime, datetime]] = set()
        day_start = datetime.combine(capture.tracking_date, time.min, tzinfo=UTC)
        day_end = day_start + timedelta(days=1)
        if retained_sources is None:
            retained_sources = tuple(
                self._session.exec(
                    select(CCloudCostSourceRecordTable)
                    .where(
                        col(CCloudCostSourceRecordTable.ecosystem) == capture.ecosystem,
                        col(CCloudCostSourceRecordTable.tenant_id) == capture.tenant_id,
                        col(CCloudCostSourceRecordTable.billing_timestamp) >= day_start,
                        col(CCloudCostSourceRecordTable.billing_timestamp) < day_end,
                    )
                    .order_by(
                        col(CCloudCostSourceRecordTable.billing_timestamp),
                        col(CCloudCostSourceRecordTable.billing_env_id),
                        col(CCloudCostSourceRecordTable.billing_resource_id),
                        col(CCloudCostSourceRecordTable.billing_product_type),
                        col(CCloudCostSourceRecordTable.billing_product_category),
                        col(CCloudCostSourceRecordTable.source_record_id),
                        col(CCloudCostSourceRecordTable.evidence_scope_start),
                        col(CCloudCostSourceRecordTable.evidence_scope_end),
                    )
                ).all()
            )
        sources_by_origin: dict[
            tuple[datetime, str, str, str, str],
            list[CCloudCostSourceRecordTable],
        ] = {}
        for source in retained_sources:
            if any(
                value is None
                for value in (
                    source.billing_timestamp,
                    source.billing_env_id,
                    source.billing_resource_id,
                    source.billing_product_type,
                    source.billing_product_category,
                )
            ):
                continue
            assert source.billing_timestamp is not None
            assert source.billing_env_id is not None
            assert source.billing_resource_id is not None
            assert source.billing_product_type is not None
            assert source.billing_product_category is not None
            origin_key = (
                _utc(source.billing_timestamp),
                source.billing_env_id,
                source.billing_resource_id,
                source.billing_product_type,
                source.billing_product_category,
            )
            sources_by_origin.setdefault(origin_key, []).append(source)
        for origin in capture.captures:
            if origin.status.value != "complete":
                continue
            facts = tuple(sorted(origin.facts, key=lambda fact: fact.portion_ordinal))
            if not facts or tuple(fact.portion_ordinal for fact in facts) != tuple(range(len(facts))):
                raise ValueError("generic allocation lineage is incomplete")
            sources = tuple(
                sources_by_origin.get(
                    (
                        _require_aware(origin.origin_timestamp, "origin_timestamp"),
                        origin.origin_env_id,
                        origin.origin_resource_id,
                        origin.origin_product_type,
                        origin.origin_product_category,
                    ),
                    (),
                )
            )
            if not sources:
                raise ValueError("exact Preview source association is missing")
            source_amounts: list[Decimal] = []
            source_quantities: list[Decimal] = []
            source_originals: list[Decimal] = []
            for source in sources:
                key = (
                    source.source_record_id,
                    _utc(source.evidence_scope_start),
                    _utc(source.evidence_scope_end),
                )
                if key in used_source_keys or source.malformed:
                    raise ValueError("exact Preview source association is ambiguous or malformed")
                used_source_keys.add(key)
                try:
                    amount, original_amount, _discount, _price, quantity = normalize_preview_source_economics(
                        line_type=source.line_type,
                        amount=None if source.amount is None else Decimal(source.amount),
                        original_amount=None if source.original_amount is None else Decimal(source.original_amount),
                        discount_amount=None if source.discount_amount is None else Decimal(source.discount_amount),
                        price=None if source.price is None else Decimal(source.price),
                        quantity=None if source.quantity is None else Decimal(source.quantity),
                    )
                    decoded_tiers = json.loads(source.tier_dimensions_json)
                except (ArithmeticError, TypeError, ValueError) as exc:
                    raise ValueError("exact Preview source economics are invalid") from exc
                if not isinstance(decoded_tiers, dict):
                    raise ValueError("exact Preview tier evidence is malformed")
                source_amounts.append(amount)
                source_quantities.append(quantity)
                source_originals.append(original_amount)

            generic_costs = tuple(fact.allocated_cost for fact in facts)
            generic_quantities = tuple(fact.allocated_quantity for fact in facts)
            ordinals = tuple(fact.portion_ordinal for fact in facts)
            with localcontext(_APPORTION_CONTEXT):
                if sum(source_amounts, Decimal(0)) != sum(generic_costs, Decimal(0)) or sum(
                    source_quantities, Decimal(0)
                ) != sum(generic_quantities, Decimal(0)):
                    raise ValueError("exact Preview source and compatibility margins differ")
            cost_matrix = _apportion_signed(
                source_amounts,
                generic_costs,
                ordinals,
                minimum_places=0 if sum(source_quantities, Decimal(0)) == 0 and any(source_quantities) else 2,
            )
            if sum(source_quantities, Decimal(0)) == 0 and any(source_quantities):
                if any(generic_quantities):
                    raise ValueError("zero-net source quantity requires zero generic columns")
                quantity_matrix = _apportion_zero_net_quantity(
                    source_quantities=source_quantities,
                    generic_allocated_costs=generic_costs,
                    portion_ordinals=ordinals,
                )
            else:
                quantity_matrix = _apportion_signed(
                    source_quantities,
                    generic_quantities,
                    ordinals,
                    minimum_places=2,
                )

            for source_index, source in enumerate(sources):
                original_cells = _apportion_original_cost(
                    source_originals[source_index],
                    cost_matrix[source_index],
                    ordinals,
                )
                for fact_index, fact in enumerate(facts):
                    allocated_cost = cost_matrix[source_index][fact_index]
                    source_amount = source_amounts[source_index]
                    with localcontext(_PREVIEW_RATIO_CONTEXT):
                        ratio = Decimal(0) if source_amount == 0 else allocated_cost / source_amount
                    preview_rows.append(
                        CCloudPreviewSourceAllocationLineagePortionTable(
                            ecosystem=capture.ecosystem,
                            tenant_id=capture.tenant_id,
                            tracking_date=capture.tracking_date,
                            calculation_id=capture.calculation_id,
                            source_record_id=source.source_record_id,
                            evidence_scope_start=_utc(source.evidence_scope_start),
                            evidence_scope_end=_utc(source.evidence_scope_end),
                            origin_timestamp=_require_aware(origin.origin_timestamp, "origin_timestamp"),
                            origin_env_id=origin.origin_env_id,
                            origin_resource_id=origin.origin_resource_id,
                            origin_product_type=origin.origin_product_type,
                            origin_product_category=origin.origin_product_category,
                            portion_ordinal=fact.portion_ordinal,
                            target_kind=fact.target_kind.value,
                            target_id=fact.target_id,
                            allocated_cost=_decimal_text(allocated_cost),
                            allocated_quantity=_decimal_text(quantity_matrix[source_index][fact_index]),
                            allocated_original_cost=_decimal_text(original_cells[fact_index]),
                            allocation_ratio=_decimal_text(ratio),
                            method_id=fact.method_id,
                            method_version=fact.method_version,
                            method_details_json=fact.method_details_json,
                        )
                    )
        self._session.add_all(preview_rows)
        self._session.flush()
        if selected_refresh_sources and len(used_source_keys) != len(retained_sources):
            raise ValueError("exact Preview source association is missing")
        portion_count = sum(len(item.facts) for item in capture.captures)
        return AllocationLineageRun(
            ecosystem=capture.ecosystem,
            tenant_id=capture.tenant_id,
            tracking_date=capture.tracking_date,
            calculation_id=capture.calculation_id,
            calculation_completed_at=_require_aware(calculation_completed_at, "calculation_completed_at"),
            status=AllocationLineageRunStatus.COMPLETE,
            portion_count=portion_count,
            preview_portion_count=len(preview_rows),
        )

    def mark_calculation_lineage_unavailable(
        self, value: AllocationLineageUnavailableRun
    ) -> AllocationLineageUnavailableRun:
        self._session.execute(
            delete(CCloudPreviewSourceAllocationLineagePortionTable).where(
                col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem) == value.ecosystem,
                col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id) == value.tenant_id,
                col(CCloudPreviewSourceAllocationLineagePortionTable.tracking_date) == value.tracking_date,
            )
        )
        self._session.execute(
            delete(CCloudAllocationLineagePortionTable).where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == value.ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == value.tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date) == value.tracking_date,
            )
        )
        self._session.execute(
            delete(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == value.ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == value.tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date) == value.tracking_date,
            )
        )
        self._session.add(
            CCloudAllocationLineageRunTable(
                ecosystem=value.ecosystem,
                tenant_id=value.tenant_id,
                tracking_date=value.tracking_date,
                calculation_id=value.calculation_id,
                calculation_completed_at=value.calculation_completed_at,
                capture_status=value.status.value,
                capture_reason=value.reason.value,
                portion_count=0,
            )
        )
        self._session.flush()
        return value

    def delete_unretained(
        self,
        ecosystem: str,
        tenant_id: str,
        before: date,
    ) -> LineageDeletionCount:
        from core.preview.persistence import LineageDeletionCount

        matching_calculation = exists().where(
            col(PipelineStateTable.ecosystem) == col(CCloudAllocationLineageRunTable.ecosystem),
            col(PipelineStateTable.tenant_id) == col(CCloudAllocationLineageRunTable.tenant_id),
            col(PipelineStateTable.tracking_date) == col(CCloudAllocationLineageRunTable.tracking_date),
            col(PipelineStateTable.chargeback_calculated).is_(True),
            col(PipelineStateTable.calculation_id) == col(CCloudAllocationLineageRunTable.calculation_id),
        )
        matching_billing = exists().where(
            col(CCloudBillingTable.ecosystem) == col(CCloudAllocationLineagePortionTable.ecosystem),
            col(CCloudBillingTable.tenant_id) == col(CCloudAllocationLineagePortionTable.tenant_id),
            col(CCloudBillingTable.timestamp) == col(CCloudAllocationLineagePortionTable.origin_timestamp),
            col(CCloudBillingTable.env_id) == col(CCloudAllocationLineagePortionTable.origin_env_id),
            col(CCloudBillingTable.resource_id) == col(CCloudAllocationLineagePortionTable.origin_resource_id),
            col(CCloudBillingTable.product_type) == col(CCloudAllocationLineagePortionTable.origin_product_type),
            col(CCloudBillingTable.product_category)
            == col(CCloudAllocationLineagePortionTable.origin_product_category),
        )
        missing_origin = exists().where(
            col(CCloudAllocationLineagePortionTable.ecosystem) == col(CCloudAllocationLineageRunTable.ecosystem),
            col(CCloudAllocationLineagePortionTable.tenant_id) == col(CCloudAllocationLineageRunTable.tenant_id),
            col(CCloudAllocationLineagePortionTable.tracking_date)
            == col(CCloudAllocationLineageRunTable.tracking_date),
            ~matching_billing,
        )
        matching_exact_source = exists().where(
            col(CCloudCostSourceRecordTable.ecosystem)
            == col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem),
            col(CCloudCostSourceRecordTable.tenant_id)
            == col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id),
            col(CCloudCostSourceRecordTable.source_record_id)
            == col(CCloudPreviewSourceAllocationLineagePortionTable.source_record_id),
            col(CCloudCostSourceRecordTable.evidence_scope_start)
            == col(CCloudPreviewSourceAllocationLineagePortionTable.evidence_scope_start),
            col(CCloudCostSourceRecordTable.evidence_scope_end)
            == col(CCloudPreviewSourceAllocationLineagePortionTable.evidence_scope_end),
        )
        missing_exact_source = exists().where(
            col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem)
            == col(CCloudAllocationLineageRunTable.ecosystem),
            col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id)
            == col(CCloudAllocationLineageRunTable.tenant_id),
            col(CCloudPreviewSourceAllocationLineagePortionTable.tracking_date)
            == col(CCloudAllocationLineageRunTable.tracking_date),
            ~matching_exact_source,
        )
        selected_runs = self._session.exec(
            select(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == tenant_id,
                or_(
                    col(CCloudAllocationLineageRunTable.tracking_date) < before,
                    ~matching_calculation,
                    missing_origin,
                    missing_exact_source,
                ),
            )
        )
        selected_dates = tuple(run.tracking_date for run in selected_runs)
        if not selected_dates:
            return LineageDeletionCount(portions=0, runs=0)
        portions = self._session.execute(
            delete(CCloudAllocationLineagePortionTable).where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date).in_(selected_dates),
            )
        )
        self._session.execute(
            delete(CCloudPreviewSourceAllocationLineagePortionTable).where(
                col(CCloudPreviewSourceAllocationLineagePortionTable.ecosystem) == ecosystem,
                col(CCloudPreviewSourceAllocationLineagePortionTable.tenant_id) == tenant_id,
                col(CCloudPreviewSourceAllocationLineagePortionTable.tracking_date).in_(selected_dates),
            )
        )
        runs = self._session.execute(
            delete(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date).in_(selected_dates),
            )
        )
        return LineageDeletionCount(
            portions=int(getattr(portions, "rowcount", 0)),
            runs=int(getattr(runs, "rowcount", 0)),
        )


class SQLModelPreviewSourceReadinessRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def begin_attempt(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_token: str,
        refresh_start: datetime,
        refresh_end: datetime,
        started_at: datetime,
    ) -> PreviewSourceAttempt:
        start = _require_aware(refresh_start, "refresh_start")
        end = _require_aware(refresh_end, "refresh_end")
        started = _require_aware(started_at, "started_at")
        if start >= end or not all(value.strip() for value in (ecosystem, tenant_id, refresh_token)):
            raise ValueError("invalid source attempt")
        existing = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == refresh_token,
            )
        ).first()
        if existing is not None:
            value = _source_attempt(existing)
            if (value.refresh_start, value.refresh_end, value.started_at) != (start, end, started):
                raise ValueError("source attempt token conflicts with persisted bounds")
            return value
        row = CCloudSourceEvidenceAttemptTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            refresh_token=refresh_token,
            refresh_start=start,
            refresh_end=end,
            status=SourceAttemptStatus.PENDING.value,
            started_at=started,
        )
        self._session.add(row)
        self._session.flush([row])
        return _source_attempt(row)

    def get_by_token(self, ecosystem: str, tenant_id: str, refresh_token: str) -> PreviewSourceAttempt | None:
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == refresh_token,
            )
        ).first()
        return None if row is None else _source_attempt(row)

    def ensure_begin_failed(
        self,
        value: SourceAttemptBeginFailure,
        *,
        completed_at: datetime,
    ) -> PreviewSourceAttempt:
        from core.preview.persistence import PreviewSourceAttemptConflictError

        completed = _require_aware(completed_at, "completed_at")
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable).where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == value.ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == value.tenant_id,
                col(CCloudSourceEvidenceAttemptTable.refresh_token) == value.refresh_token,
            )
        ).first()
        if row is None:
            row = CCloudSourceEvidenceAttemptTable(
                ecosystem=value.ecosystem,
                tenant_id=value.tenant_id,
                refresh_token=value.refresh_token,
                refresh_start=value.refresh_start,
                refresh_end=value.refresh_end,
                status=SourceAttemptFinalStatus.FAILED.value,
                started_at=value.started_at,
                completed_at=completed,
                failure_reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED.value,
            )
            self._session.add(row)
            self._session.flush([row])
            return _source_attempt(row)
        persisted = _source_attempt(row)
        if (
            persisted.refresh_start != value.refresh_start
            or persisted.refresh_end != value.refresh_end
            or persisted.started_at != value.started_at
        ):
            raise PreviewSourceAttemptConflictError("source attempt token bounds conflict")
        if persisted.status is SourceAttemptStatus.PENDING:
            return self.finalize_attempt(
                persisted.attempt_sequence,
                SourceAttemptFinalStatus.FAILED,
                completed_at=completed,
                reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            )
        if (
            persisted.status is SourceAttemptStatus.FAILED
            and persisted.failure_reason is SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED
        ):
            return persisted
        raise PreviewSourceAttemptConflictError("source attempt token status conflicts")

    def finalize_attempt(
        self,
        attempt_sequence: int,
        status: SourceAttemptFinalStatus,
        *,
        completed_at: datetime,
        reason: SourceAttemptFailureReason | None,
    ) -> PreviewSourceAttempt:
        if not isinstance(status, SourceAttemptFinalStatus):
            raise ValueError("invalid source attempt final status")
        if reason is not None and not isinstance(reason, SourceAttemptFailureReason):
            raise ValueError("invalid source attempt failure reason")
        failed_reasons = {
            SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
            SourceAttemptFailureReason.CONSTRUCTION_FAILED,
            SourceAttemptFailureReason.PERSISTENCE_FAILED,
            SourceAttemptFailureReason.CAPABILITY_UNAVAILABLE,
            SourceAttemptFailureReason.BOOTSTRAP_INVALID,
            SourceAttemptFailureReason.BOOTSTRAP_CONCURRENT_CHANGE,
        }
        aborted_reasons = {
            SourceAttemptFailureReason.GENERIC_GATHER_FAILED,
            SourceAttemptFailureReason.GENERIC_COMMIT_FAILED,
        }
        if (
            (status is SourceAttemptFinalStatus.COMPLETE and reason is not None)
            or (status is SourceAttemptFinalStatus.FAILED and reason not in failed_reasons)
            or (status is SourceAttemptFinalStatus.ABORTED and reason not in aborted_reasons)
        ):
            raise ValueError("source attempt status and reason do not match")
        row = self._session.get(CCloudSourceEvidenceAttemptTable, attempt_sequence)
        normalized_completed = _require_aware(completed_at, "completed_at")
        expected_reason = None if reason is None else reason.value
        if (
            row is not None
            and row.status == status.value
            and row.completed_at is not None
            and _utc(row.completed_at) == normalized_completed
            and row.failure_reason == expected_reason
        ):
            return _source_attempt(row)
        if row is None or row.status != SourceAttemptStatus.PENDING.value:
            raise RuntimeError("source attempt is missing or already finalized")
        row.status = status.value
        row.completed_at = normalized_completed
        row.failure_reason = expected_reason
        self._session.add(row)
        self._session.flush([row])
        return _source_attempt(row)

    def get_current_authority(self, ecosystem: str, tenant_id: str) -> PreviewSourceAttempt | None:
        row = self._session.exec(
            select(CCloudSourceEvidenceAttemptTable)
            .where(
                col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
                col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
                col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.ABORTED.value,
            )
            .order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
            .limit(1)
        ).first()
        return None if row is None else _source_attempt(row)

    def resolve_authority(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> tuple[PreviewSourceAuthoritySlice, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        if start > end:
            raise ValueError("authority bounds must be ordered")
        base = select(CCloudSourceEvidenceAttemptTable).where(
            col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
            col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
            col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.ABORTED.value,
        )
        if start == end:
            rows = self._session.exec(
                base.order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
            ).all()
            selected = next(
                (
                    row
                    for row in rows
                    if source_attempt_origin(row.refresh_token) is PreviewSourceAttemptOrigin.ORDINARY
                    or _utc(row.refresh_start) <= start < _utc(row.refresh_end)
                ),
                None,
            )
            return (
                PreviewSourceAuthoritySlice(
                    start,
                    end,
                    None if selected is None else _source_attempt(selected),
                ),
            )
        rows = self._session.exec(
            base.where(
                col(CCloudSourceEvidenceAttemptTable.refresh_start) < end,
                col(CCloudSourceEvidenceAttemptTable.refresh_end) > start,
            ).order_by(col(CCloudSourceEvidenceAttemptTable.attempt_sequence).desc())
        ).all()
        unresolved: list[tuple[datetime, datetime]] = [(start, end)]
        assigned: list[PreviewSourceAuthoritySlice] = []
        for row in rows:
            attempt = _source_attempt(row)
            next_unresolved: list[tuple[datetime, datetime]] = []
            for segment_start, segment_end in unresolved:
                overlap_start = max(segment_start, attempt.refresh_start)
                overlap_end = min(segment_end, attempt.refresh_end)
                if overlap_start >= overlap_end:
                    next_unresolved.append((segment_start, segment_end))
                    continue
                if segment_start < overlap_start:
                    next_unresolved.append((segment_start, overlap_start))
                assigned.append(PreviewSourceAuthoritySlice(overlap_start, overlap_end, attempt))
                if overlap_end < segment_end:
                    next_unresolved.append((overlap_end, segment_end))
            unresolved = next_unresolved
            if not unresolved:
                break
        assigned.extend(PreviewSourceAuthoritySlice(left, right, None) for left, right in unresolved)
        ordered = sorted(assigned, key=lambda item: (item.start, item.end))
        merged: list[PreviewSourceAuthoritySlice] = []
        for item in ordered:
            if merged and merged[-1].end == item.start and merged[-1].attempt == item.attempt:
                previous = merged[-1]
                merged[-1] = PreviewSourceAuthoritySlice(previous.start, item.end, item.attempt)
            else:
                merged.append(item)
        return tuple(merged)

    def list_covering(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
    ) -> tuple[PreviewSourceReadiness, ...]:
        start = _require_aware(start, "start")
        end = _require_aware(end, "end")
        if start >= end:
            raise ValueError("readiness bounds must be ordered")
        rows = self._session.exec(
            select(CCloudSourceCaptureReadinessHistoryTable)
            .where(
                col(CCloudSourceCaptureReadinessHistoryTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessHistoryTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessHistoryTable.window_start) < end,
                col(CCloudSourceCaptureReadinessHistoryTable.window_end) > start,
            )
            .order_by(
                col(CCloudSourceCaptureReadinessHistoryTable.window_start),
                col(CCloudSourceCaptureReadinessHistoryTable.window_end),
                col(CCloudSourceCaptureReadinessHistoryTable.attempt_sequence),
            )
        ).all()
        return tuple(_readiness(row) for row in rows)

    def replace_overlapping(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_start: datetime,
        refresh_end: datetime,
        captures: Sequence[PreviewSourceReadiness],
    ) -> tuple[PreviewSourceReadiness, ...]:
        start = _require_aware(refresh_start, "refresh_start")
        end = _require_aware(refresh_end, "refresh_end")
        if start >= end:
            raise ValueError("readiness bounds must be ordered")
        canonical_captures = tuple(
            replace(
                capture,
                window_start=_require_aware(
                    capture.window_start,
                    "capture.window_start",
                ),
                window_end=_require_aware(
                    capture.window_end,
                    "capture.window_end",
                ),
                captured_at=_require_aware(
                    capture.captured_at,
                    "capture.captured_at",
                ),
            )
            for capture in captures
        )
        rows: list[CCloudSourceCaptureReadinessTable] = []
        cursor = start
        attempt_sequence: int | None = None
        for capture in canonical_captures:
            if capture.ecosystem != ecosystem or capture.tenant_id != tenant_id:
                raise ValueError("readiness owner mismatch")
            if capture.window_start != cursor or capture.window_end > end:
                raise ValueError("readiness rows must exactly partition the refresh interval")
            if attempt_sequence is None:
                attempt_sequence = capture.attempt_sequence
            elif capture.attempt_sequence != attempt_sequence:
                raise ValueError("readiness rows must use one attempt")
            cursor = capture.window_end
            rows.append(
                CCloudSourceCaptureReadinessTable(
                    ecosystem=capture.ecosystem,
                    tenant_id=capture.tenant_id,
                    window_start=capture.window_start,
                    window_end=capture.window_end,
                    capture_id=capture.capture_id,
                    captured_at=capture.captured_at,
                    source_count=capture.source_count,
                    attempt_sequence=capture.attempt_sequence,
                )
            )
        if cursor != end or attempt_sequence is None:
            raise ValueError("readiness rows must exactly partition the refresh interval")
        attempt = self._session.get(CCloudSourceEvidenceAttemptTable, attempt_sequence)
        if (
            attempt is None
            or attempt.status != SourceAttemptStatus.PENDING.value
            or attempt.ecosystem != ecosystem
            or attempt.tenant_id != tenant_id
            or _utc(attempt.refresh_start) != start
            or _utc(attempt.refresh_end) != end
        ):
            raise ValueError("readiness attempt is not the current pending refresh")
        self._session.exec(
            delete(CCloudSourceCaptureReadinessTable).where(
                col(CCloudSourceCaptureReadinessTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessTable.window_start) < end,
                col(CCloudSourceCaptureReadinessTable.window_end) > start,
            )
        )
        self._session.add_all(rows)
        for row in rows:
            self._session.merge(
                CCloudSourceCaptureReadinessHistoryTable(
                    ecosystem=row.ecosystem,
                    tenant_id=row.tenant_id,
                    attempt_sequence=row.attempt_sequence,
                    window_start=row.window_start,
                    window_end=row.window_end,
                    capture_id=row.capture_id,
                    captured_at=row.captured_at,
                    source_count=row.source_count,
                )
            )
        self._session.flush(rows)
        return tuple(_readiness(row) for row in rows)

    def delete_orphaned_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        cutoff = exclusive_utc_second_upper_bound(before, field="before")
        source_exists = exists().where(
            col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
            col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
            col(CCloudCostSourceRecordTable.capture_id) == col(CCloudSourceCaptureReadinessTable.capture_id),
        )
        result = self._session.exec(
            delete(CCloudSourceCaptureReadinessTable).where(
                col(CCloudSourceCaptureReadinessTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessTable.window_end) < cutoff,
                ~source_exists,
            )
        )
        deleted = int(getattr(result, "rowcount", 0))
        history_source_exists = exists().where(
            col(CCloudCostSourceRecordTable.ecosystem) == ecosystem,
            col(CCloudCostSourceRecordTable.tenant_id) == tenant_id,
            col(CCloudCostSourceRecordTable.capture_id) == col(CCloudSourceCaptureReadinessHistoryTable.capture_id),
        )
        history_result = self._session.exec(
            delete(CCloudSourceCaptureReadinessHistoryTable).where(
                col(CCloudSourceCaptureReadinessHistoryTable.ecosystem) == ecosystem,
                col(CCloudSourceCaptureReadinessHistoryTable.tenant_id) == tenant_id,
                col(CCloudSourceCaptureReadinessHistoryTable.window_end) < cutoff,
                ~history_source_exists,
            )
        )
        deleted += int(getattr(history_result, "rowcount", 0))
        current = self.get_current_authority(ecosystem, tenant_id)
        current_readiness_exists = exists().where(
            col(CCloudSourceCaptureReadinessTable.attempt_sequence)
            == col(CCloudSourceEvidenceAttemptTable.attempt_sequence)
        )
        history_readiness_exists = exists().where(
            col(CCloudSourceCaptureReadinessHistoryTable.attempt_sequence)
            == col(CCloudSourceEvidenceAttemptTable.attempt_sequence)
        )
        conditions = [
            col(CCloudSourceEvidenceAttemptTable.ecosystem) == ecosystem,
            col(CCloudSourceEvidenceAttemptTable.tenant_id) == tenant_id,
            col(CCloudSourceEvidenceAttemptTable.status) != SourceAttemptStatus.PENDING.value,
            col(CCloudSourceEvidenceAttemptTable.completed_at).is_not(None),
            col(CCloudSourceEvidenceAttemptTable.completed_at) < cutoff,
            ~current_readiness_exists,
            ~history_readiness_exists,
        ]
        if current is not None:
            conditions.append(col(CCloudSourceEvidenceAttemptTable.attempt_sequence) != current.attempt_sequence)
        self._session.exec(delete(CCloudSourceEvidenceAttemptTable).where(*conditions))
        return deleted


def _organization_attempt(row: CCloudOrganizationAuthorityAttemptTable) -> OrganizationAuthorityAttempt:
    if row.attempt_sequence is None:
        raise PreviewOrganizationAuthorityDecodeError("organization attempt sequence was not assigned")
    try:
        return OrganizationAuthorityAttempt(
            attempt_sequence=row.attempt_sequence,
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            status=OrganizationAuthorityAttemptStatus(row.status),
            started_at=_utc(row.started_at),
            completed_at=None if row.completed_at is None else _utc(row.completed_at),
            organization_id=row.organization_id,
            failure_reason=(
                None if row.failure_reason is None else OrganizationAuthorityFailureReason(row.failure_reason)
            ),
        )
    except ValueError as exc:
        raise PreviewOrganizationAuthorityDecodeError("invalid persisted organization authority") from exc


class SQLModelPreviewOrganizationAuthorityRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def begin(self, ecosystem: str, tenant_id: str, started_at: datetime) -> OrganizationAuthorityAttempt:
        if not ecosystem.strip() or not tenant_id.strip():
            raise ValueError("organization authority owner must not be blank")
        row = CCloudOrganizationAuthorityAttemptTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            status=OrganizationAuthorityAttemptStatus.PENDING.value,
            started_at=_require_aware(started_at, "started_at"),
        )
        self._session.add(row)
        self._session.flush([row])
        return _organization_attempt(row)

    def get_latest(self, ecosystem: str, tenant_id: str) -> OrganizationAuthorityAttempt | None:
        row = self._session.exec(
            select(CCloudOrganizationAuthorityAttemptTable)
            .where(
                col(CCloudOrganizationAuthorityAttemptTable.ecosystem) == ecosystem,
                col(CCloudOrganizationAuthorityAttemptTable.tenant_id) == tenant_id,
            )
            .order_by(col(CCloudOrganizationAuthorityAttemptTable.attempt_sequence).desc())
            .limit(1)
        ).first()
        return None if row is None else _organization_attempt(row)

    def finalize(
        self,
        attempt_sequence: int,
        status: OrganizationAuthorityFinalStatus,
        *,
        completed_at: datetime,
        organization_id: str | None,
        reason: OrganizationAuthorityFailureReason | None,
    ) -> OrganizationAuthorityAttempt:
        row = self._session.get(CCloudOrganizationAuthorityAttemptTable, attempt_sequence)
        normalized_completed = _require_aware(completed_at, "completed_at")
        expected_reason = None if reason is None else reason.value
        if (
            row is not None
            and row.status == status.value
            and row.completed_at is not None
            and _utc(row.completed_at) == normalized_completed
            and row.organization_id == organization_id
            and row.failure_reason == expected_reason
        ):
            return _organization_attempt(row)
        if row is None or row.status != OrganizationAuthorityAttemptStatus.PENDING.value:
            raise PreviewOrganizationAuthorityConflictError("organization authority attempt is not pending")
        row.status = status.value
        row.completed_at = normalized_completed
        row.organization_id = organization_id
        row.failure_reason = expected_reason
        self._session.add(row)
        self._session.flush([row])
        return _organization_attempt(row)

    def delete_superseded_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        latest = self.get_latest(ecosystem, tenant_id)
        cutoff = exclusive_utc_second_upper_bound(before, field="before")
        conditions = [
            col(CCloudOrganizationAuthorityAttemptTable.ecosystem) == ecosystem,
            col(CCloudOrganizationAuthorityAttemptTable.tenant_id) == tenant_id,
            col(CCloudOrganizationAuthorityAttemptTable.completed_at).is_not(None),
            col(CCloudOrganizationAuthorityAttemptTable.completed_at) < cutoff,
        ]
        if latest is not None:
            conditions.append(col(CCloudOrganizationAuthorityAttemptTable.attempt_sequence) != latest.attempt_sequence)
        result = self._session.exec(delete(CCloudOrganizationAuthorityAttemptTable).where(*conditions))
        return int(getattr(result, "rowcount", 0))
