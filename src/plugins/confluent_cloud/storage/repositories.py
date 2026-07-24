from __future__ import annotations

import json
import logging
from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import and_, delete, func, or_, update
from sqlmodel import Session, col, select

from core.preview.evidence import (
    AllocationLineageRunStatus,
    AllocationLineageUnavailableReason,
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewAllocationEvidenceDecodeError,
    PreviewAllocationRunEvidence,
    PreviewEvidenceScope,
    PreviewSourceEvidence,
    decode_lineage_decimal,
)
from core.preview.evidence_capture import (
    NativeSourceWindow,
    SourceWindowCount,
    SourceWindowWriteResult,
)
from core.storage.backends.sqlmodel.mappers import chargeback_to_dimension
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from core.storage.backends.sqlmodel.time_bounds import exact_utc_half_open_bounds
from core.storage.backends.sqlmodel.timestamps import (
    canonical_utc_second,
    exclusive_utc_second_upper_bound,
)
from core.storage.interface import AllocationLineageRunCapture, LineageCaptureStatus
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.storage.tables import (
    CCloudAllocationLineagePortionTable,
    CCloudAllocationLineageRunTable,
    CCloudBillingTable,
    CCloudCostSourceTable,
)

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.chargeback import ChargebackRow

logger = logging.getLogger(__name__)


def _date_to_range(d: date) -> tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _ensure_utc_strict(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        msg = f"Naive datetime not allowed — must be UTC-aware: {dt}"
        raise ValueError(msg)
    return dt.astimezone(UTC)


def _canonical_second(dt: datetime, field: str = "timestamp") -> datetime:
    return canonical_utc_second(dt, field=field)


def _metadata_to_json(metadata: dict[str, Any]) -> str | None:
    if not metadata:
        return None
    return json.dumps(metadata, default=str)


def _json_to_metadata(json_str: str | None) -> dict[str, Any]:
    if not json_str:
        return {}
    return json.loads(json_str)  # type: ignore[no-any-return]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _optional_decimal_string(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _source_to_table(record: CCloudCostSourceRecord) -> CCloudCostSourceTable:
    return CCloudCostSourceTable(
        ecosystem=record.ecosystem,
        tenant_id=record.tenant_id,
        source_record_id=record.source_record_id,
        identity_scheme=record.identity_scheme,
        provider_cost_id=record.provider_cost_id,
        source_period_start=None
        if record.source_period_start is None
        else _canonical_second(record.source_period_start, "source_period_start"),
        source_period_end=(
            None
            if record.source_period_end is None
            else _canonical_second(record.source_period_end, "source_period_end")
        ),
        collection_window_start=_canonical_second(
            record.collection_window_start,
            "collection_window_start",
        ),
        collection_window_end=_canonical_second(
            record.collection_window_end,
            "collection_window_end",
        ),
        evidence_scope_start=_canonical_second(
            record.evidence_scope_start,
            "evidence_scope_start",
        ),
        evidence_scope_end=_canonical_second(
            record.evidence_scope_end,
            "evidence_scope_end",
        ),
        allocation_timestamp=_canonical_second(
            record.allocation_timestamp,
            "allocation_timestamp",
        ),
        retention_timestamp=_canonical_second(
            record.retention_timestamp,
            "retention_timestamp",
        ),
        granularity=record.granularity,
        product=record.product,
        line_type=record.line_type,
        amount=_optional_decimal_string(record.amount),
        original_amount=_optional_decimal_string(record.original_amount),
        discount_amount=_optional_decimal_string(record.discount_amount),
        price=_optional_decimal_string(record.price),
        quantity=_optional_decimal_string(record.quantity),
        unit=record.unit,
        description=record.description,
        network_access_type=record.network_access_type,
        resource_id=record.resource_id,
        resource_name=record.resource_name,
        environment_id=record.environment_id,
        billing_timestamp=(
            None
            if record.billing_timestamp is None
            else _canonical_second(record.billing_timestamp, "billing_timestamp")
        ),
        billing_env_id=record.billing_env_id,
        billing_resource_id=record.billing_resource_id,
        billing_product_type=record.billing_product_type,
        billing_product_category=record.billing_product_category,
        tier_dimensions_json=_canonical_json(record.tier_dimensions),
        malformed=record.malformed,
        diagnostics_json=_canonical_json(record.diagnostics),
        raw_payload_json=_canonical_json(record.raw_payload),
    )


def _source_table_to_preview(row: CCloudCostSourceTable) -> PreviewSourceEvidence:
    tiers = json.loads(row.tier_dimensions_json)
    if not isinstance(tiers, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in tiers.items()
    ):
        raise ValueError("source tier dimensions must be a string mapping")
    if _canonical_json(tiers) != row.tier_dimensions_json:
        raise ValueError("source tier dimensions must use canonical JSON")
    diagnostics = json.loads(row.diagnostics_json)
    if not isinstance(diagnostics, list) or not all(isinstance(value, str) for value in diagnostics):
        raise ValueError("source diagnostics must be a string list")
    if _canonical_json(diagnostics) != row.diagnostics_json:
        raise ValueError("source diagnostics must use canonical JSON")
    raw_payload = json.loads(row.raw_payload_json)
    if not isinstance(raw_payload, dict) or _canonical_json(raw_payload) != row.raw_payload_json:
        raise ValueError("source raw payload must be a canonical JSON object")
    return PreviewSourceEvidence(
        source_record_id=row.source_record_id,
        identity_scheme=row.identity_scheme,
        provider_cost_id=row.provider_cost_id,
        source_period_start=_ensure_utc(row.source_period_start) if row.source_period_start else None,
        source_period_end=_ensure_utc(row.source_period_end) if row.source_period_end else None,
        collection_window_start=_ensure_utc(row.collection_window_start),
        collection_window_end=_ensure_utc(row.collection_window_end),
        evidence_scope_start=_ensure_utc(row.evidence_scope_start),
        evidence_scope_end=_ensure_utc(row.evidence_scope_end),
        allocation_timestamp=_ensure_utc(row.allocation_timestamp),
        granularity=row.granularity,
        native_product=row.product,
        native_line_type=row.line_type,
        amount=Decimal(row.amount) if row.amount is not None else None,
        original_amount=Decimal(row.original_amount) if row.original_amount is not None else None,
        discount_amount=Decimal(row.discount_amount) if row.discount_amount is not None else None,
        price=Decimal(row.price) if row.price is not None else None,
        quantity=Decimal(row.quantity) if row.quantity is not None else None,
        unit=row.unit,
        native_description=row.description,
        native_network_access_type=row.network_access_type,
        resource_id=row.resource_id,
        resource_name=row.resource_name,
        environment_id=row.environment_id,
        billing_timestamp=_ensure_utc(row.billing_timestamp) if row.billing_timestamp else None,
        billing_env_id=row.billing_env_id,
        billing_resource_id=row.billing_resource_id,
        billing_product_type=row.billing_product_type,
        billing_product_category=row.billing_product_category,
        capture_id=row.capture_id,
        ecosystem=row.ecosystem,
        tenant_id=row.tenant_id,
        retention_timestamp=_ensure_utc(row.retention_timestamp),
        raw_payload_json=row.raw_payload_json,
        native_tier_dimensions=tuple(sorted(tiers.items())),
        malformed=row.malformed,
        diagnostics=tuple(diagnostics),
    )


def _copy_source_scope(
    row: CCloudCostSourceTable, evidence_scope_start: datetime, evidence_scope_end: datetime
) -> CCloudCostSourceTable:
    values: dict[str, Any] = row.model_dump()
    values["evidence_scope_start"] = evidence_scope_start
    values["evidence_scope_end"] = evidence_scope_end
    values["retention_timestamp"] = evidence_scope_end
    return CCloudCostSourceTable(**values)


def _validate_utc_midnight(value: datetime, field: str) -> datetime:
    utc_value = _ensure_utc_strict(value)
    if any((utc_value.hour, utc_value.minute, utc_value.second, utc_value.microsecond)):
        raise ValueError(f"{field} must be UTC midnight")
    return utc_value


def _validate_source_record(
    record: CCloudCostSourceRecord,
    ecosystem: str,
    tenant_id: str,
    refresh_start: datetime,
    refresh_end: datetime,
) -> None:
    if record.ecosystem != ecosystem or record.tenant_id != tenant_id:
        raise ValueError("Source record ecosystem and tenant must match replacement owner")

    collection_start = _ensure_utc_strict(record.collection_window_start)
    collection_end = _ensure_utc_strict(record.collection_window_end)
    scope_start = _ensure_utc_strict(record.evidence_scope_start)
    scope_end = _ensure_utc_strict(record.evidence_scope_end)
    allocation = _ensure_utc_strict(record.allocation_timestamp)
    retention = _ensure_utc_strict(record.retention_timestamp)
    if collection_start >= collection_end:
        raise ValueError("Source record collection window must be non-empty")
    if not (refresh_start <= collection_start < collection_end <= refresh_end):
        raise ValueError("Source record collection window must be inside replacement window")
    if scope_start >= scope_end:
        raise ValueError("Source record evidence scope must be non-empty")

    if record.source_period_start is not None:
        source_start = _ensure_utc_strict(record.source_period_start)
        if allocation != source_start or retention != allocation:
            raise ValueError("Usable source start must equal allocation and retention timestamps")
        if not refresh_start <= allocation < refresh_end:
            raise ValueError("Source allocation timestamp must be inside replacement window")
    elif retention != scope_end:
        raise ValueError("Undated source retention timestamp must equal evidence scope end")

    association = (
        record.billing_timestamp,
        record.billing_env_id,
        record.billing_resource_id,
        record.billing_product_type,
        record.billing_product_category,
    )
    if any(value is None for value in association):
        raise ValueError("Source record billing association must be complete")
    billing_timestamp = _ensure_utc_strict(cast("datetime", record.billing_timestamp))
    if not record.malformed:
        billing_resource_id = cast("str", record.billing_resource_id)
        expected_billing_product_category = record.product or ""
        resource_matches = billing_resource_id == record.resource_id or (
            record.resource_id is None and billing_resource_id.startswith("unresolved_billing_")
        )
        if (
            billing_timestamp != allocation
            or record.billing_env_id != (record.environment_id or "")
            or not resource_matches
            or record.billing_product_type != record.line_type
            or record.billing_product_category != expected_billing_product_category
        ):
            raise ValueError("Source record billing association is inconsistent with mapped billing identity")


def _line_to_table(line: CCloudBillingLineItem) -> CCloudBillingTable:
    return CCloudBillingTable(
        ecosystem=line.ecosystem,
        tenant_id=line.tenant_id,
        timestamp=_canonical_second(line.timestamp),
        env_id=line.env_id,
        resource_id=line.resource_id,
        product_type=line.product_type,
        product_category=line.product_category,
        quantity=str(line.quantity),
        unit_price=str(line.unit_price),
        total_cost=str(line.total_cost),
        currency=line.currency,
        granularity=line.granularity,
        metadata_json=_metadata_to_json(line.metadata),
    )


def _table_to_line(t: CCloudBillingTable) -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem=t.ecosystem,
        tenant_id=t.tenant_id,
        timestamp=_ensure_utc(t.timestamp),
        env_id=t.env_id,
        resource_id=t.resource_id,
        product_type=t.product_type,
        product_category=t.product_category,
        quantity=Decimal(t.quantity),
        unit_price=Decimal(t.unit_price),
        total_cost=Decimal(t.total_cost),
        currency=t.currency,
        granularity=t.granularity,
        metadata=_json_to_metadata(t.metadata_json),
    )


def _billing_pk(line: CCloudBillingLineItem) -> tuple[str, str, datetime, str, str, str, str]:
    return (
        line.ecosystem,
        line.tenant_id,
        _canonical_second(line.timestamp),
        line.env_id,
        line.resource_id,
        line.product_type,
        line.product_category,
    )


class CCloudBillingRepository:
    """BillingRepository for Confluent Cloud billing with 7-field composite PK."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, line: BillingLineItem) -> CCloudBillingLineItem:
        ccloud_line = cast("CCloudBillingLineItem", line)
        table_obj = _line_to_table(ccloud_line)

        existing = self._session.get(CCloudBillingTable, _billing_pk(ccloud_line))
        if existing is not None and existing.total_cost != table_obj.total_cost:
            logger.warning(
                "Billing revision detected: %s/%s/%s cost changed %s → %s",
                table_obj.resource_id,
                table_obj.product_type,
                table_obj.timestamp.date(),
                existing.total_cost,
                table_obj.total_cost,
            )

        merged = self._session.merge(table_obj)
        self._session.flush()
        return _table_to_line(merged)

    def replace_for_date(
        self,
        ecosystem: str,
        tenant_id: str,
        tracking_date: date,
        lines: Sequence[BillingLineItem],
    ) -> int:
        validated: list[CCloudBillingLineItem] = []
        natural_keys: set[tuple[str, str, datetime, str, str, str, str]] = set()
        for line in lines:
            if line.ecosystem != ecosystem or line.tenant_id != tenant_id:
                raise ValueError("historical repair billing owner mismatch")
            timestamp = _canonical_second(line.timestamp)
            if timestamp.date() != tracking_date:
                raise ValueError("historical repair billing timestamp outside requested date")
            env_id = getattr(line, "env_id", None)
            if not isinstance(env_id, str) or not env_id.strip():
                raise ValueError("historical repair billing line requires env_id")
            ccloud_line = cast("CCloudBillingLineItem", line)
            key = (
                ecosystem,
                tenant_id,
                timestamp,
                env_id,
                line.resource_id,
                line.product_type,
                line.product_category,
            )
            if key in natural_keys:
                raise ValueError("duplicate historical repair billing natural key")
            natural_keys.add(key)
            validated.append(ccloud_line)

        start, end = exact_utc_half_open_bounds(self._session, *_date_to_range(tracking_date))
        self._session.exec(
            delete(CCloudBillingTable).where(
                col(CCloudBillingTable.ecosystem) == ecosystem,
                col(CCloudBillingTable.tenant_id) == tenant_id,
                col(CCloudBillingTable.timestamp) >= start,
                col(CCloudBillingTable.timestamp) < end,
            )
        )
        rows = [_line_to_table(line) for line in validated]
        self._session.add_all(rows)
        self._session.flush()
        return len(rows)

    def replace_source_window(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_window_start: datetime,
        refresh_window_end: datetime,
        records: Sequence[CCloudCostSourceRecord],
    ) -> SourceWindowWriteResult:
        refresh_start = _validate_utc_midnight(refresh_window_start, "refresh_window_start")
        refresh_end = _validate_utc_midnight(refresh_window_end, "refresh_window_end")
        if refresh_start >= refresh_end:
            raise ValueError("Source replacement window must be non-empty")
        for record in records:
            _validate_source_record(record, ecosystem, tenant_id, refresh_start, refresh_end)
        table_records_by_key: dict[
            tuple[str, str, str, datetime, datetime],
            CCloudCostSourceTable,
        ] = {}
        for record in records:
            table_record = _source_to_table(record)
            key = (
                table_record.ecosystem,
                table_record.tenant_id,
                table_record.source_record_id,
                table_record.evidence_scope_start,
                table_record.evidence_scope_end,
            )
            existing = table_records_by_key.get(key)
            if existing is not None and existing.model_dump() != table_record.model_dump():
                raise ValueError("conflicting source records resolve to the same canonical natural key")
            table_records_by_key[key] = table_record
        table_records = list(table_records_by_key.values())

        valid_delete = delete(CCloudCostSourceTable).where(
            col(CCloudCostSourceTable.ecosystem) == ecosystem,
            col(CCloudCostSourceTable.tenant_id) == tenant_id,
            col(CCloudCostSourceTable.source_period_start).is_not(None),
            col(CCloudCostSourceTable.allocation_timestamp) >= refresh_start,
            col(CCloudCostSourceTable.allocation_timestamp) < refresh_end,
        )
        self._session.execute(valid_delete)

        overlap_stmt = select(CCloudCostSourceTable).where(
            col(CCloudCostSourceTable.ecosystem) == ecosystem,
            col(CCloudCostSourceTable.tenant_id) == tenant_id,
            col(CCloudCostSourceTable.source_period_start).is_(None),
            col(CCloudCostSourceTable.evidence_scope_start) < refresh_end,
            col(CCloudCostSourceTable.evidence_scope_end) > refresh_start,
        )
        residuals: list[CCloudCostSourceTable] = []
        for existing in self._session.exec(overlap_stmt).all():
            existing_start = _ensure_utc(existing.evidence_scope_start)
            existing_end = _ensure_utc(existing.evidence_scope_end)
            self._session.delete(existing)
            if existing_start < refresh_start:
                residuals.append(_copy_source_scope(existing, existing_start, min(existing_end, refresh_start)))
            if existing_end > refresh_end:
                residuals.append(_copy_source_scope(existing, max(existing_start, refresh_end), existing_end))

        self._session.flush()
        self._session.add_all(residuals)
        self._session.add_all(table_records)
        self._session.flush()
        counts = Counter(
            (
                record.collection_window_start,
                record.collection_window_end,
            )
            for record in table_records
        )
        return SourceWindowWriteResult(
            records_written=len(table_records),
            window_counts=tuple(
                SourceWindowCount(
                    window=NativeSourceWindow(start, end),
                    source_count=count,
                )
                for (start, end), count in sorted(counts.items())
            ),
        )

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[CCloudBillingLineItem]:
        start, end = exact_utc_half_open_bounds(self._session, *_date_to_range(target_date))
        stmt = select(CCloudBillingTable).where(
            col(CCloudBillingTable.ecosystem) == ecosystem,
            col(CCloudBillingTable.tenant_id) == tenant_id,
            col(CCloudBillingTable.timestamp) >= start,
            col(CCloudBillingTable.timestamp) < end,
        )
        return [_table_to_line(r) for r in self._session.exec(stmt).all()]

    def find_by_range(
        self, ecosystem: str, tenant_id: str, start: datetime, end: datetime
    ) -> list[CCloudBillingLineItem]:
        start, end = exact_utc_half_open_bounds(self._session, start, end)
        stmt = select(CCloudBillingTable).where(
            col(CCloudBillingTable.ecosystem) == ecosystem,
            col(CCloudBillingTable.tenant_id) == tenant_id,
            col(CCloudBillingTable.timestamp) >= start,
            col(CCloudBillingTable.timestamp) < end,
        )
        return [_table_to_line(r) for r in self._session.exec(stmt).all()]

    def find_preview_source_candidates(self, scope: PreviewEvidenceScope) -> tuple[PreviewSourceEvidence, ...]:
        scope_start, scope_end = exact_utc_half_open_bounds(
            self._session,
            scope.start,
            scope.end,
        )
        dated_overlap = (
            col(CCloudCostSourceTable.malformed) == False,  # noqa: E712
            col(CCloudCostSourceTable.source_period_start).is_not(None),
            col(CCloudCostSourceTable.source_period_end).is_not(None),
            col(CCloudCostSourceTable.source_period_start) < scope_end,
            col(CCloudCostSourceTable.source_period_end) > scope_start,
        )
        fallback_overlap = (
            or_(
                col(CCloudCostSourceTable.malformed) == True,  # noqa: E712
                col(CCloudCostSourceTable.source_period_start).is_(None),
                col(CCloudCostSourceTable.source_period_end).is_(None),
            ),
            col(CCloudCostSourceTable.evidence_scope_start) < scope_end,
            col(CCloudCostSourceTable.evidence_scope_end) > scope_start,
        )
        statement = (
            select(CCloudCostSourceTable)
            .where(
                col(CCloudCostSourceTable.ecosystem) == scope.ecosystem,
                col(CCloudCostSourceTable.tenant_id) == scope.tenant_id,
                or_(and_(*dated_overlap), and_(*fallback_overlap)),
            )
            .order_by(
                col(CCloudCostSourceTable.evidence_scope_start),
                col(CCloudCostSourceTable.evidence_scope_end),
                col(CCloudCostSourceTable.source_record_id),
                col(CCloudCostSourceTable.identity_scheme),
            )
            .limit(2)
        )
        return tuple(_source_table_to_preview(row) for row in self._session.exec(statement).all())

    def iter_preview_sources(self, scope: PreviewEvidenceScope) -> Iterator[PreviewSourceEvidence]:
        scope_start, scope_end = exact_utc_half_open_bounds(
            self._session,
            scope.start,
            scope.end,
        )
        dated_overlap = (
            col(CCloudCostSourceTable.malformed) == False,  # noqa: E712
            col(CCloudCostSourceTable.source_period_start).is_not(None),
            col(CCloudCostSourceTable.source_period_end).is_not(None),
            col(CCloudCostSourceTable.source_period_start) < scope_end,
            col(CCloudCostSourceTable.source_period_end) > scope_start,
        )
        fallback_overlap = (
            or_(
                col(CCloudCostSourceTable.malformed) == True,  # noqa: E712
                col(CCloudCostSourceTable.source_period_start).is_(None),
                col(CCloudCostSourceTable.source_period_end).is_(None),
            ),
            col(CCloudCostSourceTable.evidence_scope_start) < scope_end,
            col(CCloudCostSourceTable.evidence_scope_end) > scope_start,
        )
        statement = (
            select(CCloudCostSourceTable)
            .where(
                col(CCloudCostSourceTable.ecosystem) == scope.ecosystem,
                col(CCloudCostSourceTable.tenant_id) == scope.tenant_id,
                or_(and_(*dated_overlap), and_(*fallback_overlap)),
            )
            .order_by(
                col(CCloudCostSourceTable.allocation_timestamp),
                col(CCloudCostSourceTable.environment_id).nulls_first(),
                col(CCloudCostSourceTable.resource_id).nulls_first(),
                col(CCloudCostSourceTable.product).nulls_first(),
                col(CCloudCostSourceTable.line_type).nulls_first(),
                col(CCloudCostSourceTable.source_record_id),
                col(CCloudCostSourceTable.identity_scheme),
            )
            .execution_options(yield_per=256, stream_results=True)
        )
        rows = self._session.exec(statement).yield_per(256)
        for row in rows:
            yield _source_table_to_preview(row)

    def iter_preview_aggregates(self, scope: PreviewEvidenceScope) -> Iterator[PreviewAggregateEvidence]:
        start, end = exact_utc_half_open_bounds(self._session, scope.start, scope.end)
        statement = (
            select(CCloudBillingTable)
            .where(
                col(CCloudBillingTable.ecosystem) == scope.ecosystem,
                col(CCloudBillingTable.tenant_id) == scope.tenant_id,
                col(CCloudBillingTable.timestamp) >= start,
                col(CCloudBillingTable.timestamp) < end,
            )
            .order_by(
                col(CCloudBillingTable.timestamp),
                col(CCloudBillingTable.env_id),
                col(CCloudBillingTable.resource_id),
                col(CCloudBillingTable.product_category),
                col(CCloudBillingTable.product_type),
            )
            .execution_options(yield_per=256, stream_results=True)
        )
        rows = self._session.exec(statement).yield_per(256)
        for row in rows:
            yield PreviewAggregateEvidence(
                timestamp=_ensure_utc(row.timestamp),
                environment_id=row.env_id,
                resource_id=row.resource_id,
                native_product=row.product_category,
                native_line_type=row.product_type,
                quantity=Decimal(row.quantity),
                unit_price=Decimal(row.unit_price),
                total_cost=Decimal(row.total_cost),
                compatibility_currency=row.currency,
                granularity=row.granularity,
            )

    def find_preview_aggregate_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAggregateEvidence, ...]:
        statement = (
            select(CCloudBillingTable)
            .where(
                col(CCloudBillingTable.ecosystem) == scope.ecosystem,
                col(CCloudBillingTable.tenant_id) == scope.tenant_id,
                col(CCloudBillingTable.timestamp)
                == _canonical_second(
                    source.allocation_timestamp,
                    "source.allocation_timestamp",
                ),
                col(CCloudBillingTable.env_id) == source.environment_id,
                col(CCloudBillingTable.resource_id) == source.resource_id,
                col(CCloudBillingTable.product_category) == source.native_product,
                col(CCloudBillingTable.product_type) == source.native_line_type,
            )
            .order_by(
                col(CCloudBillingTable.timestamp),
                col(CCloudBillingTable.env_id),
                col(CCloudBillingTable.resource_id),
                col(CCloudBillingTable.product_category),
                col(CCloudBillingTable.product_type),
            )
            .limit(2)
        )
        return tuple(
            PreviewAggregateEvidence(
                timestamp=_ensure_utc(row.timestamp),
                environment_id=row.env_id,
                resource_id=row.resource_id,
                native_product=row.product_category,
                native_line_type=row.product_type,
                quantity=Decimal(row.quantity),
                unit_price=Decimal(row.unit_price),
                total_cost=Decimal(row.total_cost),
                compatibility_currency=row.currency,
                granularity=row.granularity,
            )
            for row in self._session.exec(statement).all()
        )

    def _increment_int_column(self, line: BillingLineItem, attr: str) -> int:
        ccloud_line = cast("CCloudBillingLineItem", line)
        row = self._session.get(CCloudBillingTable, _billing_pk(ccloud_line))
        if row is None:
            msg = (
                f"Billing line not found: ecosystem={line.ecosystem!r}, tenant_id={line.tenant_id!r}, "
                f"timestamp={line.timestamp!r}, resource_id={line.resource_id!r}, "
                f"product_type={line.product_type!r}, product_category={line.product_category!r}"
            )
            raise KeyError(msg)
        setattr(row, attr, getattr(row, attr) + 1)
        self._session.add(row)
        self._session.flush()
        return int(getattr(row, attr))

    def increment_allocation_attempts(self, line: BillingLineItem) -> int:
        return self._increment_int_column(line, "allocation_attempts")

    def increment_topic_attribution_attempts(self, line: BillingLineItem) -> int:
        return self._increment_int_column(line, "topic_attribution_attempts")

    def _reset_int_column_by_date(self, ecosystem: str, tenant_id: str, tracking_date: date, attr: str) -> int:
        start, end = exact_utc_half_open_bounds(self._session, *_date_to_range(tracking_date))
        stmt = (
            update(CCloudBillingTable)
            .where(
                col(CCloudBillingTable.ecosystem) == ecosystem,
                col(CCloudBillingTable.tenant_id) == tenant_id,
                col(CCloudBillingTable.timestamp) >= start,
                col(CCloudBillingTable.timestamp) < end,
            )
            .values({attr: 0})
        )
        result = self._session.execute(stmt)
        self._session.flush()
        return result.rowcount  # type: ignore[attr-defined, no-any-return]

    def reset_allocation_attempts_by_date(self, ecosystem: str, tenant_id: str, tracking_date: date) -> int:
        return self._reset_int_column_by_date(ecosystem, tenant_id, tracking_date, "allocation_attempts")

    def reset_topic_attribution_attempts_by_date(self, ecosystem: str, tenant_id: str, tracking_date: date) -> int:
        return self._reset_int_column_by_date(ecosystem, tenant_id, tracking_date, "topic_attribution_attempts")

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        before = exclusive_utc_second_upper_bound(before)
        stmt = delete(CCloudBillingTable).where(
            col(CCloudBillingTable.ecosystem) == ecosystem,
            col(CCloudBillingTable.tenant_id) == tenant_id,
            col(CCloudBillingTable.timestamp) < before,
        )
        result = self._session.execute(stmt)
        return result.rowcount  # type: ignore[attr-defined, no-any-return]

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[CCloudBillingLineItem], int]:
        where: list[Any] = [
            col(CCloudBillingTable.ecosystem) == ecosystem,
            col(CCloudBillingTable.tenant_id) == tenant_id,
        ]
        if start is not None:
            start = exact_utc_half_open_bounds(
                self._session,
                start,
                start,
            )[0]
            where.append(col(CCloudBillingTable.timestamp) >= start)
        if end is not None:
            end = exact_utc_half_open_bounds(
                self._session,
                end,
                end,
            )[0]
            where.append(col(CCloudBillingTable.timestamp) < end)
        if product_type is not None:
            where.append(col(CCloudBillingTable.product_type) == product_type)
        if resource_id is not None:
            where.append(col(CCloudBillingTable.resource_id) == resource_id)

        count_stmt = select(func.count()).select_from(CCloudBillingTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        stmt = select(CCloudBillingTable).where(*where).offset(offset).limit(limit)
        items = [_table_to_line(r) for r in self._session.exec(stmt).all()]
        return items, total


class CCloudChargebackRepository(SQLModelChargebackRepository):
    """ChargebackRepository for Confluent Cloud.

    Extends the core repo to include env_id in the dimension natural key and
    lookup query. env_id is read from row.metadata (set by orchestrator via
    AllocationContext.dimension_metadata).

    All other methods (find_*, aggregate, delete_*, iter_*) are inherited
    unchanged — aggregate() uses ChargebackDimensionTable.env_id natively
    after the core aggregate() fix.
    """

    def _make_dimension_key(self, row: ChargebackRow) -> tuple[str | None, ...]:
        base = super()._make_dimension_key(row)
        return (*base, row.metadata.get("env_id", ""))

    def replace_calculation_lineage(
        self,
        run: AllocationLineageRunCapture,
        *,
        calculation_completed_at: datetime,
    ) -> None:
        if not run.calculation_id:
            raise ValueError("calculation_id must not be empty")
        completed_at = _canonical_second(
            calculation_completed_at,
            "calculation_completed_at",
        )
        self._session.execute(
            delete(CCloudAllocationLineagePortionTable).where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == run.ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == run.tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date) == run.tracking_date,
            )
        )
        self._session.execute(
            delete(CCloudAllocationLineageRunTable).where(
                col(CCloudAllocationLineageRunTable.ecosystem) == run.ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == run.tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date) == run.tracking_date,
            )
        )
        invalid = next((capture for capture in run.captures if capture.status is LineageCaptureStatus.INVALID), None)
        portions = [
            CCloudAllocationLineagePortionTable(
                ecosystem=run.ecosystem,
                tenant_id=run.tenant_id,
                tracking_date=run.tracking_date,
                calculation_id=run.calculation_id,
                origin_timestamp=_canonical_second(
                    capture.origin_timestamp,
                    "origin_timestamp",
                ),
                origin_env_id=capture.origin_env_id,
                origin_resource_id=capture.origin_resource_id,
                origin_product_type=capture.origin_product_type,
                origin_product_category=capture.origin_product_category,
                portion_ordinal=fact.portion_ordinal,
                target_kind=fact.target_kind.value,
                target_id=fact.target_id,
                allocated_cost=str(fact.allocated_cost),
                allocated_quantity=str(fact.allocated_quantity),
                allocation_ratio=str(fact.allocation_ratio),
                method_id=fact.method_id,
                method_version=fact.method_version,
                method_details_json=fact.method_details_json,
            )
            for capture in run.captures
            if capture.status is LineageCaptureStatus.COMPLETE
            for fact in capture.facts
        ]
        self._session.add(
            CCloudAllocationLineageRunTable(
                ecosystem=run.ecosystem,
                tenant_id=run.tenant_id,
                tracking_date=run.tracking_date,
                calculation_id=run.calculation_id,
                calculation_completed_at=completed_at,
                capture_status=(
                    LineageCaptureStatus.INVALID.value if invalid is not None else LineageCaptureStatus.COMPLETE.value
                ),
                capture_reason=None if invalid is None or invalid.reason is None else invalid.reason.value,
                portion_count=len(portions),
            )
        )
        self._session.add_all(portions)
        self._session.flush()

    def iter_preview_allocation_runs(
        self,
        scope: PreviewEvidenceScope,
        calculation_ids: tuple[str, ...],
    ) -> Iterator[PreviewAllocationRunEvidence]:
        if not calculation_ids:
            return
        statement = (
            select(CCloudAllocationLineageRunTable)
            .where(
                col(CCloudAllocationLineageRunTable.ecosystem) == scope.ecosystem,
                col(CCloudAllocationLineageRunTable.tenant_id) == scope.tenant_id,
                col(CCloudAllocationLineageRunTable.tracking_date) >= scope.start.date(),
                col(CCloudAllocationLineageRunTable.tracking_date) < scope.end.date(),
                col(CCloudAllocationLineageRunTable.calculation_id).in_(calculation_ids),
            )
            .order_by(col(CCloudAllocationLineageRunTable.tracking_date))
            .execution_options(yield_per=256, stream_results=True)
        )
        for row in self._session.exec(statement).yield_per(256):
            try:
                yield PreviewAllocationRunEvidence(
                    ecosystem=row.ecosystem,
                    tenant_id=row.tenant_id,
                    tracking_date=row.tracking_date,
                    calculation_id=row.calculation_id,
                    calculation_completed_at=_ensure_utc(row.calculation_completed_at),
                    capture_status=AllocationLineageRunStatus(row.capture_status),
                    capture_reason=(
                        None if row.capture_reason is None else AllocationLineageUnavailableReason(row.capture_reason)
                    ),
                    portion_count=row.portion_count,
                )
            except (AttributeError, TypeError, ValueError) as exc:
                raise PreviewAllocationEvidenceDecodeError("invalid persisted allocation lineage run") from exc

    def iter_preview_allocations(
        self,
        scope: PreviewEvidenceScope,
        calculation_ids: tuple[str, ...],
    ) -> Iterator[PreviewAllocationEvidence]:
        if not calculation_ids:
            return
        statement = (
            select(CCloudAllocationLineagePortionTable, CCloudBillingTable)
            .join(
                CCloudBillingTable,
                and_(
                    col(CCloudBillingTable.ecosystem) == col(CCloudAllocationLineagePortionTable.ecosystem),
                    col(CCloudBillingTable.tenant_id) == col(CCloudAllocationLineagePortionTable.tenant_id),
                    col(CCloudBillingTable.timestamp) == col(CCloudAllocationLineagePortionTable.origin_timestamp),
                    col(CCloudBillingTable.env_id) == col(CCloudAllocationLineagePortionTable.origin_env_id),
                    col(CCloudBillingTable.resource_id) == col(CCloudAllocationLineagePortionTable.origin_resource_id),
                    col(CCloudBillingTable.product_type)
                    == col(CCloudAllocationLineagePortionTable.origin_product_type),
                    col(CCloudBillingTable.product_category)
                    == col(CCloudAllocationLineagePortionTable.origin_product_category),
                ),
            )
            .where(
                col(CCloudAllocationLineagePortionTable.ecosystem) == scope.ecosystem,
                col(CCloudAllocationLineagePortionTable.tenant_id) == scope.tenant_id,
                col(CCloudAllocationLineagePortionTable.tracking_date) >= scope.start.date(),
                col(CCloudAllocationLineagePortionTable.tracking_date) < scope.end.date(),
                col(CCloudAllocationLineagePortionTable.calculation_id).in_(calculation_ids),
            )
            .order_by(
                col(CCloudAllocationLineagePortionTable.origin_timestamp),
                col(CCloudAllocationLineagePortionTable.origin_env_id),
                col(CCloudAllocationLineagePortionTable.origin_resource_id),
                col(CCloudAllocationLineagePortionTable.origin_product_type),
                col(CCloudAllocationLineagePortionTable.origin_product_category),
                col(CCloudAllocationLineagePortionTable.portion_ordinal),
            )
            .execution_options(yield_per=256, stream_results=True)
        )
        for portion, origin in self._session.exec(statement).yield_per(256):
            allocated_cost = decode_lineage_decimal(portion.allocated_cost)
            allocated_quantity = decode_lineage_decimal(portion.allocated_quantity)
            allocation_ratio = decode_lineage_decimal(portion.allocation_ratio)
            yield PreviewAllocationEvidence(
                timestamp=_ensure_utc(origin.timestamp),
                environment_id=origin.env_id,
                resource_id=origin.resource_id,
                native_product=origin.product_category,
                native_line_type=origin.product_type,
                allocation_target_id=portion.target_id or "UNALLOCATED",
                allocation_method=portion.method_id,
                amount=allocated_cost,
                calculation_id=portion.calculation_id,
                portion_ordinal=portion.portion_ordinal,
                target_kind=portion.target_kind,
                target_id=portion.target_id,
                allocated_cost=allocated_cost,
                allocated_quantity=allocated_quantity,
                allocation_ratio=allocation_ratio,
                method_id=portion.method_id,
                method_version=portion.method_version,
                method_details_json=portion.method_details_json,
                origin_total_cost=Decimal(origin.total_cost),
                origin_quantity=Decimal(origin.quantity),
                origin_unit_price=Decimal(origin.unit_price),
                origin_currency=origin.currency,
                origin_granularity=origin.granularity,
            )

    def find_preview_allocation_candidates(
        self, scope: PreviewEvidenceScope, source: PreviewSourceEvidence
    ) -> tuple[PreviewAllocationEvidence, ...]:
        statement = (
            select(ChargebackDimensionTable, ChargebackFactTable)
            .join(
                ChargebackFactTable,
                col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id),
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == scope.ecosystem,
                col(ChargebackDimensionTable.tenant_id) == scope.tenant_id,
                col(ChargebackFactTable.timestamp)
                == _canonical_second(
                    source.allocation_timestamp,
                    "source.allocation_timestamp",
                ),
                col(ChargebackDimensionTable.env_id) == source.environment_id,
                col(ChargebackDimensionTable.resource_id) == source.resource_id,
                col(ChargebackDimensionTable.product_category) == source.native_product,
                col(ChargebackDimensionTable.product_type) == source.native_line_type,
            )
            .order_by(
                col(ChargebackFactTable.timestamp),
                col(ChargebackDimensionTable.env_id),
                col(ChargebackDimensionTable.resource_id),
                col(ChargebackDimensionTable.product_category),
                col(ChargebackDimensionTable.product_type),
                col(ChargebackDimensionTable.identity_id),
            )
            .limit(2)
        )
        return tuple(
            PreviewAllocationEvidence(
                timestamp=_ensure_utc(fact.timestamp),
                environment_id=dimension.env_id,
                resource_id=dimension.resource_id or "",
                native_product=dimension.product_category,
                native_line_type=dimension.product_type,
                allocation_target_id=dimension.identity_id,
                allocation_method=dimension.allocation_method or "",
                amount=Decimal(fact.amount),
            )
            for dimension, fact in self._session.exec(statement).all()
        )

    def _get_or_create_dimension(self, row: ChargebackRow) -> ChargebackDimensionTable:
        # Full override (not super()) required because the SQL WHERE clause must include
        # env_id to match the 10-field unique constraint. The parent's WHERE clause only
        # covers 9 fields — reusing it would produce false cache hits for rows that share
        # all fields except env_id.
        key = self._make_dimension_key(row)
        cached = self._dimension_cache.get(key)
        if cached is not None:
            return cached

        env_id = row.metadata.get("env_id", "")
        stmt = select(ChargebackDimensionTable).where(
            col(ChargebackDimensionTable.ecosystem) == row.ecosystem,
            col(ChargebackDimensionTable.tenant_id) == row.tenant_id,
            col(ChargebackDimensionTable.resource_id) == row.resource_id,
            col(ChargebackDimensionTable.product_category) == row.product_category,
            col(ChargebackDimensionTable.product_type) == row.product_type,
            col(ChargebackDimensionTable.identity_id) == row.identity_id,
            col(ChargebackDimensionTable.cost_type) == row.cost_type.value,
            col(ChargebackDimensionTable.allocation_method) == row.allocation_method,
            col(ChargebackDimensionTable.allocation_detail) == row.allocation_detail,
            col(ChargebackDimensionTable.env_id) == env_id,
        )
        existing = self._session.exec(stmt).first()
        if existing:
            assert existing.dimension_id is not None
            self._dimension_cache[key] = existing
            return existing

        # chargeback_to_dimension() already sets env_id via row.metadata.get("env_id", "")
        dim = chargeback_to_dimension(row)
        self._session.add(dim)
        self._session.flush()
        assert dim.dimension_id is not None
        self._dimension_cache[key] = dim
        return dim
