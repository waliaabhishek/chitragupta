from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import and_, delete, func, or_, update
from sqlmodel import Session, col, select

from core.preview.evidence import (
    PreviewAggregateEvidence,
    PreviewAllocationEvidence,
    PreviewEvidenceScope,
    PreviewSourceEvidence,
)
from core.storage.backends.sqlmodel.mappers import chargeback_to_dimension
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.storage.tables import CCloudBillingTable, CCloudCostSourceTable

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
        else _ensure_utc_strict(record.source_period_start),
        source_period_end=None if record.source_period_end is None else _ensure_utc_strict(record.source_period_end),
        collection_window_start=_ensure_utc_strict(record.collection_window_start),
        collection_window_end=_ensure_utc_strict(record.collection_window_end),
        evidence_scope_start=_ensure_utc_strict(record.evidence_scope_start),
        evidence_scope_end=_ensure_utc_strict(record.evidence_scope_end),
        allocation_timestamp=_ensure_utc_strict(record.allocation_timestamp),
        retention_timestamp=_ensure_utc_strict(record.retention_timestamp),
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
    diagnostics = json.loads(row.diagnostics_json)
    if not isinstance(diagnostics, list) or not all(isinstance(value, str) for value in diagnostics):
        raise ValueError("source diagnostics must be a string list")
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


def _line_to_table(line: CCloudBillingLineItem) -> CCloudBillingTable:
    return CCloudBillingTable(
        ecosystem=line.ecosystem,
        tenant_id=line.tenant_id,
        timestamp=_ensure_utc_strict(line.timestamp),
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
        line.timestamp,
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

    def replace_source_window(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_window_start: datetime,
        refresh_window_end: datetime,
        records: Sequence[CCloudCostSourceRecord],
    ) -> None:
        refresh_start = _validate_utc_midnight(refresh_window_start, "refresh_window_start")
        refresh_end = _validate_utc_midnight(refresh_window_end, "refresh_window_end")
        if refresh_start >= refresh_end:
            raise ValueError("Source replacement window must be non-empty")
        for record in records:
            _validate_source_record(record, ecosystem, tenant_id, refresh_start, refresh_end)
        table_records = [_source_to_table(record) for record in records]

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

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[CCloudBillingLineItem]:
        start, end = _date_to_range(target_date)
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
        stmt = select(CCloudBillingTable).where(
            col(CCloudBillingTable.ecosystem) == ecosystem,
            col(CCloudBillingTable.tenant_id) == tenant_id,
            col(CCloudBillingTable.timestamp) >= start,
            col(CCloudBillingTable.timestamp) < end,
        )
        return [_table_to_line(r) for r in self._session.exec(stmt).all()]

    def find_preview_source_candidates(self, scope: PreviewEvidenceScope) -> tuple[PreviewSourceEvidence, ...]:
        dated_overlap = (
            col(CCloudCostSourceTable.malformed) == False,  # noqa: E712
            col(CCloudCostSourceTable.source_period_start).is_not(None),
            col(CCloudCostSourceTable.source_period_end).is_not(None),
            col(CCloudCostSourceTable.source_period_start) < scope.end,
            col(CCloudCostSourceTable.source_period_end) > scope.start,
        )
        fallback_overlap = (
            or_(
                col(CCloudCostSourceTable.malformed) == True,  # noqa: E712
                col(CCloudCostSourceTable.source_period_start).is_(None),
                col(CCloudCostSourceTable.source_period_end).is_(None),
            ),
            col(CCloudCostSourceTable.evidence_scope_start) < scope.end,
            col(CCloudCostSourceTable.evidence_scope_end) > scope.start,
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
        dated_overlap = (
            col(CCloudCostSourceTable.malformed) == False,  # noqa: E712
            col(CCloudCostSourceTable.source_period_start).is_not(None),
            col(CCloudCostSourceTable.source_period_end).is_not(None),
            col(CCloudCostSourceTable.source_period_start) < scope.end,
            col(CCloudCostSourceTable.source_period_end) > scope.start,
        )
        fallback_overlap = (
            or_(
                col(CCloudCostSourceTable.malformed) == True,  # noqa: E712
                col(CCloudCostSourceTable.source_period_start).is_(None),
                col(CCloudCostSourceTable.source_period_end).is_(None),
            ),
            col(CCloudCostSourceTable.evidence_scope_start) < scope.end,
            col(CCloudCostSourceTable.evidence_scope_end) > scope.start,
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
        statement = (
            select(CCloudBillingTable)
            .where(
                col(CCloudBillingTable.ecosystem) == scope.ecosystem,
                col(CCloudBillingTable.tenant_id) == scope.tenant_id,
                col(CCloudBillingTable.timestamp) >= scope.start,
                col(CCloudBillingTable.timestamp) < scope.end,
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
                col(CCloudBillingTable.timestamp) == source.allocation_timestamp,
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
        start, end = _date_to_range(tracking_date)
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
        source_stmt = delete(CCloudCostSourceTable).where(
            col(CCloudCostSourceTable.ecosystem) == ecosystem,
            col(CCloudCostSourceTable.tenant_id) == tenant_id,
            col(CCloudCostSourceTable.retention_timestamp) < before,
        )
        self._session.execute(source_stmt)
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
            where.append(col(CCloudBillingTable.timestamp) >= start)
        if end is not None:
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
                col(ChargebackFactTable.timestamp) == source.allocation_timestamp,
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
