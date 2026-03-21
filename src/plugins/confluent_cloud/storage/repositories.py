from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import delete, func
from sqlmodel import Session, col, select

from core.storage.backends.sqlmodel.mappers import chargeback_to_dimension
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.tables import CCloudBillingTable

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
        if existing is not None:
            # Accumulate: API can return partial costs for same PK, sum them
            existing.quantity = str(Decimal(existing.quantity) + Decimal(table_obj.quantity))
            existing.total_cost = str(Decimal(existing.total_cost) + Decimal(table_obj.total_cost))
            self._session.add(existing)
            self._session.flush()
            return _table_to_line(existing)

        # New row - insert as-is
        merged = self._session.merge(table_obj)
        self._session.flush()
        return _table_to_line(merged)

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

    def increment_allocation_attempts(self, line: BillingLineItem) -> int:
        ccloud_line = cast("CCloudBillingLineItem", line)
        row = self._session.get(CCloudBillingTable, _billing_pk(ccloud_line))
        if row is None:
            msg = (
                f"Billing line not found: ecosystem={line.ecosystem!r}, tenant_id={line.tenant_id!r}, "
                f"timestamp={line.timestamp!r}, resource_id={line.resource_id!r}, "
                f"product_type={line.product_type!r}, product_category={line.product_category!r}"
            )
            raise KeyError(msg)
        row.allocation_attempts += 1
        self._session.add(row)
        self._session.flush()
        return row.allocation_attempts

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
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
