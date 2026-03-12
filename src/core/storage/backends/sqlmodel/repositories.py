from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache
from sqlalchemy import case, cast, delete, func, or_, update
from sqlalchemy.types import String
from sqlmodel import Session, col, select

from core.models.chargeback import AggregationRow, CostType

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem, CoreBillingLineItem
    from core.models.chargeback import ChargebackDimensionInfo, ChargebackRow, CustomTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineRun, PipelineState
    from core.models.resource import Resource

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.mappers import (
    billing_to_domain,
    billing_to_table,
    chargeback_to_dimension,
    chargeback_to_domain,
    chargeback_to_fact,
    identity_to_domain,
    identity_to_table,
    pipeline_run_to_domain,
    pipeline_run_to_table,
    pipeline_state_to_domain,
    pipeline_state_to_table,
    resource_to_domain,
    resource_to_table,
    tag_to_domain,
    tag_to_table,
)
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
    CustomTagTable,
    PipelineRunTable,
    PipelineStateTable,
)

logger = logging.getLogger(__name__)


def _date_to_range(d: date) -> tuple[datetime, datetime]:
    """Convert a date to a half-open datetime range [start_of_day, start_of_next_day) in UTC."""
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


# --- Temporal helpers ---


def _temporal_active_at_filter(
    table: type[ResourceTable] | type[IdentityTable],
    ecosystem: str,
    tenant_id: str,
    timestamp: datetime,
) -> list[Any]:  # list of SQLAlchemy BinaryExpression column elements
    """Build WHERE clauses for point-in-time active query."""
    return [
        col(table.ecosystem) == ecosystem,
        col(table.tenant_id) == tenant_id,
        or_(col(table.created_at).is_(None), col(table.created_at) <= timestamp),
        or_(col(table.deleted_at).is_(None), col(table.deleted_at) > timestamp),
    ]


def _temporal_by_period_filter(
    table: type[ResourceTable] | type[IdentityTable],
    ecosystem: str,
    tenant_id: str,
    start: datetime,
    end: datetime,
) -> list[Any]:  # list of SQLAlchemy BinaryExpression column elements
    """Build WHERE clauses for half-open interval [start, end)."""
    return [
        col(table.ecosystem) == ecosystem,
        col(table.tenant_id) == tenant_id,
        or_(col(table.created_at).is_(None), col(table.created_at) < end),
        or_(col(table.deleted_at).is_(None), col(table.deleted_at) >= start),
    ]


# --- ResourceRepository ---


class SQLModelResourceRepository:
    def __init__(
        self,
        session: Session,
        *,
        cache_maxsize: int = 1000,
        cache_ttl_seconds: float = 300.0,
    ) -> None:
        self._session = session
        self._resource_cache: TTLCache[tuple[str, str, str], Resource | None] = TTLCache(
            maxsize=cache_maxsize, ttl=cache_ttl_seconds
        )

    def upsert(self, resource: Resource) -> Resource:
        table_obj = resource_to_table(resource)
        merged = self._session.merge(table_obj)
        result = resource_to_domain(merged)
        self._resource_cache.pop((result.ecosystem, result.tenant_id, result.resource_id), None)
        return result

    def get(self, ecosystem: str, tenant_id: str, resource_id: str) -> Resource | None:
        key = (ecosystem, tenant_id, resource_id)
        if key in self._resource_cache:
            return self._resource_cache[key]
        row = self._session.get(ResourceTable, (ecosystem, tenant_id, resource_id))
        result = resource_to_domain(row) if row else None
        self._resource_cache[key] = result
        return result

    def find_active_at(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        *,
        resource_type: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, timestamp)
        if resource_type is not None:
            where.append(col(ResourceTable.resource_type) == resource_type)
        if status is not None:
            where.append(col(ResourceTable.status) == status)

        total: int = 0
        if count:
            count_stmt = select(func.count()).select_from(ResourceTable).where(*where)
            total = self._session.exec(count_stmt).one()

        stmt = select(ResourceTable).where(*where).order_by(col(ResourceTable.resource_id))
        if offset:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        return [resource_to_domain(r) for r in self._session.exec(stmt).all()], total

    def find_by_period(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        resource_type: str | None = None,
        status: str | None = None,
        metadata_filter: dict[str, str | int | float | bool | None] | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        where = _temporal_by_period_filter(ResourceTable, ecosystem, tenant_id, start, end)
        if resource_type is not None:
            where.append(col(ResourceTable.resource_type) == resource_type)
        if status is not None:
            where.append(col(ResourceTable.status) == status)
        if metadata_filter is not None:
            for key, value in metadata_filter.items():
                where.append(func.json_extract(ResourceTable.metadata_json, f"$.{key}") == value)

        total: int = 0
        if count:
            count_stmt = select(func.count()).select_from(ResourceTable).where(*where)
            total = self._session.exec(count_stmt).one()

        stmt = select(ResourceTable).where(*where).order_by(col(ResourceTable.resource_id))
        if offset:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        return [resource_to_domain(r) for r in self._session.exec(stmt).all()], total

    def find_by_type(self, ecosystem: str, tenant_id: str, resource_type: str) -> list[Resource]:
        stmt = select(ResourceTable).where(
            col(ResourceTable.ecosystem) == ecosystem,
            col(ResourceTable.tenant_id) == tenant_id,
            col(ResourceTable.resource_type) == resource_type,
        )
        return [resource_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_paginated(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        offset: int,
        resource_type: str | None = None,
        status: str | None = None,
    ) -> tuple[list[Resource], int]:
        where = [col(ResourceTable.ecosystem) == ecosystem, col(ResourceTable.tenant_id) == tenant_id]
        if resource_type is not None:
            where.append(col(ResourceTable.resource_type) == resource_type)
        if status is not None:
            where.append(col(ResourceTable.status) == status)

        count_stmt = select(func.count()).select_from(ResourceTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        stmt = select(ResourceTable).where(*where).offset(offset).limit(limit)
        items = [resource_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None:
        self._resource_cache.pop((ecosystem, tenant_id, resource_id), None)
        row = self._session.get(ResourceTable, (ecosystem, tenant_id, resource_id))
        if row:
            row.deleted_at = deleted_at
            row.status = "deleted"
            self._session.add(row)
            self._session.flush()

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        stmt = delete(ResourceTable).where(
            col(ResourceTable.ecosystem) == ecosystem,
            col(ResourceTable.tenant_id) == tenant_id,
            col(ResourceTable.deleted_at).is_not(None),
            col(ResourceTable.deleted_at) < before,
        )
        result = self._session.execute(stmt)
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount


# --- IdentityRepository ---


class SQLModelIdentityRepository:
    def __init__(
        self,
        session: Session,
        *,
        cache_maxsize: int = 1000,
        cache_ttl_seconds: float = 300.0,
    ) -> None:
        self._session = session
        self._identity_cache: TTLCache[tuple[str, str, str], Identity | None] = TTLCache(
            maxsize=cache_maxsize, ttl=cache_ttl_seconds
        )

    def upsert(self, identity: Identity) -> Identity:
        table_obj = identity_to_table(identity)
        merged = self._session.merge(table_obj)
        result = identity_to_domain(merged)
        self._identity_cache.pop((result.ecosystem, result.tenant_id, result.identity_id), None)
        return result

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None:
        key = (ecosystem, tenant_id, identity_id)
        if key in self._identity_cache:
            return self._identity_cache[key]
        row = self._session.get(IdentityTable, (ecosystem, tenant_id, identity_id))
        result = identity_to_domain(row) if row else None
        self._identity_cache[key] = result
        return result

    def find_active_at(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        *,
        identity_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Identity], int]:
        where = _temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, timestamp)
        if identity_type is not None:
            where.append(col(IdentityTable.identity_type) == identity_type)

        total: int = 0
        if count:
            count_stmt = select(func.count()).select_from(IdentityTable).where(*where)
            total = self._session.exec(count_stmt).one()

        stmt = select(IdentityTable).where(*where).order_by(col(IdentityTable.identity_id))
        if offset:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        return [identity_to_domain(r) for r in self._session.exec(stmt).all()], total

    def find_by_period(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        *,
        identity_type: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Identity], int]:
        where = _temporal_by_period_filter(IdentityTable, ecosystem, tenant_id, start, end)
        if identity_type is not None:
            where.append(col(IdentityTable.identity_type) == identity_type)

        total: int = 0
        if count:
            count_stmt = select(func.count()).select_from(IdentityTable).where(*where)
            total = self._session.exec(count_stmt).one()

        stmt = select(IdentityTable).where(*where).order_by(col(IdentityTable.identity_id))
        if offset:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        return [identity_to_domain(r) for r in self._session.exec(stmt).all()], total

    def find_by_type(self, ecosystem: str, tenant_id: str, identity_type: str) -> list[Identity]:
        stmt = select(IdentityTable).where(
            col(IdentityTable.ecosystem) == ecosystem,
            col(IdentityTable.tenant_id) == tenant_id,
            col(IdentityTable.identity_type) == identity_type,
        )
        return [identity_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_paginated(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        offset: int,
        identity_type: str | None = None,
    ) -> tuple[list[Identity], int]:
        where = [col(IdentityTable.ecosystem) == ecosystem, col(IdentityTable.tenant_id) == tenant_id]
        if identity_type is not None:
            where.append(col(IdentityTable.identity_type) == identity_type)

        count_stmt = select(func.count()).select_from(IdentityTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        stmt = select(IdentityTable).where(*where).offset(offset).limit(limit)
        items = [identity_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None:
        self._identity_cache.pop((ecosystem, tenant_id, identity_id), None)
        row = self._session.get(IdentityTable, (ecosystem, tenant_id, identity_id))
        if row:
            row.deleted_at = deleted_at
            self._session.add(row)
            self._session.flush()

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        stmt = delete(IdentityTable).where(
            col(IdentityTable.ecosystem) == ecosystem,
            col(IdentityTable.tenant_id) == tenant_id,
            col(IdentityTable.deleted_at).is_not(None),
            col(IdentityTable.deleted_at) < before,
        )
        result = self._session.execute(stmt)
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount


# --- BillingRepository ---


def _billing_pk(line: BillingLineItem) -> tuple[str, str, datetime, str, str, str]:
    """Extract billing table primary key tuple from domain object."""
    return (line.ecosystem, line.tenant_id, line.timestamp, line.resource_id, line.product_type, line.product_category)


class SQLModelBillingRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, line: BillingLineItem) -> CoreBillingLineItem:
        table_obj = billing_to_table(line)  # type: ignore[arg-type]  # CoreBillingLineItem satisfies BillingLineItem

        # Check for existing record
        existing = self._session.get(BillingTable, _billing_pk(line))

        if existing is not None and existing.total_cost != table_obj.total_cost:
            # Detect and log billing revisions
            logger.warning(
                "Billing revision detected: %s/%s/%s cost changed %s → %s",
                table_obj.resource_id,
                table_obj.product_type,
                table_obj.timestamp.date(),
                existing.total_cost,
                table_obj.total_cost,
            )

        merged = self._session.merge(table_obj)
        return billing_to_domain(merged)

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[BillingLineItem]:
        start, end = _date_to_range(target_date)
        stmt = select(BillingTable).where(
            col(BillingTable.ecosystem) == ecosystem,
            col(BillingTable.tenant_id) == tenant_id,
            col(BillingTable.timestamp) >= start,
            col(BillingTable.timestamp) < end,
        )
        return [billing_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_range(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[BillingLineItem]:
        stmt = select(BillingTable).where(
            col(BillingTable.ecosystem) == ecosystem,
            col(BillingTable.tenant_id) == tenant_id,
            col(BillingTable.timestamp) >= start,
            col(BillingTable.timestamp) < end,
        )
        return [billing_to_domain(r) for r in self._session.exec(stmt).all()]

    def increment_allocation_attempts(self, line: BillingLineItem) -> int:
        row = self._session.get(BillingTable, _billing_pk(line))
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
    ) -> tuple[list[BillingLineItem], int]:
        where: list[Any] = [col(BillingTable.ecosystem) == ecosystem, col(BillingTable.tenant_id) == tenant_id]
        if start is not None:
            where.append(col(BillingTable.timestamp) >= start)
        if end is not None:
            where.append(col(BillingTable.timestamp) < end)
        if product_type is not None:
            where.append(col(BillingTable.product_type) == product_type)
        if resource_id is not None:
            where.append(col(BillingTable.resource_id) == resource_id)

        count_stmt = select(func.count()).select_from(BillingTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        stmt = select(BillingTable).where(*where).offset(offset).limit(limit)
        items = [billing_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        stmt = delete(BillingTable).where(
            col(BillingTable.ecosystem) == ecosystem,
            col(BillingTable.tenant_id) == tenant_id,
            col(BillingTable.timestamp) < before,
        )
        result = self._session.execute(stmt)
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount


# --- ChargebackRepository ---


class SQLModelChargebackRepository:
    def __init__(self, session: Session) -> None:
        self._session = session
        self._dimension_cache: dict[tuple[str | None, ...], ChargebackDimensionTable] = {}

    def _make_dimension_key(self, row: ChargebackRow) -> tuple[str | None, ...]:
        return (
            row.ecosystem,
            row.tenant_id,
            row.resource_id,
            row.product_category,
            row.product_type,
            row.identity_id,
            row.cost_type.value,
            row.allocation_method,
            row.allocation_detail,
        )

    def _get_or_create_dimension(self, row: ChargebackRow) -> ChargebackDimensionTable:
        """Get existing dimension by UQ columns (cached), or create a new one."""
        key = self._make_dimension_key(row)

        cached = self._dimension_cache.get(key)
        if cached is not None:
            return cached

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
        )
        existing = self._session.exec(stmt).first()
        if existing:
            assert existing.dimension_id is not None
            self._dimension_cache[key] = existing
            return existing

        dim = chargeback_to_dimension(row)
        self._session.add(dim)
        self._session.flush()
        assert dim.dimension_id is not None  # auto-incremented PK needed as FK
        self._dimension_cache[key] = dim
        return dim

    def upsert(self, row: ChargebackRow) -> ChargebackRow:
        dim = self._get_or_create_dimension(row)
        assert dim.dimension_id is not None
        fact = chargeback_to_fact(row, dim.dimension_id)
        merged = self._session.merge(fact)
        return chargeback_to_domain(dim, merged)

    def upsert_batch(self, rows: list[ChargebackRow]) -> int:
        """Batch-insert all fact rows using session.add_all()."""
        unique_facts: dict[tuple[datetime, int], ChargebackFactTable] = {}
        for row in rows:
            dim = self._get_or_create_dimension(row)
            assert dim.dimension_id is not None
            fact = chargeback_to_fact(row, dim.dimension_id)
            unique_facts[(fact.timestamp, fact.dimension_id)] = fact
        if unique_facts:
            self._session.add_all(unique_facts.values())
        return len(unique_facts)

    def _query_joined(self, *where_clauses: Any) -> list[ChargebackRow]:
        """Execute a joined query on dimensions+facts. where_clauses are SQLAlchemy column elements."""
        stmt = (
            select(ChargebackDimensionTable, ChargebackFactTable)
            .join(
                ChargebackFactTable,
                col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id),
            )
            .where(*where_clauses)
        )
        results = self._session.execute(stmt).all()
        return [chargeback_to_domain(dim, fact) for dim, fact in results]

    def find_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> list[ChargebackRow]:
        start, end = _date_to_range(target_date)
        return self._query_joined(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackFactTable.timestamp) >= start,
            col(ChargebackFactTable.timestamp) < end,
        )

    def find_by_range(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[ChargebackRow]:
        return self._query_joined(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackFactTable.timestamp) >= start,
            col(ChargebackFactTable.timestamp) < end,
        )

    def find_by_identity(self, ecosystem: str, tenant_id: str, identity_id: str) -> list[ChargebackRow]:
        return self._query_joined(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackDimensionTable.identity_id) == identity_id,
        )

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> int:
        start, end = _date_to_range(target_date)

        dim_subquery = (
            select(ChargebackDimensionTable.dimension_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
            )
            .scalar_subquery()
        )

        fact_del = delete(ChargebackFactTable).where(
            col(ChargebackFactTable.dimension_id).in_(dim_subquery),
            col(ChargebackFactTable.timestamp) >= start,
            col(ChargebackFactTable.timestamp) < end,
        )
        result = self._session.execute(fact_del)
        self._session.flush()
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount

    def _build_chargeback_where(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None,
        end: datetime | None,
        identity_id: str | None,
        product_type: str | None,
        resource_id: str | None,
        cost_type: str | None,
    ) -> tuple[list[Any], Any]:
        """Return (where_clauses, join_clause) for chargeback dimension+fact queries."""
        where: list[Any] = [
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        ]
        if start is not None:
            where.append(col(ChargebackFactTable.timestamp) >= start)
        if end is not None:
            where.append(col(ChargebackFactTable.timestamp) < end)
        if identity_id is not None:
            where.append(col(ChargebackDimensionTable.identity_id) == identity_id)
        if product_type is not None:
            where.append(col(ChargebackDimensionTable.product_type) == product_type)
        if resource_id is not None:
            where.append(col(ChargebackDimensionTable.resource_id) == resource_id)
        if cost_type is not None:
            where.append(col(ChargebackDimensionTable.cost_type) == cost_type)
        join_clause = col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id)
        return where, join_clause

    def _overlay_tags(self, rows: list[ChargebackRow]) -> None:
        """Fetch custom tags for rows and mutate row.tags in-place."""
        dim_ids = list({row.dimension_id for row in rows if row.dimension_id is not None})
        if not dim_ids:
            return
        tag_rows = self._session.exec(select(CustomTagTable).where(col(CustomTagTable.dimension_id).in_(dim_ids))).all()
        tags_by_dim: dict[int, list[str]] = {}
        for t in tag_rows:
            tags_by_dim.setdefault(t.dimension_id, []).append(t.display_name)
        for row in rows:
            if row.dimension_id is not None:
                row.tags = tags_by_dim.get(row.dimension_id, [])

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[ChargebackRow], int]:
        where, join_clause = self._build_chargeback_where(
            ecosystem, tenant_id, start, end, identity_id, product_type, resource_id, cost_type
        )

        count_stmt = (
            select(func.count())
            .select_from(ChargebackDimensionTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
        )
        total: int = self._session.exec(count_stmt).one()

        stmt = (
            select(ChargebackDimensionTable, ChargebackFactTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .offset(offset)
            .limit(limit)
        )
        results = self._session.execute(stmt).all()
        items = [chargeback_to_domain(dim, fact) for dim, fact in results]
        self._overlay_tags(items)
        return items, total

    def iter_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        batch_size: int = 5000,
    ) -> Iterator[ChargebackRow]:
        """Yield ChargebackRow objects in batches. Memory bounded to batch_size rows."""
        where, join_clause = self._build_chargeback_where(
            ecosystem, tenant_id, start, end, identity_id, product_type, resource_id, cost_type
        )
        stmt = (
            select(ChargebackDimensionTable, ChargebackFactTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .execution_options(yield_per=batch_size)
        )
        for partition in self._session.execute(stmt).partitions(batch_size):
            batch = [chargeback_to_domain(dim, fact) for dim, fact in partition]
            self._overlay_tags(batch)
            yield from batch

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        # Get dimension IDs for this ecosystem+tenant
        dim_stmt = select(ChargebackDimensionTable.dimension_id).where(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        )
        dim_ids = list(self._session.exec(dim_stmt).all())
        if not dim_ids:
            return 0

        # Delete facts before cutoff for those dimensions
        fact_del = delete(ChargebackFactTable).where(
            col(ChargebackFactTable.dimension_id).in_(dim_ids),
            col(ChargebackFactTable.timestamp) < before,
        )
        result = self._session.execute(fact_del)
        deleted_count: int = result.rowcount  # type: ignore[attr-defined]  # CursorResult always has rowcount

        # Clean up orphaned dimensions (no remaining facts)
        orphan_del = delete(ChargebackDimensionTable).where(
            col(ChargebackDimensionTable.dimension_id).in_(dim_ids),
            ~col(ChargebackDimensionTable.dimension_id).in_(select(ChargebackFactTable.dimension_id).distinct()),
        )
        self._session.execute(orphan_del)
        self._session.flush()
        return deleted_count

    def get_dimension(self, dimension_id: int) -> ChargebackDimensionInfo | None:
        from core.models.chargeback import ChargebackDimensionInfo

        row = self._session.get(ChargebackDimensionTable, dimension_id)
        if row is None:
            return None
        return ChargebackDimensionInfo(
            dimension_id=row.dimension_id,  # type: ignore[arg-type]  # PK always set
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            resource_id=row.resource_id,
            product_category=row.product_category,
            product_type=row.product_type,
            identity_id=row.identity_id,
            cost_type=row.cost_type,
            allocation_method=row.allocation_method,
            allocation_detail=row.allocation_detail,
        )

    def get_dimensions_batch(self, dimension_ids: list[int]) -> dict[int, ChargebackDimensionInfo]:
        from core.models.chargeback import ChargebackDimensionInfo

        if not dimension_ids:
            return {}
        stmt = select(ChargebackDimensionTable).where(col(ChargebackDimensionTable.dimension_id).in_(dimension_ids))
        result: dict[int, ChargebackDimensionInfo] = {}
        for row in self._session.exec(stmt).all():
            if row.dimension_id is not None:
                result[row.dimension_id] = ChargebackDimensionInfo(
                    dimension_id=row.dimension_id,
                    ecosystem=row.ecosystem,
                    tenant_id=row.tenant_id,
                    resource_id=row.resource_id,
                    product_category=row.product_category,
                    product_type=row.product_type,
                    identity_id=row.identity_id,
                    cost_type=row.cost_type,
                    allocation_method=row.allocation_method,
                    allocation_detail=row.allocation_detail,
                )
        return result

    def find_dimension_ids_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime,
        end: datetime,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
    ) -> list[int]:
        where, join_clause = self._build_chargeback_where(
            ecosystem, tenant_id, start, end, identity_id, product_type, resource_id, cost_type
        )
        stmt = (
            select(ChargebackDimensionTable.dimension_id)
            .select_from(ChargebackDimensionTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .distinct()
        )
        return [row for row in self._session.exec(stmt).all() if row is not None]

    def aggregate(
        self,
        ecosystem: str,
        tenant_id: str,
        group_by: list[str],
        time_bucket: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 10000,
    ) -> list[AggregationRow]:
        # Build dimension group columns
        group_cols = []
        group_labels = []
        for gb in group_by:
            col_ref = getattr(ChargebackDimensionTable, gb)
            label = f"dim_{gb}"
            group_cols.append(cast(col(col_ref), String).label(label))
            group_labels.append(label)

        # SQLite strftime-based time bucketing
        bucket_formats: dict[str, str] = {
            "hour": "%Y-%m-%dT%H:00:00",
            "day": "%Y-%m-%d",
            "week": "%Y-W%W",
            "month": "%Y-%m",
        }
        fmt = bucket_formats[time_bucket]
        time_expr = func.strftime(fmt, ChargebackFactTable.timestamp)

        join_clause = col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id)

        where: list[Any] = [
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        ]
        if start is not None:
            where.append(col(ChargebackFactTable.timestamp) >= start)
        if end is not None:
            where.append(col(ChargebackFactTable.timestamp) < end)
        if identity_id is not None:
            where.append(col(ChargebackDimensionTable.identity_id) == identity_id)
        if product_type is not None:
            where.append(col(ChargebackDimensionTable.product_type) == product_type)
        if resource_id is not None:
            where.append(col(ChargebackDimensionTable.resource_id) == resource_id)
        if cost_type is not None:
            where.append(col(ChargebackDimensionTable.cost_type) == cost_type)

        usage_expr = func.sum(
            case(
                (col(ChargebackDimensionTable.cost_type) == CostType.USAGE, col(ChargebackFactTable.amount)),
                else_=0,
            )
        ).label("usage_amount")

        shared_expr = func.sum(
            case(
                (col(ChargebackDimensionTable.cost_type) == CostType.SHARED, col(ChargebackFactTable.amount)),
                else_=0,
            )
        ).label("shared_amount")

        select_cols = [
            *group_cols,
            time_expr.label("time_bucket"),
            func.sum(col(ChargebackFactTable.amount)).label("total_amount"),
            usage_expr,
            shared_expr,
            func.count().label("row_count"),
        ]
        group_by_labels = [*group_labels, "time_bucket"]

        stmt = (
            select(*select_cols)
            .select_from(ChargebackDimensionTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .group_by(*group_by_labels)
            .order_by("time_bucket", *group_labels)
            .limit(limit)
        )

        results = self._session.execute(stmt).all()
        return [
            AggregationRow(
                dimensions={gb: str(getattr(r, f"dim_{gb}", "") or "") for gb in group_by},
                time_bucket=str(r.time_bucket),
                total_amount=Decimal(str(r.total_amount or 0)),
                usage_amount=Decimal(str(r.usage_amount or 0)),
                shared_amount=Decimal(str(r.shared_amount or 0)),
                row_count=int(r.row_count),
            )
            for r in results
        ]


# --- PipelineStateRepository ---


class SQLModelPipelineStateRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, state: PipelineState) -> PipelineState:
        table_obj = pipeline_state_to_table(state)
        merged = self._session.merge(table_obj)
        return pipeline_state_to_domain(merged)

    def get(self, ecosystem: str, tenant_id: str, tracking_date: date) -> PipelineState | None:
        row = self._session.get(PipelineStateTable, (ecosystem, tenant_id, tracking_date))
        return pipeline_state_to_domain(row) if row else None

    def find_needing_calculation(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        stmt = (
            select(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.billing_gathered) == True,  # noqa: E712
                col(PipelineStateTable.resources_gathered) == True,  # noqa: E712
                col(PipelineStateTable.chargeback_calculated) == False,  # noqa: E712
            )
            .order_by(col(PipelineStateTable.tracking_date).asc())
        )
        return [pipeline_state_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_range(self, ecosystem: str, tenant_id: str, start: date, end: date) -> list[PipelineState]:
        stmt = select(PipelineStateTable).where(
            col(PipelineStateTable.ecosystem) == ecosystem,
            col(PipelineStateTable.tenant_id) == tenant_id,
            col(PipelineStateTable.tracking_date) >= start,
            col(PipelineStateTable.tracking_date) < end,
        )
        return [pipeline_state_to_domain(r) for r in self._session.exec(stmt).all()]

    def mark_billing_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(billing_gathered=True)
        )
        self._session.execute(stmt)

    def mark_resources_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(resources_gathered=True)
        )
        self._session.execute(stmt)

    def mark_needs_recalculation(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(chargeback_calculated=False)
        )
        self._session.execute(stmt)

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(chargeback_calculated=True)
        )
        self._session.execute(stmt)

    def count_pending(self, ecosystem: str, tenant_id: str) -> int:
        stmt = (
            select(func.count())
            .select_from(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.billing_gathered) == True,  # noqa: E712
                col(PipelineStateTable.resources_gathered) == True,  # noqa: E712
                col(PipelineStateTable.chargeback_calculated) == False,  # noqa: E712
            )
        )
        return self._session.exec(stmt).one()

    def count_calculated(self, ecosystem: str, tenant_id: str) -> int:
        stmt = (
            select(func.count())
            .select_from(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.chargeback_calculated) == True,  # noqa: E712
            )
        )
        return self._session.exec(stmt).one()

    def get_last_calculated_date(self, ecosystem: str, tenant_id: str) -> date | None:
        stmt = select(func.max(PipelineStateTable.tracking_date)).where(
            col(PipelineStateTable.ecosystem) == ecosystem,
            col(PipelineStateTable.tenant_id) == tenant_id,
            col(PipelineStateTable.chargeback_calculated) == True,  # noqa: E712
        )
        return self._session.exec(stmt).one()


# --- PipelineRunRepository ---


class SQLModelPipelineRunRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def create_run(self, tenant_name: str, started_at: datetime) -> PipelineRun:
        from core.models.pipeline import PipelineRun

        run = PipelineRun(tenant_name=tenant_name, started_at=started_at, status="running")
        table_obj = pipeline_run_to_table(run)
        self._session.add(table_obj)
        self._session.flush()
        return pipeline_run_to_domain(table_obj)

    def update_run(self, run: PipelineRun) -> PipelineRun:
        table_obj = pipeline_run_to_table(run)
        merged = self._session.merge(table_obj)
        self._session.flush()
        return pipeline_run_to_domain(merged)

    def get_run(self, run_id: int) -> PipelineRun | None:
        row = self._session.get(PipelineRunTable, run_id)
        return pipeline_run_to_domain(row) if row else None

    def list_runs_for_tenant(self, tenant_name: str, limit: int = 100) -> list[PipelineRun]:
        stmt = (
            select(PipelineRunTable)
            .where(col(PipelineRunTable.tenant_name) == tenant_name)
            .order_by(col(PipelineRunTable.started_at).desc())
            .limit(limit)
        )
        return [pipeline_run_to_domain(r) for r in self._session.exec(stmt).all()]

    def get_latest_run(self, tenant_name: str) -> PipelineRun | None:
        stmt = (
            select(PipelineRunTable)
            .where(col(PipelineRunTable.tenant_name) == tenant_name)
            .order_by(col(PipelineRunTable.started_at).desc())
            .limit(1)
        )
        row = self._session.exec(stmt).first()
        return pipeline_run_to_domain(row) if row else None


# --- TagRepository ---


class SQLModelTagRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_tag(self, dimension_id: int, tag_key: str, display_name: str, created_by: str) -> CustomTag:
        """Create tag. Backend auto-generates tag_value = uuid4()."""
        from core.models.chargeback import CustomTag as CustomTagDomain

        domain_tag = CustomTagDomain(
            tag_id=None,
            dimension_id=dimension_id,
            tag_key=tag_key,
            tag_value=str(uuid.uuid4()),
            display_name=display_name,
            created_by=created_by,
        )
        row = tag_to_table(domain_tag)
        self._session.add(row)
        self._session.flush()
        return tag_to_domain(row)

    def get_tag(self, tag_id: int) -> CustomTag | None:
        row = self._session.get(CustomTagTable, tag_id)
        return tag_to_domain(row) if row else None

    def get_tags(self, dimension_id: int) -> list[CustomTag]:
        stmt = select(CustomTagTable).where(col(CustomTagTable.dimension_id) == dimension_id)
        return [tag_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_tags_for_tenant(
        self,
        ecosystem: str,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
        search: str | None = None,
    ) -> tuple[list[CustomTag], int]:
        join_clause = col(CustomTagTable.dimension_id) == col(ChargebackDimensionTable.dimension_id)
        where: list[Any] = [
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        ]
        if search:
            pattern = f"%{search}%"
            where.append(
                or_(
                    col(CustomTagTable.tag_key).ilike(pattern),
                    col(CustomTagTable.tag_value).ilike(pattern),
                    col(CustomTagTable.display_name).ilike(pattern),
                )
            )

        count_stmt = (
            select(func.count()).select_from(CustomTagTable).join(ChargebackDimensionTable, join_clause).where(*where)
        )
        total: int = self._session.exec(count_stmt).one()

        stmt = (
            select(CustomTagTable).join(ChargebackDimensionTable, join_clause).where(*where).offset(offset).limit(limit)
        )
        items = [tag_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total

    def update_display_name(self, tag_id: int, display_name: str) -> CustomTag:
        """Update display_name only. tag_value remains immutable."""
        row = self._session.get(CustomTagTable, tag_id)
        if row is None:
            msg = f"Tag {tag_id} not found"
            raise KeyError(msg)
        row.display_name = display_name
        self._session.add(row)
        self._session.flush()
        return tag_to_domain(row)

    def find_by_dimension_and_key(self, dimension_id: int, tag_key: str) -> CustomTag | None:
        """Find existing tag by dimension and key."""
        stmt = select(CustomTagTable).where(
            col(CustomTagTable.dimension_id) == dimension_id,
            col(CustomTagTable.tag_key) == tag_key,
        )
        row = self._session.exec(stmt).first()
        return tag_to_domain(row) if row else None

    def delete_tag(self, tag_id: int) -> None:
        row = self._session.get(CustomTagTable, tag_id)
        if row:
            self._session.delete(row)
            self._session.flush()
