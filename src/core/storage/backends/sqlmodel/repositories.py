from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cachetools import TTLCache
from sqlalchemy import case, cast, delete, func, literal, or_, update
from sqlalchemy.types import String
from sqlmodel import Session, col, select

from core.models.chargeback import AggregationRow, AllocationDetail, AllocationIssueRow, CostType
from core.models.counts import TypeStatusCounts

if TYPE_CHECKING:
    from core.emitters.models import EmissionRecord
    from core.models.billing import BillingLineItem, CoreBillingLineItem
    from core.models.chargeback import ChargebackDimensionInfo, ChargebackRow
    from core.models.entity_tag import EntityTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineRun, PipelineState
    from core.models.resource import Resource
    from core.storage.interface import EntityTagRepository

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.mappers import (
    billing_to_domain,
    billing_to_table,
    chargeback_to_dimension,
    chargeback_to_domain,
    chargeback_to_fact,
    emission_record_to_table,
    entity_tag_to_domain,
    identity_to_domain,
    identity_to_table,
    pipeline_run_to_domain,
    pipeline_run_to_table,
    pipeline_state_to_domain,
    pipeline_state_to_table,
    resource_to_domain,
    resource_to_table,
)
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
    EmissionRecordTable,
    EntityTagTable,
    PipelineRunTable,
    PipelineStateTable,
)
from core.storage.backends.sqlmodel.tag_joins import TagJoinSpec, build_tag_join_specs

logger = logging.getLogger(__name__)

# Maximum values per .in_() clause. SQLite hard limit is 32,767; 500 is a safe margin.
# See also: _BULK_CHUNK_SIZE in tags.py — same rationale, separate constant for API layer.
_CHUNK_SIZE = 500

# Allowlists for sort_by parameter — prevents arbitrary column injection.
# Fallback to primary-key column when sort_by is absent or invalid.
_IDENTITY_SORT_COLS: dict[str, Any] = {
    "identity_id": IdentityTable.identity_id,
    "display_name": IdentityTable.display_name,
    "identity_type": IdentityTable.identity_type,
}

_RESOURCE_SORT_COLS: dict[str, Any] = {
    "resource_id": ResourceTable.resource_id,
    "display_name": ResourceTable.display_name,
    "resource_type": ResourceTable.resource_type,
    "status": ResourceTable.status,
}


def _overlay_tags(
    rows: list[ChargebackRow],
    tags_repo: EntityTagRepository,
) -> None:
    """Batch-fetch entity tags and merge into row.tags. Resource tags override identity tags."""
    if not rows:
        return
    tenant_id = rows[0].tenant_id
    resource_ids = list({row.resource_id for row in rows if row.resource_id is not None})
    identity_ids = list({row.identity_id for row in rows if row.identity_id})

    resource_tags_map = tags_repo.find_tags_for_entities(tenant_id, "resource", resource_ids)
    identity_tags_map = tags_repo.find_tags_for_entities(tenant_id, "identity", identity_ids)

    for row in rows:
        merged: dict[str, str] = {t.tag_key: t.tag_value for t in identity_tags_map.get(row.identity_id, [])}
        if row.resource_id is not None:
            merged.update({t.tag_key: t.tag_value for t in resource_tags_map.get(row.resource_id, [])})
        row.tags = merged


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


def _apply_resource_type_filter(where: list[Any], resource_type: str | Sequence[str]) -> None:
    """Append resource_type WHERE clause.

    str → equality clause.
    Non-empty Sequence → IN clause.
    Empty Sequence → always-false clause (literal(False)) — guarantees zero rows,
    consistent with IN () semantics and avoids an accidental full-table scan.
    """
    if isinstance(resource_type, str):
        where.append(col(ResourceTable.resource_type) == resource_type)
    elif resource_type:  # non-empty sequence → IN clause
        where.append(col(ResourceTable.resource_type).in_(list(resource_type)))
    else:  # empty sequence → always-false clause → zero rows returned
        where.append(literal(False))


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
        table_obj = resource_to_table(resource)  # type: ignore[arg-type]  # domain/table type bridge
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
        resource_type: str | Sequence[str],
        status: str | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, timestamp)
        _apply_resource_type_filter(where, resource_type)
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
        parent_id: str | None = None,
        resource_type: str | Sequence[str],
        status: str | None = None,
        metadata_filter: dict[str, str | int | float | bool | None] | None = None,
        limit: int | None = None,
        offset: int = 0,
        count: bool = True,
    ) -> tuple[list[Resource], int]:
        where = _temporal_by_period_filter(ResourceTable, ecosystem, tenant_id, start, end)
        if parent_id is not None:
            where.append(col(ResourceTable.parent_id) == parent_id)
        _apply_resource_type_filter(where, resource_type)
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

    # Result set bounded by gather cardinality (hundreds per tenant+type, not millions).
    # Not API-exposed; pagination not needed. Review if ever called from bulk/export paths.
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
        *,
        resource_type: str | Sequence[str],
        status: str | None = None,
        search: str | None = None,
        sort_by: str | None = None,
        sort_order: str = "asc",
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[Resource], int]:
        # tags_repo gates tag filtering per Protocol contract — callers must provide it
        # when tag_key is set. The actual subquery uses self._session directly.
        where = [col(ResourceTable.ecosystem) == ecosystem, col(ResourceTable.tenant_id) == tenant_id]
        _apply_resource_type_filter(where, resource_type)
        if status is not None:
            where.append(col(ResourceTable.status) == status)
        if search is not None:
            pattern = f"%{search}%"
            where.append(
                or_(
                    col(ResourceTable.resource_id).ilike(pattern),
                    col(ResourceTable.display_name).ilike(pattern),
                )
            )
        if tag_key is not None and tags_repo is not None:
            tag_where: list[Any] = [
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.tag_key) == tag_key,
                col(EntityTagTable.entity_type) == "resource",
            ]
            if tag_value is not None:
                tag_where.append(col(EntityTagTable.tag_value) == tag_value)
            tag_sub = select(EntityTagTable.entity_id).where(*tag_where).scalar_subquery()
            where.append(col(ResourceTable.resource_id).in_(tag_sub))

        count_stmt = select(func.count()).select_from(ResourceTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        sort_col = _RESOURCE_SORT_COLS.get(sort_by or "", ResourceTable.resource_id)
        order_expr = col(sort_col).desc() if sort_order == "desc" else col(sort_col).asc()
        stmt = select(ResourceTable).where(*where).order_by(order_expr).offset(offset).limit(limit)
        items = [resource_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total  # type: ignore[return-value]  # SQLModel returns table types, protocol expects domain types

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

    def count_by_type(self, ecosystem: str, tenant_id: str) -> dict[str, TypeStatusCounts]:
        stmt = (
            select(ResourceTable.resource_type, ResourceTable.status, func.count())
            .where(
                col(ResourceTable.ecosystem) == ecosystem,
                col(ResourceTable.tenant_id) == tenant_id,
            )
            .group_by(ResourceTable.resource_type, ResourceTable.status)
        )
        return _rows_to_type_status_counts(self._session.exec(stmt).all())

    def find_by_parent(
        self,
        ecosystem: str,
        tenant_id: str,
        parent_id: str,
        *,
        resource_type: str | Sequence[str],
    ) -> list[Resource]:
        where: list[Any] = [
            col(ResourceTable.ecosystem) == ecosystem,
            col(ResourceTable.tenant_id) == tenant_id,
            col(ResourceTable.parent_id) == parent_id,
            col(ResourceTable.deleted_at).is_(None),
        ]
        _apply_resource_type_filter(where, resource_type)
        stmt = select(ResourceTable).where(*where)
        return [resource_to_domain(r) for r in self._session.exec(stmt).all()]


# --- IdentityRepository ---


def _rows_to_type_status_counts(rows: Sequence[tuple[str, str, int]]) -> dict[str, TypeStatusCounts]:
    result: dict[str, dict[str, int]] = {}
    for type_key, status, count in rows:
        result.setdefault(type_key, {"active": 0, "deleted": 0})
        result[type_key][status] = count
    return {
        t: TypeStatusCounts(
            total=counts["active"] + counts["deleted"], active=counts["active"], deleted=counts["deleted"]
        )
        for t, counts in result.items()
    }


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
        table_obj = identity_to_table(identity)  # type: ignore[arg-type]  # domain/table type bridge
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

    # Result set bounded by gather cardinality (hundreds per tenant+type, not millions).
    # Not API-exposed; pagination not needed. Review if ever called from bulk/export paths.
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
        search: str | None = None,
        sort_by: str | None = None,
        sort_order: str = "asc",
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[Identity], int]:
        # tags_repo gates tag filtering per Protocol contract — callers must provide it
        # when tag_key is set. The actual subquery uses self._session directly.
        where = [col(IdentityTable.ecosystem) == ecosystem, col(IdentityTable.tenant_id) == tenant_id]
        if identity_type is not None:
            where.append(col(IdentityTable.identity_type) == identity_type)
        if search is not None:
            pattern = f"%{search}%"
            where.append(
                or_(
                    col(IdentityTable.identity_id).ilike(pattern),
                    col(IdentityTable.display_name).ilike(pattern),
                )
            )
        if tag_key is not None and tags_repo is not None:
            tag_where: list[Any] = [
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.tag_key) == tag_key,
                col(EntityTagTable.entity_type) == "identity",
            ]
            if tag_value is not None:
                tag_where.append(col(EntityTagTable.tag_value) == tag_value)
            tag_sub = select(EntityTagTable.entity_id).where(*tag_where).scalar_subquery()
            where.append(col(IdentityTable.identity_id).in_(tag_sub))

        count_stmt = select(func.count()).select_from(IdentityTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        sort_col = _IDENTITY_SORT_COLS.get(sort_by or "", IdentityTable.identity_id)
        order_expr = col(sort_col).desc() if sort_order == "desc" else col(sort_col).asc()
        stmt = select(IdentityTable).where(*where).order_by(order_expr).offset(offset).limit(limit)
        items = [identity_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total  # type: ignore[return-value]  # SQLModel returns table types, protocol expects domain types

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

    def count_by_type(self, ecosystem: str, tenant_id: str) -> dict[str, TypeStatusCounts]:
        derived_status = case(
            (col(IdentityTable.deleted_at).is_(None), "active"),
            else_="deleted",
        ).label("derived_status")
        stmt = (
            select(IdentityTable.identity_type, derived_status, func.count())
            .where(
                col(IdentityTable.ecosystem) == ecosystem,
                col(IdentityTable.tenant_id) == tenant_id,
            )
            .group_by(IdentityTable.identity_type, derived_status)
        )
        return _rows_to_type_status_counts(self._session.exec(stmt).all())


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

    def _increment_int_column(self, line: BillingLineItem, attr: str) -> int:
        row = self._session.get(BillingTable, _billing_pk(line))
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
            update(BillingTable)
            .where(
                col(BillingTable.ecosystem) == ecosystem,
                col(BillingTable.tenant_id) == tenant_id,
                col(BillingTable.timestamp) >= start,
                col(BillingTable.timestamp) < end,
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
        return items, total  # type: ignore[return-value]  # SQLModel returns table types, protocol expects domain types

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        stmt = delete(BillingTable).where(
            col(BillingTable.ecosystem) == ecosystem,
            col(BillingTable.tenant_id) == tenant_id,
            col(BillingTable.timestamp) < before,
        )
        result = self._session.execute(stmt)
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount


# --- ChargebackRepository ---

_ALLOCATION_SUCCESS_CODES = frozenset(
    {
        AllocationDetail.USAGE_RATIO_ALLOCATION,
        AllocationDetail.EVEN_SPLIT_ALLOCATION,
    }
)


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

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        """Return sorted distinct dates with chargeback facts for the tenant."""
        dim_subquery = (
            select(ChargebackDimensionTable.dimension_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
            )
            .scalar_subquery()
        )
        stmt = (
            select(func.date(ChargebackFactTable.timestamp))
            .where(col(ChargebackFactTable.dimension_id).in_(dim_subquery))
            .distinct()
            .order_by(func.date(ChargebackFactTable.timestamp))
        )
        rows = self._session.execute(stmt).scalars().all()
        # func.date() returns str in SQLite (e.g. "2026-01-15"); coerce to date
        return [date.fromisoformat(r) if isinstance(r, str) else r for r in rows]

    def find_aggregated_for_emit(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: str,
    ) -> list[ChargebackRow]:
        """SQL GROUP BY aggregation for emit. Returns ChargebackRow with floored timestamp, dimension_id=None."""
        from core.models.chargeback import ChargebackRow, CostType

        # strftime bucket expression: daily → "%Y-%m-%d", monthly → "%Y-%m"
        bucket_fmt = "%Y-%m" if granularity == "monthly" else "%Y-%m-%d"

        start_dt = datetime(start.year, start.month, start.day, tzinfo=UTC)
        end_dt = datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)

        bucket_expr = func.strftime(bucket_fmt, ChargebackFactTable.timestamp).label("bucket")

        stmt = (
            select(  # type: ignore[call-overload]  # SQLModel select() overload stubs don't cover 10+ columns
                ChargebackDimensionTable.ecosystem,
                ChargebackDimensionTable.tenant_id,
                bucket_expr,
                ChargebackDimensionTable.resource_id,
                ChargebackDimensionTable.product_category,
                ChargebackDimensionTable.product_type,
                ChargebackDimensionTable.identity_id,
                ChargebackDimensionTable.cost_type,
                func.max(ChargebackDimensionTable.allocation_method).label("allocation_method"),
                func.sum(cast(col(ChargebackFactTable.amount), String)).label("total_amount"),
            )
            .join(
                ChargebackFactTable,
                col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id),
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackFactTable.timestamp) >= start_dt,
                col(ChargebackFactTable.timestamp) < end_dt,
            )
            .group_by(
                ChargebackDimensionTable.ecosystem,
                ChargebackDimensionTable.tenant_id,
                "bucket",
                ChargebackDimensionTable.resource_id,
                ChargebackDimensionTable.product_category,
                ChargebackDimensionTable.product_type,
                ChargebackDimensionTable.identity_id,
                ChargebackDimensionTable.cost_type,
            )
        )

        rows = self._session.execute(stmt).all()
        result: list[ChargebackRow] = []
        for r in rows:
            bucket_str = r.bucket
            # Parse bucket → floor timestamp
            if granularity == "monthly":
                ts = datetime(int(bucket_str[:4]), int(bucket_str[5:7]), 1, tzinfo=UTC)
            else:
                ts = datetime(
                    int(bucket_str[:4]),
                    int(bucket_str[5:7]),
                    int(bucket_str[8:10]),
                    tzinfo=UTC,
                )
            result.append(
                ChargebackRow(
                    ecosystem=r.ecosystem,
                    tenant_id=r.tenant_id,
                    timestamp=ts,
                    resource_id=r.resource_id,
                    product_category=r.product_category,
                    product_type=r.product_type,
                    identity_id=r.identity_id,
                    cost_type=CostType(r.cost_type),
                    amount=Decimal(str(r.total_amount)),
                    allocation_method=r.allocation_method,
                    allocation_detail=None,
                    dimension_id=None,
                    tags={},
                    metadata={},
                )
            )
        return result

    def find_allocation_issues(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[AllocationIssueRow], int]:
        join_clause = col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id)
        where: list[Any] = [
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackDimensionTable.allocation_detail).is_not(None),
            col(ChargebackDimensionTable.allocation_detail).not_in(_ALLOCATION_SUCCESS_CODES),
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

        group_cols = [
            col(ChargebackDimensionTable.ecosystem),
            col(ChargebackDimensionTable.env_id),
            col(ChargebackDimensionTable.resource_id),
            col(ChargebackDimensionTable.product_type),
            col(ChargebackDimensionTable.identity_id),
            col(ChargebackDimensionTable.allocation_detail),
        ]

        usage_expr = func.sum(
            case(
                (col(ChargebackDimensionTable.cost_type) == CostType.USAGE, col(ChargebackFactTable.amount)),
                else_=0,
            )
        ).label("usage_cost")
        shared_expr = func.sum(
            case(
                (col(ChargebackDimensionTable.cost_type) == CostType.SHARED, col(ChargebackFactTable.amount)),
                else_=0,
            )
        ).label("shared_cost")
        total_expr = func.sum(col(ChargebackFactTable.amount)).label("total_cost")

        count_subq = select(func.count()).select_from(
            select(*group_cols)
            .select_from(ChargebackDimensionTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .group_by(*group_cols)
            .subquery()
        )
        total: int = self._session.exec(count_subq).one()

        stmt = (
            select(  # type: ignore[call-overload]  # SQLModel select() overload stubs don't cover dynamic column lists
                *group_cols,
                func.count().label("row_count"),
                usage_expr,
                shared_expr,
                total_expr,
            )
            .select_from(ChargebackDimensionTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .group_by(*group_cols)
            .order_by(total_expr.desc())
            .offset(offset)
            .limit(limit)
        )

        rows = self._session.execute(stmt).all()
        items = [
            AllocationIssueRow(
                ecosystem=r.ecosystem,
                env_id=r.env_id,
                resource_id=r.resource_id,
                product_type=r.product_type,
                identity_id=r.identity_id,
                allocation_detail=r.allocation_detail,
                row_count=r.row_count,
                usage_cost=Decimal(str(r.usage_cost or 0)),
                shared_cost=Decimal(str(r.shared_cost or 0)),
                total_cost=Decimal(str(r.total_cost or 0)),
            )
            for r in rows
        ]
        return items, total

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
        tag_key: str | None = None,
        tag_value: str | None = None,
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
        if tag_key is not None:
            tag_where: list[Any] = [
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.tag_key) == tag_key,
            ]
            if tag_value is not None:
                tag_where.append(col(EntityTagTable.tag_value) == tag_value)
            # Scope each subquery to its entity_type to prevent cross-type false positives
            # (e.g. a resource_id that happens to equal some identity's entity_id)
            resource_sub = (
                select(EntityTagTable.entity_id)
                .where(*tag_where, col(EntityTagTable.entity_type) == "resource")
                .scalar_subquery()
            )
            identity_sub = (
                select(EntityTagTable.entity_id)
                .where(*tag_where, col(EntityTagTable.entity_type) == "identity")
                .scalar_subquery()
            )
            where.append(
                or_(
                    col(ChargebackDimensionTable.resource_id).in_(resource_sub),
                    col(ChargebackDimensionTable.identity_id).in_(identity_sub),
                )
            )
        join_clause = col(ChargebackDimensionTable.dimension_id) == col(ChargebackFactTable.dimension_id)
        return where, join_clause

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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[ChargebackRow], int]:
        where, join_clause = self._build_chargeback_where(
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            tag_key=tag_key,
            tag_value=tag_value,
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
        if tags_repo is not None:
            _overlay_tags(items, tags_repo)
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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[ChargebackRow]:
        """Yield ChargebackRow objects in batches. Memory bounded to batch_size rows."""
        where, join_clause = self._build_chargeback_where(
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            tag_key=tag_key,
            tag_value=tag_value,
        )
        stmt = (
            select(ChargebackDimensionTable, ChargebackFactTable)
            .join(ChargebackFactTable, join_clause)
            .where(*where)
            .execution_options(yield_per=batch_size)
        )
        for partition in self._session.execute(stmt).partitions(batch_size):
            batch = [chargeback_to_domain(dim, fact) for dim, fact in partition]
            if tags_repo is not None:
                _overlay_tags(batch, tags_repo)
            yield from batch

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        dim_subquery = (
            select(ChargebackDimensionTable.dimension_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
            )
            .scalar_subquery()
        )

        # Delete facts before cutoff for those dimensions
        fact_del = delete(ChargebackFactTable).where(
            col(ChargebackFactTable.dimension_id).in_(dim_subquery),
            col(ChargebackFactTable.timestamp) < before,
        )
        result = self._session.execute(fact_del)
        deleted_count: int = result.rowcount  # type: ignore[attr-defined]  # CursorResult always has rowcount

        # Clean up orphaned dimensions (no remaining facts)
        orphan_del = delete(ChargebackDimensionTable).where(
            col(ChargebackDimensionTable.dimension_id).in_(dim_subquery),
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
            env_id=row.env_id,
        )

    def get_dimensions_batch(self, dimension_ids: list[int]) -> dict[int, ChargebackDimensionInfo]:
        from core.models.chargeback import ChargebackDimensionInfo

        if not dimension_ids:
            return {}
        result: dict[int, ChargebackDimensionInfo] = {}
        for i in range(0, len(dimension_ids), _CHUNK_SIZE):
            chunk = dimension_ids[i : i + _CHUNK_SIZE]
            stmt = select(ChargebackDimensionTable).where(col(ChargebackDimensionTable.dimension_id).in_(chunk))
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
                        env_id=row.env_id,
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
        """Return all distinct dimension IDs matching the given filters.

        Result set is unbounded — callers must chunk their processing (e.g., _run_bulk_tag
        uses _BULK_CHUNK_SIZE=500 to bound memory and SQL parameter counts).
        The query itself operates entirely at SQL level; no parameter explosion risk here.
        """
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
        limit: int | None = None,
        tag_group_by: list[str] | None = None,
        tag_filters: dict[str, list[str]] | None = None,
    ) -> list[AggregationRow]:
        # Collect all unique tag keys needed across group_by and filters, preserving order
        all_tag_keys: list[str] = list(dict.fromkeys([*(tag_group_by or []), *(tag_filters or {})]))

        tag_specs: dict[str, TagJoinSpec] = {}
        if all_tag_keys:
            specs = build_tag_join_specs(
                tag_keys=all_tag_keys,
                tenant_id=tenant_id,
                resource_id_col=col(ChargebackDimensionTable.resource_id),
                identity_id_col=col(ChargebackDimensionTable.identity_id),
            )
            tag_specs = {s.tag_key: s for s in specs}

        # Build dimension group columns
        group_cols = []
        group_labels = []
        for gb in group_by:
            if gb == "environment_id":
                # env_id is now a native column — no resource table join needed
                group_cols.append(cast(col(ChargebackDimensionTable.env_id), String).label("dim_environment_id"))
            else:
                col_ref = getattr(ChargebackDimensionTable, gb)
                group_cols.append(cast(col(col_ref), String).label(f"dim_{gb}"))
            group_labels.append(f"dim_{gb}")

        # Append tag GROUP BY columns before select_cols/group_by_labels snapshots
        tag_group_labels: list[tuple[str, str]] = []  # [(tag_key, sql_label), ...]
        for key in tag_group_by or []:
            spec = tag_specs[key]
            group_cols.append(spec.group_expr.label(spec.label))
            group_labels.append(spec.label)
            tag_group_labels.append((key, spec.label))

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

        stmt = select(*select_cols).select_from(ChargebackDimensionTable).join(ChargebackFactTable, join_clause)

        # Apply one LEFT JOIN pair per tag key
        for spec in tag_specs.values():
            stmt = stmt.join(spec.resource_alias, spec.resource_join_cond, isouter=True)
            if spec.identity_alias is not None:
                stmt = stmt.join(spec.identity_alias, spec.identity_join_cond, isouter=True)

        # Tag WHERE filters: intra-tag OR (IN clause), inter-tag AND (chained .where())
        for key, values in (tag_filters or {}).items():
            spec = tag_specs[key]
            stmt = stmt.where(spec.resolved_expr.in_(values))

        stmt = stmt.where(*where).group_by(*group_by_labels).order_by("time_bucket", *group_labels).limit(limit)

        results = self._session.execute(stmt).all()
        return [
            AggregationRow(
                dimensions={
                    **{gb: str(getattr(r, f"dim_{gb}", "") or "") for gb in group_by},
                    **{f"tag:{key}": str(getattr(r, label) or "") for key, label in tag_group_labels},
                },
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
            .values(chargeback_calculated=False, topic_attribution_calculated=False)
        )
        self._session.execute(stmt)

    def mark_topic_overlay_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(topic_overlay_gathered=True)
        )
        self._session.execute(stmt)

    def mark_topic_attribution_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        stmt = (
            update(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.tracking_date) == tracking_date,
            )
            .values(topic_attribution_calculated=True)
        )
        self._session.execute(stmt)

    def find_needing_topic_attribution(self, ecosystem: str, tenant_id: str) -> list[PipelineState]:
        stmt = (
            select(PipelineStateTable)
            .where(
                col(PipelineStateTable.ecosystem) == ecosystem,
                col(PipelineStateTable.tenant_id) == tenant_id,
                col(PipelineStateTable.topic_overlay_gathered) == True,  # noqa: E712
                col(PipelineStateTable.topic_attribution_calculated) == False,  # noqa: E712
            )
            .order_by(col(PipelineStateTable.tracking_date))
        )
        return [pipeline_state_to_domain(row) for row in self._session.exec(stmt).all()]

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


# --- EntityTagRepository ---


class SQLModelEntityTagRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_tag(
        self,
        tenant_id: str,
        entity_type: str,
        entity_id: str,
        tag_key: str,
        tag_value: str,
        created_by: str,
    ) -> EntityTag:
        row = EntityTagTable(
            tenant_id=tenant_id,
            entity_type=entity_type,
            entity_id=entity_id,
            tag_key=tag_key,
            tag_value=tag_value,
            created_by=created_by,
        )
        self._session.add(row)
        self._session.flush()  # raises IntegrityError on duplicate composite key
        return entity_tag_to_domain(row)

    def get_tags(self, tenant_id: str, entity_type: str, entity_id: str) -> list[EntityTag]:
        stmt = select(EntityTagTable).where(
            col(EntityTagTable.tenant_id) == tenant_id,
            col(EntityTagTable.entity_type) == entity_type,
            col(EntityTagTable.entity_id) == entity_id,
        )
        return [entity_tag_to_domain(r) for r in self._session.exec(stmt).all()]

    def update_tag(self, tag_id: int, tag_value: str) -> EntityTag:
        row = self._session.get(EntityTagTable, tag_id)
        if row is None:
            raise KeyError(f"Tag {tag_id} not found")
        row.tag_value = tag_value
        self._session.add(row)
        self._session.flush()
        return entity_tag_to_domain(row)

    def delete_tag(self, tag_id: int) -> None:
        row = self._session.get(EntityTagTable, tag_id)
        if row:
            self._session.delete(row)
            self._session.flush()

    def find_tags_for_tenant(
        self,
        tenant_id: str,
        limit: int = 100,
        offset: int = 0,
        entity_type: str | None = None,
        tag_key: str | None = None,
    ) -> tuple[list[EntityTag], int]:
        where: list[Any] = [col(EntityTagTable.tenant_id) == tenant_id]
        if entity_type is not None:
            where.append(col(EntityTagTable.entity_type) == entity_type)
        if tag_key is not None:
            where.append(col(EntityTagTable.tag_key).ilike(f"%{tag_key}%"))

        count_stmt = select(func.count()).select_from(EntityTagTable).where(*where)
        total: int = self._session.exec(count_stmt).one()

        stmt = select(EntityTagTable).where(*where).offset(offset).limit(limit)
        items = [entity_tag_to_domain(r) for r in self._session.exec(stmt).all()]
        return items, total

    def find_tags_for_entities(
        self,
        tenant_id: str,
        entity_type: str,
        entity_ids: list[str],
    ) -> dict[str, list[EntityTag]]:
        if not entity_ids:
            return {}
        result: dict[str, list[EntityTag]] = {}
        for i in range(0, len(entity_ids), _CHUNK_SIZE):
            chunk = entity_ids[i : i + _CHUNK_SIZE]
            stmt = select(EntityTagTable).where(
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.entity_type) == entity_type,
                col(EntityTagTable.entity_id).in_(chunk),
            )
            for row in self._session.exec(stmt).all():
                result.setdefault(row.entity_id, []).append(entity_tag_to_domain(row))
        return result

    def bulk_add_tags(
        self,
        tenant_id: str,
        items: list[dict[str, Any]],
        override_existing: bool,
        created_by: str,
    ) -> tuple[int, int, int]:
        # Known limitation: N individual SELECT queries (up to 10,000). Acceptable for initial
        # implementation; a chunked INSERT ON CONFLICT UPSERT can replace this if bulk performance
        # becomes a concern.
        created = updated = skipped = 0
        for item in items:
            entity_type = item["entity_type"]
            entity_id = item["entity_id"]
            tag_key = item["tag_key"]
            tag_value = item["tag_value"]
            stmt = select(EntityTagTable).where(
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.entity_type) == entity_type,
                col(EntityTagTable.entity_id) == entity_id,
                col(EntityTagTable.tag_key) == tag_key,
            )
            existing = self._session.exec(stmt).first()
            if existing is not None:
                if override_existing:
                    existing.tag_value = tag_value
                    self._session.add(existing)
                    updated += 1
                else:
                    skipped += 1
            else:
                row = EntityTagTable(
                    tenant_id=tenant_id,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    tag_key=tag_key,
                    tag_value=tag_value,
                    created_by=created_by,
                )
                self._session.add(row)
                created += 1
        self._session.flush()
        return created, updated, skipped


class SQLModelEmissionRepository:
    """SQLModel implementation of EmissionRepository."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, record: EmissionRecord) -> None:
        existing = self._session.exec(
            select(EmissionRecordTable).where(
                EmissionRecordTable.ecosystem == record.ecosystem,
                EmissionRecordTable.tenant_id == record.tenant_id,
                EmissionRecordTable.emitter_name == record.emitter_name,
                EmissionRecordTable.pipeline == record.pipeline,
                EmissionRecordTable.date == record.date,
            )
        ).first()
        if existing is not None:
            existing.status = record.status
            existing.attempt_count += 1
            self._session.add(existing)
        else:
            self._session.add(emission_record_to_table(record))

    def get_emitted_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        rows = self._session.exec(
            select(col(EmissionRecordTable.date)).where(
                EmissionRecordTable.ecosystem == ecosystem,
                EmissionRecordTable.tenant_id == tenant_id,
                EmissionRecordTable.emitter_name == emitter_name,
                EmissionRecordTable.pipeline == pipeline,
                EmissionRecordTable.status == "emitted",
            )
        ).all()
        return set(rows)

    def get_failed_dates(self, ecosystem: str, tenant_id: str, emitter_name: str, pipeline: str) -> set[date]:
        rows = self._session.exec(
            select(col(EmissionRecordTable.date)).where(
                EmissionRecordTable.ecosystem == ecosystem,
                EmissionRecordTable.tenant_id == tenant_id,
                EmissionRecordTable.emitter_name == emitter_name,
                EmissionRecordTable.pipeline == pipeline,
                EmissionRecordTable.status == "failed",
            )
        ).all()
        return set(rows)


# --- TopicAttributionRepository ---

from core.models.topic_attribution import TopicAttributionRow  # noqa: E402
from core.storage.backends.sqlmodel.tables import (  # noqa: E402
    TopicAttributionDimensionTable,
    TopicAttributionFactTable,
)

if TYPE_CHECKING:
    from core.models.topic_attribution import TopicAttributionAggregationResult


class TopicAttributionRepository:
    """Repository for topic attribution star schema.

    Same pattern as SQLModelChargebackRepository: dimension cache,
    _get_or_create_dimension, upsert_batch, find_by_date, find_by_cluster,
    find_by_filters, aggregate, get_distinct_dates, delete_before.
    """

    def __init__(self, session: Session) -> None:
        self._session = session
        self._dimension_cache: dict[tuple[str, ...], TopicAttributionDimensionTable] = {}

    def upsert_batch(self, rows: list[TopicAttributionRow]) -> int:
        unique_facts: dict[tuple[datetime, int], TopicAttributionFactTable] = {}
        for row in rows:
            dim = self._get_or_create_dimension(row)
            fact = TopicAttributionFactTable(
                timestamp=row.timestamp,
                dimension_id=dim.dimension_id,
                amount=str(row.amount),
            )
            unique_facts[(fact.timestamp, fact.dimension_id)] = fact

        if unique_facts:
            for fact in unique_facts.values():
                self._session.merge(fact)
        return len(unique_facts)

    def _get_or_create_dimension(self, row: TopicAttributionRow) -> TopicAttributionDimensionTable:
        key = (
            row.ecosystem,
            row.tenant_id,
            row.env_id,
            row.cluster_resource_id,
            row.topic_name,
            row.product_category,
            row.product_type,
            row.attribution_method,
        )
        cached = self._dimension_cache.get(key)
        if cached is not None:
            return cached

        stmt = select(TopicAttributionDimensionTable).where(
            col(TopicAttributionDimensionTable.ecosystem) == row.ecosystem,
            col(TopicAttributionDimensionTable.tenant_id) == row.tenant_id,
            col(TopicAttributionDimensionTable.env_id) == row.env_id,
            col(TopicAttributionDimensionTable.cluster_resource_id) == row.cluster_resource_id,
            col(TopicAttributionDimensionTable.topic_name) == row.topic_name,
            col(TopicAttributionDimensionTable.product_category) == row.product_category,
            col(TopicAttributionDimensionTable.product_type) == row.product_type,
            col(TopicAttributionDimensionTable.attribution_method) == row.attribution_method,
        )
        existing = self._session.exec(stmt).first()
        if existing:
            self._dimension_cache[key] = existing
            return existing

        dim = TopicAttributionDimensionTable(
            ecosystem=row.ecosystem,
            tenant_id=row.tenant_id,
            env_id=row.env_id,
            cluster_resource_id=row.cluster_resource_id,
            topic_name=row.topic_name,
            resource_id=f"{row.cluster_resource_id}:topic:{row.topic_name}",
            product_category=row.product_category,
            product_type=row.product_type,
            attribution_method=row.attribution_method,
        )
        self._session.add(dim)
        self._session.flush()
        self._dimension_cache[key] = dim
        return dim

    def delete_by_date(self, ecosystem: str, tenant_id: str, target_date: date) -> int:
        """Delete all facts for a specific date. Returns count deleted."""
        start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        end = start + timedelta(days=1)

        dim_ids_stmt = select(TopicAttributionDimensionTable.dimension_id).where(
            col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
            col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
        )
        dim_ids = list(self._session.execute(dim_ids_stmt).scalars().all())
        if not dim_ids:
            return 0

        count: int = 0
        for chunk_start in range(0, len(dim_ids), _CHUNK_SIZE):
            chunk = dim_ids[chunk_start : chunk_start + _CHUNK_SIZE]
            count += self._session.execute(
                select(func.count())
                .select_from(TopicAttributionFactTable)
                .where(
                    col(TopicAttributionFactTable.dimension_id).in_(chunk),
                    col(TopicAttributionFactTable.timestamp) >= start,
                    col(TopicAttributionFactTable.timestamp) < end,
                )
            ).scalar_one()
            self._session.execute(
                delete(TopicAttributionFactTable).where(
                    col(TopicAttributionFactTable.dimension_id).in_(chunk),
                    col(TopicAttributionFactTable.timestamp) >= start,
                    col(TopicAttributionFactTable.timestamp) < end,
                )
            )
        return count

    def find_by_date(
        self,
        ecosystem: str,
        tenant_id: str,
        target_date: date,
    ) -> list[TopicAttributionRow]:
        start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        end = start + timedelta(days=1)
        stmt = (
            select(TopicAttributionDimensionTable, TopicAttributionFactTable)
            .join(
                TopicAttributionFactTable,
                col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id),
            )
            .where(
                col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
                col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
                col(TopicAttributionFactTable.timestamp) >= start,
                col(TopicAttributionFactTable.timestamp) < end,
            )
        )
        return [_ta_to_domain(dim, fact) for dim, fact in self._session.exec(stmt).all()]

    def find_by_cluster(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_resource_id: str,
        start: datetime,
        end: datetime,
    ) -> list[TopicAttributionRow]:
        stmt = (
            select(TopicAttributionDimensionTable, TopicAttributionFactTable)
            .join(
                TopicAttributionFactTable,
                col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id),
            )
            .where(
                col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
                col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
                col(TopicAttributionDimensionTable.cluster_resource_id) == cluster_resource_id,
                col(TopicAttributionFactTable.timestamp) >= start,
                col(TopicAttributionFactTable.timestamp) < end,
            )
        )
        return [_ta_to_domain(dim, fact) for dim, fact in self._session.exec(stmt).all()]

    def _build_ta_where(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
    ) -> tuple[list[Any], Any]:
        """Build WHERE clauses and join condition for topic attribution queries."""
        _base_where = [
            col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
            col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
        ]
        optional: list[Any] = []
        if start is not None:
            optional.append(col(TopicAttributionFactTable.timestamp) >= start)
        if end is not None:
            optional.append(col(TopicAttributionFactTable.timestamp) < end)
        if cluster_resource_id is not None:
            optional.append(col(TopicAttributionDimensionTable.cluster_resource_id).contains(cluster_resource_id))
        if topic_name is not None:
            optional.append(col(TopicAttributionDimensionTable.topic_name).contains(topic_name))
        if product_type is not None:
            optional.append(col(TopicAttributionDimensionTable.product_type) == product_type)
        if attribution_method is not None:
            optional.append(col(TopicAttributionDimensionTable.attribution_method) == attribution_method)
        join_cond = col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id)
        return [*_base_where, *optional], join_cond

    def find_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[TopicAttributionRow], int]:
        """Returns (items, total_count). All filters applied at SQL level."""
        from sqlalchemy import func as sa_func

        where, join_cond = self._build_ta_where(
            ecosystem, tenant_id, start, end, cluster_resource_id, topic_name, product_type, attribution_method
        )
        count_stmt = (
            select(sa_func.count())
            .select_from(TopicAttributionFactTable)
            .join(TopicAttributionDimensionTable, join_cond)
            .where(*where)
        )
        total: int = self._session.execute(count_stmt).scalar_one()

        data_stmt = (
            select(TopicAttributionDimensionTable, TopicAttributionFactTable)
            .join(TopicAttributionFactTable, join_cond)
            .where(*where)
            .offset(offset)
            .limit(limit)
        )
        rows = self._session.exec(data_stmt).all()
        return [_ta_to_domain(dim, fact) for dim, fact in rows], total

    def iter_by_filters(
        self,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        batch_size: int = 5000,
    ) -> Iterator[TopicAttributionRow]:
        """Yield TopicAttributionRow objects in batches. Memory bounded to batch_size rows."""
        where, join_cond = self._build_ta_where(
            ecosystem, tenant_id, start, end, cluster_resource_id, topic_name, product_type, attribution_method
        )
        stmt = (
            select(TopicAttributionDimensionTable, TopicAttributionFactTable)
            .join(TopicAttributionFactTable, join_cond)
            .where(*where)
            .execution_options(yield_per=batch_size)
        )
        for partition in self._session.execute(stmt).partitions(batch_size):
            yield from (_ta_to_domain(dim, fact) for dim, fact in partition)

    def aggregate(
        self,
        ecosystem: str,
        tenant_id: str,
        group_by: list[str],
        time_bucket: str = "day",
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        tag_group_by: list[str] | None = None,
        tag_filters: dict[str, list[str]] | None = None,
    ) -> TopicAttributionAggregationResult:
        """SQL GROUP BY aggregation. Returns domain type."""
        from sqlalchemy import Numeric
        from sqlalchemy import cast as sa_cast
        from sqlalchemy import func as sa_func
        from sqlalchemy import select as sa_select

        from core.models.topic_attribution import (
            TopicAttributionAggregationBucket,
            TopicAttributionAggregationResult,
        )

        valid_group_fields = {
            "cluster_resource_id",
            "topic_name",
            "product_type",
            "product_category",
            "attribution_method",
            "env_id",
        }
        valid_group_by = [f for f in group_by if f in valid_group_fields]

        # Collect all unique tag keys across group_by and filters
        all_tag_keys: list[str] = list(dict.fromkeys([*(tag_group_by or []), *(tag_filters or {})]))

        tag_specs: dict[str, TagJoinSpec] = {}
        if all_tag_keys:
            specs = build_tag_join_specs(
                tag_keys=all_tag_keys,
                tenant_id=tenant_id,
                resource_id_col=col(TopicAttributionDimensionTable.resource_id),
                identity_id_col=None,  # topic attribution: resource entity only
            )
            tag_specs = {s.tag_key: s for s in specs}

        dim_table = TopicAttributionDimensionTable
        group_cols: list[Any] = []
        group_labels: list[str] = []
        for gb in valid_group_by:
            col_ref = getattr(dim_table, gb)
            group_cols.append(col(col_ref).label(f"dim_{gb}"))
            group_labels.append(f"dim_{gb}")

        tag_group_labels: list[tuple[str, str]] = []  # [(tag_key, sql_label), ...]
        for key in tag_group_by or []:
            spec = tag_specs[key]
            group_cols.append(spec.group_expr.label(spec.label))
            group_labels.append(spec.label)
            tag_group_labels.append((key, spec.label))

        group_by_labels = [*group_labels, "time_bucket"]

        bucket_formats: dict[str, str] = {
            "hour": "%Y-%m-%dT%H:00:00",
            "day": "%Y-%m-%d",
            "week": "%Y-W%W",
            "month": "%Y-%m",
        }
        fmt = bucket_formats.get(time_bucket, "%Y-%m-%d")
        time_expr = sa_func.strftime(fmt, TopicAttributionFactTable.timestamp)
        join_clause = col(TopicAttributionDimensionTable.dimension_id) == col(TopicAttributionFactTable.dimension_id)

        where: list[Any] = [
            col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
            col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
        ]
        if start is not None:
            where.append(col(TopicAttributionFactTable.timestamp) >= start)
        if end is not None:
            where.append(col(TopicAttributionFactTable.timestamp) < end)
        if cluster_resource_id is not None:
            where.append(col(TopicAttributionDimensionTable.cluster_resource_id).contains(cluster_resource_id))
        if topic_name is not None:
            where.append(col(TopicAttributionDimensionTable.topic_name).contains(topic_name))
        if product_type is not None:
            where.append(col(TopicAttributionDimensionTable.product_type) == product_type)

        amount_sum = sa_func.sum(sa_cast(col(TopicAttributionFactTable.amount), Numeric)).label("total_amount")
        row_count_expr = sa_func.count().label("row_count")

        stmt = (
            sa_select(*group_cols, time_expr.label("time_bucket"), amount_sum, row_count_expr)
            .select_from(TopicAttributionDimensionTable)
            .join(TopicAttributionFactTable, join_clause)
        )

        # Apply one LEFT JOIN per tag key (resource-only; identity_id_col=None → no identity alias)
        for spec in tag_specs.values():
            stmt = stmt.join(spec.resource_alias, spec.resource_join_cond, isouter=True)
            if spec.identity_alias is not None:
                stmt = stmt.join(spec.identity_alias, spec.identity_join_cond, isouter=True)

        # Tag WHERE filters: intra-tag OR (IN), inter-tag AND (chained .where())
        for key, values in (tag_filters or {}).items():
            spec = tag_specs[key]
            stmt = stmt.where(spec.resolved_expr.in_(values))

        stmt = stmt.where(*where).group_by(*group_by_labels).order_by("time_bucket", *group_labels)

        results = self._session.execute(stmt).all()
        result_buckets = [
            TopicAttributionAggregationBucket(
                dimensions={
                    **{gb: str(getattr(r, f"dim_{gb}", "") or "") for gb in valid_group_by},
                    **{f"tag:{key}": str(getattr(r, label) or "") for key, label in tag_group_labels},
                },
                time_bucket=str(r.time_bucket),
                total_amount=Decimal(str(r.total_amount or 0)),
                row_count=int(r.row_count),
            )
            for r in results
        ]
        total_amount = sum((b.total_amount for b in result_buckets), Decimal(0))
        return TopicAttributionAggregationResult(
            buckets=result_buckets,
            total_amount=total_amount,
            total_rows=sum(b.row_count for b in result_buckets),
        )

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        """Return sorted list of distinct dates that have topic attribution facts."""
        stmt = (
            select(func.date(TopicAttributionFactTable.timestamp))
            .join(
                TopicAttributionDimensionTable,
                col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id),
            )
            .where(
                col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
                col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
            )
            .distinct()
            .order_by(func.date(TopicAttributionFactTable.timestamp))
        )
        rows = self._session.execute(stmt).scalars().all()
        return [date.fromisoformat(r) if isinstance(r, str) else r for r in rows]

    def delete_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int:
        """Delete fact rows older than cutoff, then prune orphaned dimension rows."""
        dim_ids_stmt = select(TopicAttributionDimensionTable.dimension_id).where(
            col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
            col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
        )
        dim_ids = list(self._session.execute(dim_ids_stmt).scalars().all())
        if not dim_ids:
            return 0

        deleted_facts_count = 0
        for chunk_start in range(0, len(dim_ids), _CHUNK_SIZE):
            chunk = dim_ids[chunk_start : chunk_start + _CHUNK_SIZE]
            result = self._session.execute(
                delete(TopicAttributionFactTable).where(
                    col(TopicAttributionFactTable.dimension_id).in_(chunk),
                    col(TopicAttributionFactTable.timestamp) < before,
                )
            )
            deleted_facts_count += result.rowcount  # type: ignore[attr-defined]

        # Prune orphaned dimensions (no remaining facts)
        remaining_stmt = (
            select(TopicAttributionFactTable.dimension_id)
            .where(col(TopicAttributionFactTable.dimension_id).in_(dim_ids))
            .distinct()
        )
        remaining_ids = set(self._session.execute(remaining_stmt).scalars().all())
        orphaned = [d for d in dim_ids if d not in remaining_ids]
        if orphaned:
            for chunk_start in range(0, len(orphaned), _CHUNK_SIZE):
                chunk = orphaned[chunk_start : chunk_start + _CHUNK_SIZE]
                self._session.execute(
                    delete(TopicAttributionDimensionTable).where(
                        col(TopicAttributionDimensionTable.dimension_id).in_(chunk)
                    )
                )

        return deleted_facts_count


def _ta_to_domain(
    dim: TopicAttributionDimensionTable,
    fact: TopicAttributionFactTable,
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=dim.ecosystem,
        tenant_id=dim.tenant_id,
        timestamp=fact.timestamp,
        env_id=dim.env_id,
        cluster_resource_id=dim.cluster_resource_id,
        topic_name=dim.topic_name,
        product_category=dim.product_category,
        product_type=dim.product_type,
        attribution_method=dim.attribution_method,
        amount=Decimal(fact.amount) if fact.amount else Decimal(0),
        dimension_id=dim.dimension_id,
    )
