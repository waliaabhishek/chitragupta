from __future__ import annotations

import heapq
import logging
from collections.abc import Iterator, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from cachetools import TTLCache
from sqlalchemy import case, cast, delete, func, literal, or_, update
from sqlalchemy.types import String
from sqlmodel import Session, col, select

from core.models.chargeback import AggregationRow, AllocationDetail, AllocationIssueRow, CostType
from core.models.counts import TypeStatusCounts
from core.models.graph import (
    CrossReferenceGroup,
    CrossReferenceItem,
    EdgeType,
    GraphDiffNodeData,
    GraphEdgeData,
    GraphNeighborhood,
    GraphNodeData,
    GraphSearchResultData,
    GraphTimelineData,
)

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

# Cluster view grouping thresholds — applied per-group independently.
_CLUSTER_GROUP_THRESHOLD = 20  # per-group: if len(group) > this, use grouped mode
_CLUSTER_TOP_N = 5  # entities surfaced individually in grouped mode
_CLUSTER_EXPAND_CAP = 200  # max individual nodes returned in expand mode
TOP_N_CROSS_REFS = 5  # max items per resource_type group in cross_references

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

    def get_distinct_keys(
        self,
        tenant_id: str,
        entity_type: str | None = None,
    ) -> list[str]:
        where: list[Any] = [col(EntityTagTable.tenant_id) == tenant_id]
        if entity_type is not None:
            where.append(col(EntityTagTable.entity_type) == entity_type)
        stmt = select(col(EntityTagTable.tag_key)).where(*where).distinct().order_by(col(EntityTagTable.tag_key))
        return list(self._session.exec(stmt).all())

    def get_distinct_values(
        self,
        tenant_id: str,
        tag_key: str,
        entity_type: str | None = None,
        q: str | None = None,
    ) -> list[str]:
        where: list[Any] = [
            col(EntityTagTable.tenant_id) == tenant_id,
            col(EntityTagTable.tag_key) == tag_key,
        ]
        if entity_type is not None:
            where.append(col(EntityTagTable.entity_type) == entity_type)
        if q is not None:
            where.append(col(EntityTagTable.tag_value).ilike(f"{q}%"))
        stmt = select(col(EntityTagTable.tag_value)).where(*where).distinct().order_by(col(EntityTagTable.tag_value))
        return list(self._session.exec(stmt).all())


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
        tag_key: str | None = None,
        tag_value: str | None = None,
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
        if tag_key is not None:
            tag_where: list[Any] = [
                col(EntityTagTable.tenant_id) == tenant_id,
                col(EntityTagTable.tag_key) == tag_key,
                col(EntityTagTable.entity_type) == "resource",
            ]
            if tag_value is not None:
                tag_where.append(col(EntityTagTable.tag_value) == tag_value)
            resource_sub = select(EntityTagTable.entity_id).where(*tag_where).scalar_subquery()
            optional.append(col(TopicAttributionDimensionTable.resource_id).in_(resource_sub))
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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[TopicAttributionRow], int]:
        """Returns (items, total_count). All filters applied at SQL level."""
        where, join_cond = self._build_ta_where(
            ecosystem,
            tenant_id,
            start,
            end,
            cluster_resource_id,
            topic_name,
            product_type,
            attribution_method,
            tag_key,
            tag_value,
        )
        count_stmt = (
            select(func.count())
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
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[TopicAttributionRow]:
        """Yield TopicAttributionRow objects in batches. Memory bounded to batch_size rows."""
        where, join_cond = self._build_ta_where(
            ecosystem,
            tenant_id,
            start,
            end,
            cluster_resource_id,
            topic_name,
            product_type,
            attribution_method,
            tag_key,
            tag_value,
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


class SQLModelGraphRepository:
    def __init__(self, session: Session, tags_repo: EntityTagRepository) -> None:
        self._session = session
        self._tags = tags_repo

    def find_neighborhood(
        self,
        ecosystem: str,
        tenant_id: str,
        focus_id: str | None,
        depth: int,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
        expand: Literal["topics", "identities", "resources", "clusters"] | None = None,
        _force_full: bool = False,
    ) -> GraphNeighborhood:
        if focus_id is None:
            return self._root_view(ecosystem, tenant_id, at, period_start, period_end)

        # Resolve the focus entity — try resource first, then identity
        focus_row = self._session.get(ResourceTable, (ecosystem, tenant_id, focus_id))
        if focus_row is not None:
            resource_type = focus_row.resource_type
            # Cluster-type nodes always include identity charge relationships
            if resource_type in {"kafka_cluster", "dedicated_cluster", "cluster"}:
                return self._cluster_view(
                    focus_row,
                    ecosystem,
                    tenant_id,
                    at,
                    period_start,
                    period_end,
                    expand=expand,
                    _force_full=_force_full,
                )
            else:
                return self._resource_view(
                    focus_row,
                    ecosystem,
                    tenant_id,
                    depth,
                    at,
                    period_start,
                    period_end,
                    expand=expand,
                    _force_full=_force_full,
                )

        # Fall back to identity
        identity_row = self._session.get(IdentityTable, (ecosystem, tenant_id, focus_id))
        if identity_row is not None:
            return self._identity_view(
                identity_row,
                ecosystem,
                tenant_id,
                at,
                period_start,
                period_end,
                expand=expand,
                _force_full=_force_full,
            )

        raise KeyError(f"Entity {focus_id!r} not found for tenant {tenant_id!r}")

    # --- Root view ---

    def _root_view(
        self,
        ecosystem: str,
        tenant_id: str,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
    ) -> GraphNeighborhood:
        """Return all environments as nodes with a synthetic tenant→env edge per environment."""
        where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, at)
        where.append(col(ResourceTable.resource_type) == "environment")
        env_rows = self._session.exec(select(ResourceTable).where(*where)).all()

        env_ids = [r.resource_id for r in env_rows]
        # Environments are billed via env_id on chargeback_dimensions, not resource_id
        cost_map = self._aggregate_costs(
            ecosystem, tenant_id, env_ids, period_start, period_end, group_by_column="env_id"
        )
        tags_map = self._tags.find_tags_for_entities(tenant_id, "resource", env_ids)

        nodes = [
            GraphNodeData(
                id=r.resource_id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                cost=cost_map.get(r.resource_id, Decimal("0")),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                tags={t.tag_key: t.tag_value for t in tags_map.get(r.resource_id, [])},
                parent_id=r.parent_id,
                cloud=r.cloud,
                region=r.region,
                status=r.status,
            )
            for r in env_rows
        ]
        # Synthetic tenant node (no DB row — tenant is config-only)
        tenant_node = GraphNodeData(
            id=tenant_id,
            resource_type="tenant",
            display_name=tenant_id,
            cost=sum((n.cost for n in nodes), Decimal("0")),
            created_at=None,
            deleted_at=None,
            tags={},
            parent_id=None,
            cloud=None,
            region=None,
            status="active",
        )
        # All parent edges: parent (tenant) → child (env)
        edges = [
            GraphEdgeData(source=tenant_id, target=r.resource_id, relationship_type=EdgeType.parent) for r in env_rows
        ]
        return GraphNeighborhood(nodes=[tenant_node, *nodes], edges=edges)

    # --- Resource view (environment or other non-cluster focus) ---

    def _resource_view(  # noqa: C901
        self,
        focus_row: ResourceTable,
        ecosystem: str,
        tenant_id: str,
        depth: int,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
        expand: Literal["topics", "identities", "resources", "clusters"] | None = None,
        _force_full: bool = False,
    ) -> GraphNeighborhood:
        """Return focus node + children up to `depth` hops via parent_id relationships."""
        all_resource_rows: list[ResourceTable] = [focus_row]
        # BFS by parent_id for `depth` hops (depth=1 covers direct children)
        current_level = [focus_row.resource_id]
        for _ in range(depth):
            if not current_level:
                break
            where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, at)
            where.append(col(ResourceTable.parent_id).in_(current_level))
            children = self._session.exec(select(ResourceTable).where(*where)).all()
            all_resource_rows.extend(children)
            current_level = [c.resource_id for c in children]

        focus_id = focus_row.resource_id
        child_rows = all_resource_rows[1:]  # All except focus

        # Grouping: apply when children exceed threshold (and not forced full)
        if not _force_full and len(child_rows) > _CLUSTER_GROUP_THRESHOLD:
            child_ids = [r.resource_id for r in child_rows]
            child_cost_map = self._aggregate_costs(
                ecosystem, tenant_id, child_ids, period_start, period_end, group_by_column="resource_id"
            )

            def _child_cost(r: ResourceTable) -> Decimal:
                return child_cost_map.get(r.resource_id, Decimal("0"))

            # Focus node cost
            if focus_row.resource_type == "environment":
                focus_cost = self._aggregate_costs(
                    ecosystem, tenant_id, [focus_id], period_start, period_end, group_by_column="env_id"
                ).get(focus_id, Decimal("0"))
            else:
                focus_cost = self._aggregate_costs(
                    ecosystem, tenant_id, [focus_id], period_start, period_end, group_by_column="resource_id"
                ).get(focus_id, Decimal("0"))

            focus_tags = self._tags.find_tags_for_entities(tenant_id, "resource", [focus_id])
            focus_node = GraphNodeData(
                id=focus_id,
                resource_type=focus_row.resource_type,
                display_name=focus_row.display_name,
                cost=focus_cost,
                created_at=focus_row.created_at,
                deleted_at=focus_row.deleted_at,
                tags={t.tag_key: t.tag_value for t in focus_tags.get(focus_id, [])},
                parent_id=focus_row.parent_id,
                cloud=focus_row.cloud,
                region=focus_row.region,
                status=focus_row.status,
            )

            nodes: list[GraphNodeData] = [focus_node]
            edges: list[GraphEdgeData] = []

            if expand == "resources":
                # Sort by cost DESC, show up to cap non-zero individually, collapse zero-cost
                sorted_children = sorted(child_rows, key=_child_cost, reverse=True)
                nonzero_children = [r for r in sorted_children if _child_cost(r) > Decimal("0")]
                zero_children = [r for r in sorted_children if _child_cost(r) == Decimal("0")]

                included = nonzero_children[:_CLUSTER_EXPAND_CAP]
                overflow = nonzero_children[_CLUSTER_EXPAND_CAP:]

                included_ids = [r.resource_id for r in included]
                tags_map = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", included_ids) if included_ids else {}
                )

                for r in included:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_child_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in tags_map.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.parent_id or focus_id,
                            target=r.resource_id,
                            relationship_type=EdgeType.parent,
                        )
                    )

                if overflow:
                    overflow_count = len(overflow)
                    overflow_cost = sum((_child_cost(r) for r in overflow), Decimal("0"))
                    nodes.append(
                        GraphNodeData(
                            id=f"{focus_id}:capped_resources",
                            resource_type="capped_summary",
                            display_name=f"{overflow_count} more resources",
                            cost=overflow_cost,
                            created_at=None,
                            deleted_at=None,
                            tags={},
                            parent_id=focus_id,
                            cloud=None,
                            region=None,
                            status="active",
                            child_count=overflow_count,
                            child_total_cost=overflow_cost,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=focus_id,
                            target=f"{focus_id}:capped_resources",
                            relationship_type=EdgeType.parent,
                        )
                    )

                if zero_children:
                    zero_count = len(zero_children)
                    nodes.append(
                        GraphNodeData(
                            id=f"{focus_id}:zero_cost_resources",
                            resource_type="zero_cost_summary",
                            display_name=f"{zero_count} others at $0",
                            cost=Decimal("0"),
                            created_at=None,
                            deleted_at=None,
                            tags={},
                            parent_id=focus_id,
                            cloud=None,
                            region=None,
                            status="active",
                            child_count=zero_count,
                            child_total_cost=Decimal("0"),
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=focus_id,
                            target=f"{focus_id}:zero_cost_resources",
                            relationship_type=EdgeType.parent,
                        )
                    )

            else:
                # Grouped mode: resource_group + top-N
                sorted_children = sorted(child_rows, key=_child_cost, reverse=True)
                included = sorted_children[:_CLUSTER_TOP_N]
                child_total_cost = sum((_child_cost(r) for r in child_rows), Decimal("0"))

                nodes.append(
                    GraphNodeData(
                        id=f"{focus_id}:resource_group",
                        resource_type="resource_group",
                        display_name=f"{len(child_rows)} resources",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=focus_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(child_rows),
                        child_total_cost=child_total_cost,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=focus_id,
                        target=f"{focus_id}:resource_group",
                        relationship_type=EdgeType.parent,
                    )
                )

                included_ids = [r.resource_id for r in included]
                tags_map = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", included_ids) if included_ids else {}
                )

                for r in included:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_child_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in tags_map.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.parent_id or focus_id,
                            target=r.resource_id,
                            relationship_type=EdgeType.parent,
                        )
                    )

            return GraphNeighborhood(nodes=nodes, edges=edges)

        # Passthrough: existing behavior (small count or _force_full)
        resource_ids = [r.resource_id for r in all_resource_rows]
        # Children always group by resource_id; only environments aggregate via env_id.
        cost_map = self._aggregate_costs(
            ecosystem, tenant_id, resource_ids, period_start, period_end, group_by_column="resource_id"
        )
        tags_map = self._tags.find_tags_for_entities(tenant_id, "resource", resource_ids)

        # Environment costs are billed via chargeback_dimensions.env_id, not resource_id.
        # A resource_id-grouped query returns $0 for environment nodes even when charges exist.
        # Detect the environment case and issue a separate env_id-grouped query for the focus node.
        if focus_row.resource_type == "environment":
            focus_cost = self._aggregate_costs(
                ecosystem,
                tenant_id,
                [focus_row.resource_id],
                period_start,
                period_end,
                group_by_column="env_id",
            ).get(focus_row.resource_id, Decimal("0"))
        else:
            focus_cost = cost_map.get(focus_row.resource_id, Decimal("0"))

        pt_nodes = [
            GraphNodeData(
                id=r.resource_id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                cost=focus_cost
                if r.resource_id == focus_row.resource_id
                else cost_map.get(r.resource_id, Decimal("0")),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                tags={t.tag_key: t.tag_value for t in tags_map.get(r.resource_id, [])},
                parent_id=r.parent_id,
                cloud=r.cloud,
                region=r.region,
                status=r.status,
            )
            for r in all_resource_rows
        ]
        # Parent edges: parent → child direction (source=parent, target=child)
        pt_edges = [
            GraphEdgeData(
                source=r.parent_id or focus_id,
                target=r.resource_id,
                relationship_type=EdgeType.parent,
            )
            for r in all_resource_rows
            if r.resource_id != focus_id
        ]
        return GraphNeighborhood(nodes=pt_nodes, edges=pt_edges)

    # --- Cluster view ---

    def _aggregate_identity_costs(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        identity_ids: list[str],
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, Decimal]:
        """Sum chargeback_facts.amount per identity_id for this cluster."""
        if not identity_ids:
            return {}
        cost_stmt = (
            select(
                ChargebackDimensionTable.identity_id,
                func.sum(cast(ChargebackFactTable.amount, String)).label("total"),
            )
            .join(
                ChargebackFactTable,
                col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id),
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackDimensionTable.resource_id) == cluster_id,
                col(ChargebackDimensionTable.identity_id).in_(identity_ids),
                col(ChargebackFactTable.timestamp) >= period_start,
                col(ChargebackFactTable.timestamp) < period_end,
            )
            .group_by(col(ChargebackDimensionTable.identity_id))
        )
        result: dict[str, Decimal] = {}
        for row in self._session.exec(cost_stmt).all():
            result[row[0]] = Decimal(row[1] or "0")
        return result

    def _aggregate_topic_attribution_costs(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, Decimal]:
        """Sum topic_attribution_facts.amount per resource_id for this cluster."""
        ta_cost_stmt = (
            select(
                TopicAttributionDimensionTable.resource_id,
                func.sum(cast(TopicAttributionFactTable.amount, String)).label("total"),
            )
            .join(
                TopicAttributionFactTable,
                col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id),
            )
            .where(
                col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
                col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
                col(TopicAttributionDimensionTable.cluster_resource_id) == cluster_id,
                col(TopicAttributionFactTable.timestamp) >= period_start,
                col(TopicAttributionFactTable.timestamp) < period_end,
            )
            .group_by(col(TopicAttributionDimensionTable.resource_id))
        )
        result: dict[str, Decimal] = {}
        for row in self._session.exec(ta_cost_stmt).all():
            result[row[0]] = Decimal(row[1] or "0")
        return result

    def _fetch_cross_references(
        self,
        ecosystem: str,
        tenant_id: str,
        identity_ids: list[str],
        cluster_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> dict[str, list[CrossReferenceGroup]]:
        """Return {identity_id: [CrossReferenceGroup]} for resources each identity is charged in (excluding cluster_id).

        Groups by resource_type, ranks by cost descending, caps at TOP_N_CROSS_REFS per type.
        """
        xref_map: dict[str, list[CrossReferenceGroup]] = {iid: [] for iid in identity_ids}
        if not identity_ids:
            return xref_map

        # Query per-identity, per-resource cost within period
        cost_stmt = (
            select(
                ChargebackDimensionTable.identity_id,
                ChargebackDimensionTable.resource_id,
                func.sum(cast(ChargebackFactTable.amount, String)).label("total_cost"),
            )
            .join(
                ChargebackFactTable,
                col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id),
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackDimensionTable.identity_id).in_(identity_ids),
                col(ChargebackDimensionTable.resource_id) != cluster_id,
                col(ChargebackDimensionTable.resource_id).is_not(None),
                col(ChargebackFactTable.timestamp) >= period_start,
                col(ChargebackFactTable.timestamp) < period_end,
            )
            .group_by(
                col(ChargebackDimensionTable.identity_id),
                col(ChargebackDimensionTable.resource_id),
            )
        )
        cost_rows = self._session.exec(cost_stmt).all()
        if not cost_rows:
            return xref_map

        # Fetch resource_type and display_name for all referenced resource_ids
        resource_ids = {rid for _, rid, _ in cost_rows if rid is not None}
        resource_meta: dict[str, tuple[str, str | None]] = {}
        if resource_ids:
            resource_stmt = select(
                ResourceTable.resource_id,
                ResourceTable.resource_type,
                ResourceTable.display_name,
            ).where(
                col(ResourceTable.ecosystem) == ecosystem,
                col(ResourceTable.tenant_id) == tenant_id,
                col(ResourceTable.resource_id).in_(list(resource_ids)),
            )
            for rid, rtype, dname in self._session.exec(resource_stmt).all():
                resource_meta[rid] = (rtype, dname)

        # Group by (identity_id, resource_type) in Python
        by_type: dict[str, dict[str, list[tuple[str, str | None, Decimal]]]] = {iid: {} for iid in identity_ids}
        for iid, rid, raw_cost in cost_rows:  # type: ignore[assignment]  # SQLModel types nullable FK columns as str|None; None rows filtered by guard below
            if iid is None or rid is None or rid not in resource_meta:
                continue
            rtype, dname = resource_meta[rid]
            cost = Decimal(str(raw_cost or "0"))
            by_type[iid].setdefault(rtype, []).append((rid, dname, cost))

        for iid in identity_ids:
            groups: list[CrossReferenceGroup] = []
            for rtype, items in by_type[iid].items():
                total_count = len(items)
                top_items = heapq.nlargest(TOP_N_CROSS_REFS, items, key=lambda x: x[2])
                groups.append(
                    CrossReferenceGroup(
                        resource_type=rtype,
                        items=[
                            CrossReferenceItem(id=rid, resource_type=rtype, display_name=dname, cost=cost)
                            for rid, dname, cost in top_items
                        ],
                        total_count=total_count,
                    )
                )
            xref_map[iid] = groups

        return xref_map

    def _cluster_view(  # noqa: C901  # dispatcher — complexity is branching, not nesting
        self,
        focus_row: ResourceTable,
        ecosystem: str,
        tenant_id: str,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
        expand: Literal["topics", "identities", "resources", "clusters"] | None = None,
        _force_full: bool = False,
    ) -> GraphNeighborhood:
        """Dispatcher: grouped summary (default), expand, or full (_force_full / small clusters)."""
        cluster_id = focus_row.resource_id

        if _force_full:
            return self._cluster_view_full(focus_row, ecosystem, tenant_id, at, period_start, period_end)

        # Fetch all child rows and identity_ids (needed for threshold checks + ranking)
        child_where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, at)
        child_where.append(col(ResourceTable.parent_id) == cluster_id)
        child_rows = list(self._session.exec(select(ResourceTable).where(*child_where)).all())

        dim_stmt = (
            select(ChargebackDimensionTable.identity_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackDimensionTable.resource_id) == cluster_id,
                col(ChargebackDimensionTable.identity_id) != "",
            )
            .distinct()
        )
        identity_ids = list(self._session.exec(dim_stmt).all())

        topics_oversized = len(child_rows) > _CLUSTER_GROUP_THRESHOLD
        identities_oversized = len(identity_ids) > _CLUSTER_GROUP_THRESHOLD

        # Neither group oversized and no explicit expand → full unfiltered path
        if expand is None and not topics_oversized and not identities_oversized:
            return self._cluster_view_full(focus_row, ecosystem, tenant_id, at, period_start, period_end)

        # Compute costs for all entities (needed for ranking and totals)
        all_resource_ids = [cluster_id] + [c.resource_id for c in child_rows]
        resource_cost_map = self._aggregate_costs(
            ecosystem, tenant_id, all_resource_ids, period_start, period_end, group_by_column="resource_id"
        )
        ta_cost_map = self._aggregate_topic_attribution_costs(
            ecosystem, tenant_id, cluster_id, period_start, period_end
        )
        identity_cost_map = self._aggregate_identity_costs(
            ecosystem, tenant_id, cluster_id, identity_ids, period_start, period_end
        )

        def _topic_cost(r: ResourceTable) -> Decimal:
            return ta_cost_map.get(r.resource_id, resource_cost_map.get(r.resource_id, Decimal("0")))

        # Cluster node (always included)
        cluster_tags = self._tags.find_tags_for_entities(tenant_id, "resource", [cluster_id])
        cluster_node = GraphNodeData(
            id=cluster_id,
            resource_type=focus_row.resource_type,
            display_name=focus_row.display_name,
            cost=resource_cost_map.get(cluster_id, Decimal("0")),
            created_at=focus_row.created_at,
            deleted_at=focus_row.deleted_at,
            tags={t.tag_key: t.tag_value for t in cluster_tags.get(cluster_id, [])},
            parent_id=focus_row.parent_id,
            cloud=focus_row.cloud,
            region=focus_row.region,
            status=focus_row.status,
        )

        nodes: list[GraphNodeData] = [cluster_node]
        edges: list[GraphEdgeData] = []

        if expand == "topics":
            # All non-zero topics individually (up to cap), zero collapsed, identities as group
            sorted_topics = sorted(child_rows, key=_topic_cost, reverse=True)
            nonzero_topics = [r for r in sorted_topics if _topic_cost(r) > Decimal("0")]
            zero_topics = [r for r in sorted_topics if _topic_cost(r) == Decimal("0")]

            included_topics = nonzero_topics[:_CLUSTER_EXPAND_CAP]
            overflow_topics = nonzero_topics[_CLUSTER_EXPAND_CAP:]

            included_topic_ids = [r.resource_id for r in included_topics]
            topic_tags = (
                self._tags.find_tags_for_entities(tenant_id, "resource", included_topic_ids)
                if included_topic_ids
                else {}
            )

            for r in included_topics:
                nodes.append(
                    GraphNodeData(
                        id=r.resource_id,
                        resource_type=r.resource_type,
                        display_name=r.display_name,
                        cost=_topic_cost(r),
                        created_at=r.created_at,
                        deleted_at=r.deleted_at,
                        tags={t.tag_key: t.tag_value for t in topic_tags.get(r.resource_id, [])},
                        parent_id=r.parent_id,
                        cloud=r.cloud,
                        region=r.region,
                        status=r.status,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=r.parent_id or cluster_id,
                        target=r.resource_id,
                        relationship_type=EdgeType.parent,
                    )
                )

            included_topic_id_set = {r.resource_id for r in included_topics}
            for topic_id, cost in ta_cost_map.items():
                if topic_id in included_topic_id_set:
                    edges.append(
                        GraphEdgeData(
                            source=cluster_id,
                            target=topic_id,
                            relationship_type=EdgeType.attribution,
                            cost=cost,
                        )
                    )

            if overflow_topics:
                overflow_count = len(overflow_topics)
                overflow_cost = sum((_topic_cost(r) for r in overflow_topics), Decimal("0"))
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:capped_topics",
                        resource_type="capped_summary",
                        display_name=f"{overflow_count} more topics",
                        cost=overflow_cost,
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=overflow_count,
                        child_total_cost=overflow_cost,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:capped_topics",
                        relationship_type=EdgeType.parent,
                    )
                )

            if zero_topics:
                zero_count = len(zero_topics)
                display_name = f"{zero_count} others at $0" if nonzero_topics else f"{zero_count} topics ($0)"
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:zero_cost_topics",
                        resource_type="zero_cost_summary",
                        display_name=display_name,
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=zero_count,
                        child_total_cost=Decimal("0"),
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:zero_cost_topics",
                        relationship_type=EdgeType.parent,
                    )
                )

            if identity_ids:
                id_total = sum(identity_cost_map.values(), Decimal("0"))
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:identity_group",
                        resource_type="identity_group",
                        display_name=f"{len(identity_ids)} identities",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(identity_ids),
                        child_total_cost=id_total,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:identity_group",
                        relationship_type=EdgeType.charge,
                    )
                )

        elif expand == "identities":
            # Topics as group, non-zero identities individually (up to cap), zero collapsed
            if child_rows:
                topic_total = sum((_topic_cost(r) for r in child_rows), Decimal("0"))
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:topic_group",
                        resource_type="topic_group",
                        display_name=f"{len(child_rows)} topics",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(child_rows),
                        child_total_cost=topic_total,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:topic_group",
                        relationship_type=EdgeType.parent,
                    )
                )

            sorted_ids = sorted(
                identity_ids,
                key=lambda iid: identity_cost_map.get(iid, Decimal("0")),
                reverse=True,
            )
            nonzero_ids = [iid for iid in sorted_ids if identity_cost_map.get(iid, Decimal("0")) > Decimal("0")]
            zero_ids = [iid for iid in sorted_ids if identity_cost_map.get(iid, Decimal("0")) == Decimal("0")]

            included_ids = nonzero_ids[:_CLUSTER_EXPAND_CAP]
            overflow_ids = nonzero_ids[_CLUSTER_EXPAND_CAP:]

            id_rows: list[IdentityTable] = []
            if included_ids:
                id_where = _temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, at)
                id_where.append(col(IdentityTable.identity_id).in_(included_ids))
                id_rows = list(self._session.exec(select(IdentityTable).where(*id_where)).all())

            id_tags = (
                self._tags.find_tags_for_entities(tenant_id, "identity", [r.identity_id for r in id_rows])
                if id_rows
                else {}
            )
            cross_ref_map = self._fetch_cross_references(
                ecosystem, tenant_id, included_ids, cluster_id, period_start, period_end
            )

            for identity_row in id_rows:
                nodes.append(
                    GraphNodeData(
                        id=identity_row.identity_id,
                        resource_type=identity_row.identity_type,
                        display_name=identity_row.display_name,
                        cost=identity_cost_map.get(identity_row.identity_id, Decimal("0")),
                        created_at=identity_row.created_at,
                        deleted_at=identity_row.deleted_at,
                        tags={t.tag_key: t.tag_value for t in id_tags.get(identity_row.identity_id, [])},
                        parent_id=None,
                        cloud=None,
                        region=None,
                        status="active" if identity_row.deleted_at is None else "deleted",
                        cross_references=cross_ref_map.get(identity_row.identity_id, []),
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=identity_row.identity_id,
                        relationship_type=EdgeType.charge,
                        cost=identity_cost_map.get(identity_row.identity_id),
                    )
                )

            if overflow_ids:
                overflow_count = len(overflow_ids)
                overflow_cost = sum((identity_cost_map.get(iid, Decimal("0")) for iid in overflow_ids), Decimal("0"))
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:capped_identities",
                        resource_type="capped_summary",
                        display_name=f"{overflow_count} more identities",
                        cost=overflow_cost,
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=overflow_count,
                        child_total_cost=overflow_cost,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:capped_identities",
                        relationship_type=EdgeType.charge,
                    )
                )

            if zero_ids:
                zero_count = len(zero_ids)
                display_name = f"{zero_count} others at $0" if nonzero_ids else f"{zero_count} identities ($0)"
                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:zero_cost_identities",
                        resource_type="zero_cost_summary",
                        display_name=display_name,
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=zero_count,
                        child_total_cost=Decimal("0"),
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:zero_cost_identities",
                        relationship_type=EdgeType.charge,
                    )
                )

        else:
            # expand=None, at least one group oversized → grouped mode (per-group independent)
            if topics_oversized:
                sorted_topics = sorted(child_rows, key=_topic_cost, reverse=True)
                included_topics = sorted_topics[:_CLUSTER_TOP_N]
                topic_total = sum((_topic_cost(r) for r in child_rows), Decimal("0"))

                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:topic_group",
                        resource_type="topic_group",
                        display_name=f"{len(child_rows)} topics",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(child_rows),
                        child_total_cost=topic_total,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:topic_group",
                        relationship_type=EdgeType.parent,
                    )
                )

                included_topic_ids = [r.resource_id for r in included_topics]
                topic_tags = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", included_topic_ids)
                    if included_topic_ids
                    else {}
                )
                grouped_topic_id_set: set[str] = set(included_topic_ids)

                for r in included_topics:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_topic_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in topic_tags.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.parent_id or cluster_id,
                            target=r.resource_id,
                            relationship_type=EdgeType.parent,
                        )
                    )

                for topic_id, cost in ta_cost_map.items():
                    if topic_id in grouped_topic_id_set:
                        edges.append(
                            GraphEdgeData(
                                source=cluster_id,
                                target=topic_id,
                                relationship_type=EdgeType.attribution,
                                cost=cost,
                            )
                        )
            else:
                # Topics not oversized → return all individually using cached cost data
                all_child_ids = [r.resource_id for r in child_rows]
                topic_tags_all = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", all_child_ids) if all_child_ids else {}
                )
                all_child_id_set = set(all_child_ids)

                for r in child_rows:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_topic_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in topic_tags_all.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.parent_id or cluster_id,
                            target=r.resource_id,
                            relationship_type=EdgeType.parent,
                        )
                    )

                for topic_id, cost in ta_cost_map.items():
                    if topic_id in all_child_id_set:
                        edges.append(
                            GraphEdgeData(
                                source=cluster_id,
                                target=topic_id,
                                relationship_type=EdgeType.attribution,
                                cost=cost,
                            )
                        )

            if identities_oversized:
                sorted_ids = sorted(
                    identity_ids,
                    key=lambda iid: identity_cost_map.get(iid, Decimal("0")),
                    reverse=True,
                )
                included_ids = sorted_ids[:_CLUSTER_TOP_N]
                id_total = sum(identity_cost_map.values(), Decimal("0"))

                nodes.append(
                    GraphNodeData(
                        id=f"{cluster_id}:identity_group",
                        resource_type="identity_group",
                        display_name=f"{len(identity_ids)} identities",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=cluster_id,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(identity_ids),
                        child_total_cost=id_total,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=cluster_id,
                        target=f"{cluster_id}:identity_group",
                        relationship_type=EdgeType.charge,
                    )
                )

                id_rows_top: list[IdentityTable] = []
                if included_ids:
                    id_where = _temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, at)
                    id_where.append(col(IdentityTable.identity_id).in_(included_ids))
                    id_rows_top = list(self._session.exec(select(IdentityTable).where(*id_where)).all())

                id_tags_top = (
                    self._tags.find_tags_for_entities(tenant_id, "identity", [r.identity_id for r in id_rows_top])
                    if id_rows_top
                    else {}
                )
                xref_top = self._fetch_cross_references(
                    ecosystem, tenant_id, included_ids, cluster_id, period_start, period_end
                )

                for identity_row in id_rows_top:
                    nodes.append(
                        GraphNodeData(
                            id=identity_row.identity_id,
                            resource_type=identity_row.identity_type,
                            display_name=identity_row.display_name,
                            cost=identity_cost_map.get(identity_row.identity_id, Decimal("0")),
                            created_at=identity_row.created_at,
                            deleted_at=identity_row.deleted_at,
                            tags={t.tag_key: t.tag_value for t in id_tags_top.get(identity_row.identity_id, [])},
                            parent_id=None,
                            cloud=None,
                            region=None,
                            status="active" if identity_row.deleted_at is None else "deleted",
                            cross_references=xref_top.get(identity_row.identity_id, []),
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=cluster_id,
                            target=identity_row.identity_id,
                            relationship_type=EdgeType.charge,
                            cost=identity_cost_map.get(identity_row.identity_id),
                        )
                    )
            else:
                # Identities not oversized → return all individually using cached cost data
                id_rows_all: list[IdentityTable] = []
                if identity_ids:
                    id_where2 = _temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, at)
                    id_where2.append(col(IdentityTable.identity_id).in_(identity_ids))
                    id_rows_all = list(self._session.exec(select(IdentityTable).where(*id_where2)).all())

                id_tags_all = (
                    self._tags.find_tags_for_entities(tenant_id, "identity", [r.identity_id for r in id_rows_all])
                    if id_rows_all
                    else {}
                )
                xref_all = self._fetch_cross_references(
                    ecosystem, tenant_id, identity_ids, cluster_id, period_start, period_end
                )

                for identity_row in id_rows_all:
                    nodes.append(
                        GraphNodeData(
                            id=identity_row.identity_id,
                            resource_type=identity_row.identity_type,
                            display_name=identity_row.display_name,
                            cost=identity_cost_map.get(identity_row.identity_id, Decimal("0")),
                            created_at=identity_row.created_at,
                            deleted_at=identity_row.deleted_at,
                            tags={t.tag_key: t.tag_value for t in id_tags_all.get(identity_row.identity_id, [])},
                            parent_id=None,
                            cloud=None,
                            region=None,
                            status="active" if identity_row.deleted_at is None else "deleted",
                            cross_references=xref_all.get(identity_row.identity_id, []),
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=cluster_id,
                            target=identity_row.identity_id,
                            relationship_type=EdgeType.charge,
                            cost=identity_cost_map.get(identity_row.identity_id),
                        )
                    )

        return GraphNeighborhood(nodes=nodes, edges=edges)

    def _cluster_view_full(
        self,
        focus_row: ResourceTable,
        ecosystem: str,
        tenant_id: str,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
    ) -> GraphNeighborhood:
        """Return cluster + child topics + identities charged to cluster + charge edges (unfiltered)."""
        cluster_id = focus_row.resource_id

        # Child resources (topics, connectors, etc.) — temporal filter
        child_where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, at)
        child_where.append(col(ResourceTable.parent_id) == cluster_id)
        child_rows = self._session.exec(select(ResourceTable).where(*child_where)).all()

        # Identities charged to this cluster — look in chargeback_dimensions
        dim_stmt = (
            select(ChargebackDimensionTable.identity_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackDimensionTable.resource_id) == cluster_id,
                col(ChargebackDimensionTable.identity_id) != "",
            )
            .distinct()
        )
        identity_ids = list(self._session.exec(dim_stmt).all())

        # Identity records with temporal filter
        identity_rows: list[IdentityTable] = []
        if identity_ids:
            id_where = _temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, at)
            id_where.append(col(IdentityTable.identity_id).in_(identity_ids))
            identity_rows = list(self._session.exec(select(IdentityTable).where(*id_where)).all())

        # Cost aggregation for resources (cluster + topics)
        all_resource_ids = [cluster_id] + [c.resource_id for c in child_rows]
        resource_cost_map = self._aggregate_costs(
            ecosystem, tenant_id, all_resource_ids, period_start, period_end, group_by_column="resource_id"
        )

        # Charge cost per identity for this cluster (scoped to billing period)
        identity_cost_map = self._aggregate_identity_costs(
            ecosystem, tenant_id, cluster_id, identity_ids, period_start, period_end
        )

        # Cross-references: other resource_ids each identity is charged in (excluding this cluster).
        cross_ref_map = self._fetch_cross_references(
            ecosystem, tenant_id, identity_ids, cluster_id, period_start, period_end
        )

        # Tag resolution
        resource_tags = self._tags.find_tags_for_entities(tenant_id, "resource", all_resource_ids)
        identity_tags = self._tags.find_tags_for_entities(tenant_id, "identity", [r.identity_id for r in identity_rows])

        # Topic attribution costs — built before resource_nodes so costs inject directly
        ta_cost_map = self._aggregate_topic_attribution_costs(
            ecosystem, tenant_id, cluster_id, period_start, period_end
        )

        # Build nodes
        resource_nodes = [
            GraphNodeData(
                id=r.resource_id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                cost=ta_cost_map.get(r.resource_id, resource_cost_map.get(r.resource_id, Decimal("0"))),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                tags={t.tag_key: t.tag_value for t in resource_tags.get(r.resource_id, [])},
                parent_id=r.parent_id,
                cloud=r.cloud,
                region=r.region,
                status=r.status,
            )
            for r in [focus_row, *child_rows]
        ]
        identity_nodes = [
            GraphNodeData(
                id=r.identity_id,
                resource_type=r.identity_type,
                display_name=r.display_name,
                cost=identity_cost_map.get(r.identity_id, Decimal("0")),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                tags={t.tag_key: t.tag_value for t in identity_tags.get(r.identity_id, [])},
                parent_id=None,
                cloud=None,
                region=None,
                status="active" if r.deleted_at is None else "deleted",
                cross_references=cross_ref_map.get(r.identity_id, []),
            )
            for r in identity_rows
        ]

        # Build edges — all parent edges: parent → child direction
        parent_edges = [
            GraphEdgeData(
                source=r.parent_id or cluster_id,
                target=r.resource_id,
                relationship_type=EdgeType.parent,
            )
            for r in child_rows
        ]
        charge_edges = [
            GraphEdgeData(
                source=cluster_id,
                target=r.identity_id,
                relationship_type=EdgeType.charge,
                cost=identity_cost_map.get(r.identity_id),
            )
            for r in identity_rows
        ]

        # Attribution edges: cluster → topic (only for topics present in resource_nodes)
        resource_node_ids = {n.id for n in resource_nodes}
        attribution_edges = [
            GraphEdgeData(
                source=cluster_id,
                target=topic_id,
                relationship_type=EdgeType.attribution,
                cost=cost,
            )
            for topic_id, cost in ta_cost_map.items()
            if topic_id in resource_node_ids
        ]

        return GraphNeighborhood(
            nodes=[*resource_nodes, *identity_nodes],
            edges=[*parent_edges, *charge_edges, *attribution_edges],
        )

    # --- Identity view ---

    def _identity_view(  # noqa: C901
        self,
        focus_row: IdentityTable,
        ecosystem: str,
        tenant_id: str,
        at: datetime,
        period_start: datetime,
        period_end: datetime,
        expand: Literal["topics", "identities", "resources", "clusters"] | None = None,
        _force_full: bool = False,
    ) -> GraphNeighborhood:
        """Return identity node at center + all clusters it's charged in + charge edges."""
        identity_id = focus_row.identity_id

        # All resource_ids this identity is charged in (from chargeback_dimensions)
        cluster_stmt = (
            select(ChargebackDimensionTable.resource_id)
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                col(ChargebackDimensionTable.identity_id) == identity_id,
                col(ChargebackDimensionTable.resource_id).is_not(None),
                col(ChargebackDimensionTable.resource_id) != "",
            )
            .distinct()
        )
        cluster_ids = list(self._session.exec(cluster_stmt).all())

        # Fetch resource rows for those clusters (with temporal filter)
        cluster_rows: list[ResourceTable] = []
        if cluster_ids:
            where = _temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, at)
            where.append(col(ResourceTable.resource_id).in_(cluster_ids))
            cluster_rows = list(self._session.exec(select(ResourceTable).where(*where)).all())

        # Cost per cluster for this identity
        identity_cost_per_cluster: dict[str, Decimal] = {}
        if cluster_ids:
            cost_stmt = (
                select(
                    ChargebackDimensionTable.resource_id,
                    func.sum(cast(ChargebackFactTable.amount, String)).label("total"),
                )
                .join(
                    ChargebackFactTable,
                    col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id),
                )
                .where(
                    col(ChargebackDimensionTable.ecosystem) == ecosystem,
                    col(ChargebackDimensionTable.tenant_id) == tenant_id,
                    col(ChargebackDimensionTable.identity_id) == identity_id,
                    col(ChargebackDimensionTable.resource_id).in_(cluster_ids),
                    col(ChargebackFactTable.timestamp) >= period_start,
                    col(ChargebackFactTable.timestamp) < period_end,
                )
                .group_by(col(ChargebackDimensionTable.resource_id))
            )
            for row in self._session.exec(cost_stmt).all():
                if row[0] is not None:
                    identity_cost_per_cluster[row[0]] = Decimal(row[1] or "0")

        # Tag resolution for identity center node
        identity_tags = self._tags.find_tags_for_entities(tenant_id, "identity", [identity_id])

        # Build identity center node
        total_cost = sum(identity_cost_per_cluster.values(), Decimal("0"))
        identity_node = GraphNodeData(
            id=identity_id,
            resource_type=focus_row.identity_type,
            display_name=focus_row.display_name,
            cost=total_cost,
            created_at=focus_row.created_at,
            deleted_at=focus_row.deleted_at,
            tags={t.tag_key: t.tag_value for t in identity_tags.get(identity_id, [])},
            parent_id=None,
            cloud=None,
            region=None,
            status="active" if focus_row.deleted_at is None else "deleted",
        )

        # Grouping: apply when cluster count exceeds threshold (and not forced full)
        if not _force_full and len(cluster_rows) > _CLUSTER_GROUP_THRESHOLD:

            def _cluster_cost(r: ResourceTable) -> Decimal:
                return identity_cost_per_cluster.get(r.resource_id, Decimal("0"))

            nodes: list[GraphNodeData] = [identity_node]
            edges: list[GraphEdgeData] = []

            if expand == "clusters":
                # Sort by cost DESC, show up to cap non-zero individually, collapse zero-cost
                sorted_clusters = sorted(cluster_rows, key=_cluster_cost, reverse=True)
                nonzero_clusters = [r for r in sorted_clusters if _cluster_cost(r) > Decimal("0")]
                zero_clusters = [r for r in sorted_clusters if _cluster_cost(r) == Decimal("0")]

                included = nonzero_clusters[:_CLUSTER_EXPAND_CAP]
                overflow = nonzero_clusters[_CLUSTER_EXPAND_CAP:]

                included_ids = [r.resource_id for r in included]
                resource_tags = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", included_ids) if included_ids else {}
                )

                for r in included:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_cluster_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in resource_tags.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.resource_id,
                            target=identity_id,
                            relationship_type=EdgeType.charge,
                            cost=_cluster_cost(r),
                        )
                    )

                if overflow:
                    overflow_count = len(overflow)
                    overflow_cost = sum((_cluster_cost(r) for r in overflow), Decimal("0"))
                    nodes.append(
                        GraphNodeData(
                            id=f"{identity_id}:capped_clusters",
                            resource_type="capped_summary",
                            display_name=f"{overflow_count} more clusters",
                            cost=overflow_cost,
                            created_at=None,
                            deleted_at=None,
                            tags={},
                            parent_id=None,
                            cloud=None,
                            region=None,
                            status="active",
                            child_count=overflow_count,
                            child_total_cost=overflow_cost,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=f"{identity_id}:capped_clusters",
                            target=identity_id,
                            relationship_type=EdgeType.charge,
                        )
                    )

                if zero_clusters:
                    zero_count = len(zero_clusters)
                    nodes.append(
                        GraphNodeData(
                            id=f"{identity_id}:zero_cost_clusters",
                            resource_type="zero_cost_summary",
                            display_name=f"{zero_count} others at $0",
                            cost=Decimal("0"),
                            created_at=None,
                            deleted_at=None,
                            tags={},
                            parent_id=None,
                            cloud=None,
                            region=None,
                            status="active",
                            child_count=zero_count,
                            child_total_cost=Decimal("0"),
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=f"{identity_id}:zero_cost_clusters",
                            target=identity_id,
                            relationship_type=EdgeType.charge,
                        )
                    )

            else:
                # Grouped mode: cluster_group + top-N
                sorted_clusters = sorted(cluster_rows, key=_cluster_cost, reverse=True)
                included = sorted_clusters[:_CLUSTER_TOP_N]
                child_total_cost = sum((_cluster_cost(r) for r in cluster_rows), Decimal("0"))

                nodes.append(
                    GraphNodeData(
                        id=f"{identity_id}:cluster_group",
                        resource_type="cluster_group",
                        display_name=f"{len(cluster_rows)} clusters",
                        cost=Decimal("0"),
                        created_at=None,
                        deleted_at=None,
                        tags={},
                        parent_id=None,
                        cloud=None,
                        region=None,
                        status="active",
                        child_count=len(cluster_rows),
                        child_total_cost=child_total_cost,
                    )
                )
                edges.append(
                    GraphEdgeData(
                        source=f"{identity_id}:cluster_group",
                        target=identity_id,
                        relationship_type=EdgeType.charge,
                    )
                )

                included_ids = [r.resource_id for r in included]
                resource_tags = (
                    self._tags.find_tags_for_entities(tenant_id, "resource", included_ids) if included_ids else {}
                )

                for r in included:
                    nodes.append(
                        GraphNodeData(
                            id=r.resource_id,
                            resource_type=r.resource_type,
                            display_name=r.display_name,
                            cost=_cluster_cost(r),
                            created_at=r.created_at,
                            deleted_at=r.deleted_at,
                            tags={t.tag_key: t.tag_value for t in resource_tags.get(r.resource_id, [])},
                            parent_id=r.parent_id,
                            cloud=r.cloud,
                            region=r.region,
                            status=r.status,
                        )
                    )
                    edges.append(
                        GraphEdgeData(
                            source=r.resource_id,
                            target=identity_id,
                            relationship_type=EdgeType.charge,
                            cost=_cluster_cost(r),
                        )
                    )

            return GraphNeighborhood(nodes=nodes, edges=edges)

        # Passthrough: existing behavior (small count or _force_full)
        cluster_resource_ids = [r.resource_id for r in cluster_rows]
        resource_tags = self._tags.find_tags_for_entities(tenant_id, "resource", cluster_resource_ids)

        # Cluster nodes
        cluster_nodes = [
            GraphNodeData(
                id=r.resource_id,
                resource_type=r.resource_type,
                display_name=r.display_name,
                cost=identity_cost_per_cluster.get(r.resource_id, Decimal("0")),
                created_at=r.created_at,
                deleted_at=r.deleted_at,
                tags={t.tag_key: t.tag_value for t in resource_tags.get(r.resource_id, [])},
                parent_id=r.parent_id,
                cloud=r.cloud,
                region=r.region,
                status=r.status,
            )
            for r in cluster_rows
        ]

        # Charge edges: cluster → identity (same direction as _cluster_view)
        charge_edges = [
            GraphEdgeData(
                source=r.resource_id,
                target=identity_id,
                relationship_type=EdgeType.charge,
                cost=identity_cost_per_cluster.get(r.resource_id),
            )
            for r in cluster_rows
        ]

        return GraphNeighborhood(nodes=[identity_node, *cluster_nodes], edges=charge_edges)

    # --- Cost aggregation helper ---

    def _aggregate_costs(
        self,
        ecosystem: str,
        tenant_id: str,
        ids: list[str],
        period_start: datetime,
        period_end: datetime,
        *,
        group_by_column: Literal["env_id", "resource_id"],
    ) -> dict[str, Decimal]:
        """Sum chargeback_facts.amount grouped by the specified dimension column.

        group_by_column="env_id"      → used by _root_view (environments billed via env_id)
        group_by_column="resource_id" → used by _resource_view and _cluster_view
        """
        if not ids:
            return {}
        dim_col = (
            col(ChargebackDimensionTable.env_id)
            if group_by_column == "env_id"
            else col(ChargebackDimensionTable.resource_id)  # type: ignore[arg-type]  # resource_id is str | None; col() handles nullable columns at runtime
        )
        stmt = (
            select(
                dim_col,
                func.sum(cast(ChargebackFactTable.amount, String)).label("total"),
            )
            .join(
                ChargebackFactTable,
                col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id),
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                dim_col.in_(ids),
                col(ChargebackFactTable.timestamp) >= period_start,
                col(ChargebackFactTable.timestamp) < period_end,
            )
            .group_by(dim_col)
        )
        return {row[0]: Decimal(row[1] or "0") for row in self._session.exec(stmt).all() if row[0] is not None}

    def search_entities(
        self,
        ecosystem: str,
        tenant_id: str,
        query: str,
    ) -> list[GraphSearchResultData]:
        """Search resources and identities by partial name/id match. Returns ≤20 results."""
        pattern = f"%{query}%"

        resource_where = [
            col(ResourceTable.ecosystem) == ecosystem,
            col(ResourceTable.tenant_id) == tenant_id,
            or_(
                col(ResourceTable.resource_id).ilike(pattern),
                col(ResourceTable.display_name).ilike(pattern),
            ),
        ]
        resource_rows = self._session.exec(select(ResourceTable).where(*resource_where).limit(200)).all()

        # Batch-resolve parent display names for resource results
        parent_ids = [res.parent_id for res in resource_rows if res.parent_id is not None]
        parent_names: dict[str, str | None] = {}
        if parent_ids:
            parent_rows = self._session.exec(
                select(col(ResourceTable.resource_id), col(ResourceTable.display_name)).where(
                    col(ResourceTable.ecosystem) == ecosystem,
                    col(ResourceTable.tenant_id) == tenant_id,
                    col(ResourceTable.resource_id).in_(parent_ids),
                )
            ).all()
            parent_names = {row[0]: row[1] for row in parent_rows}

        identity_where = [
            col(IdentityTable.ecosystem) == ecosystem,
            col(IdentityTable.tenant_id) == tenant_id,
            or_(
                col(IdentityTable.identity_id).ilike(pattern),
                col(IdentityTable.display_name).ilike(pattern),
            ),
        ]
        identity_rows = self._session.exec(select(IdentityTable).where(*identity_where).limit(200)).all()

        results: list[tuple[int, GraphSearchResultData]] = []

        for res in resource_rows:
            results.append(
                (
                    _relevance_score(res.resource_id, res.display_name, query),
                    GraphSearchResultData(
                        id=res.resource_id,
                        resource_type=res.resource_type,
                        display_name=res.display_name,
                        parent_id=res.parent_id,
                        status=res.status,
                        parent_display_name=parent_names.get(res.parent_id) if res.parent_id else None,
                    ),
                )
            )

        for idt in identity_rows:
            results.append(
                (
                    _relevance_score(idt.identity_id, idt.display_name, query),
                    GraphSearchResultData(
                        id=idt.identity_id,
                        resource_type=idt.identity_type,
                        display_name=idt.display_name,
                        parent_id=None,
                        status="active" if idt.deleted_at is None else "deleted",
                    ),
                )
            )

        results.sort(key=lambda x: x[0])
        return [item for _, item in results[:20]]

    def diff_neighborhood(
        self,
        ecosystem: str,
        tenant_id: str,
        focus_id: str | None,
        depth: int,
        from_start: datetime,
        from_end: datetime,
        to_start: datetime,
        to_end: datetime,
    ) -> list[GraphDiffNodeData]:
        """Reuses find_neighborhood for both windows, merges by entity ID."""
        before = self.find_neighborhood(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            focus_id=focus_id,
            depth=depth,
            at=from_end,
            period_start=from_start,
            period_end=from_end,
            _force_full=True,
        )
        after = self.find_neighborhood(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            focus_id=focus_id,
            depth=depth,
            at=to_end,
            period_start=to_start,
            period_end=to_end,
            _force_full=True,
        )

        before_map: dict[str, GraphNodeData] = {n.id: n for n in before.nodes}
        after_map: dict[str, GraphNodeData] = {n.id: n for n in after.nodes}
        all_ids = set(before_map) | set(after_map)

        diff: list[GraphDiffNodeData] = []
        for eid in all_ids:
            b = before_map.get(eid)
            a = after_map.get(eid)
            cost_before = b.cost if b else Decimal("0")
            cost_after = a.cost if a else Decimal("0")
            cost_delta = cost_after - cost_before

            if b and a:
                status = "unchanged" if cost_delta == Decimal("0") else "changed"
                pct_change = (cost_delta / cost_before * 100) if cost_before != Decimal("0") else None
            elif a:
                status = "new"
                pct_change = None
            else:
                status = "deleted"
                pct_change = None

            representative = before_map.get(eid) or after_map[eid]
            diff.append(
                GraphDiffNodeData(
                    id=eid,
                    resource_type=representative.resource_type,
                    display_name=representative.display_name,
                    parent_id=representative.parent_id,
                    cost_before=cost_before,
                    cost_after=cost_after,
                    cost_delta=cost_delta,
                    pct_change=pct_change,
                    status=status,
                )
            )

        return diff

    def get_timeline(
        self,
        ecosystem: str,
        tenant_id: str,
        entity_id: str,
        start: datetime,
        end: datetime,
    ) -> list[GraphTimelineData]:
        """Daily cost series with gap filling for missing dates."""
        resource_row = self._session.get(ResourceTable, (ecosystem, tenant_id, entity_id))
        if resource_row is not None:
            if resource_row.resource_type == "topic":
                raw = self._timeline_topic_attribution(ecosystem, tenant_id, entity_id, start, end)
            elif resource_row.resource_type == "environment":
                raw = self._timeline_chargeback(ecosystem, tenant_id, entity_id, start, end, group_by="env_id")
            else:
                raw = self._timeline_chargeback(ecosystem, tenant_id, entity_id, start, end, group_by="resource_id")
        else:
            id_where = [
                col(IdentityTable.ecosystem) == ecosystem,
                col(IdentityTable.tenant_id) == tenant_id,
                col(IdentityTable.identity_id) == entity_id,
            ]
            identity_row = self._session.exec(select(IdentityTable).where(*id_where)).first()
            if identity_row is None:
                raise KeyError(f"Entity {entity_id!r} not found for tenant {tenant_id!r}")
            raw = self._timeline_chargeback(ecosystem, tenant_id, entity_id, start, end, group_by="identity_id")

        return self._fill_timeline_gaps(raw, start, end)

    def _timeline_chargeback(
        self,
        ecosystem: str,
        tenant_id: str,
        entity_id: str,
        start: datetime,
        end: datetime,
        *,
        group_by: Literal["env_id", "resource_id", "identity_id"],
    ) -> dict[date, Decimal]:
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable

        if group_by == "identity_id":
            dim_col = col(ChargebackDimensionTable.identity_id)
        elif group_by == "env_id":
            dim_col = col(ChargebackDimensionTable.env_id)
        else:
            dim_col = col(ChargebackDimensionTable.resource_id)  # type: ignore[arg-type]  # resource_id is nullable in schema but never NULL in practice for resource-grouped rows

        date_expr = func.date(ChargebackFactTable.timestamp).label("day")
        stmt = (
            select(date_expr, func.sum(cast(ChargebackFactTable.amount, String)).label("total"))
            .select_from(ChargebackDimensionTable)
            .join(
                ChargebackFactTable, col(ChargebackFactTable.dimension_id) == col(ChargebackDimensionTable.dimension_id)
            )
            .where(
                col(ChargebackDimensionTable.ecosystem) == ecosystem,
                col(ChargebackDimensionTable.tenant_id) == tenant_id,
                dim_col == entity_id,
                col(ChargebackFactTable.timestamp) >= start,
                col(ChargebackFactTable.timestamp) < end,
            )
            .group_by(date_expr)
        )
        return {
            date.fromisoformat(row[0]) if isinstance(row[0], str) else row[0]: Decimal(row[1] or "0")
            for row in self._session.exec(stmt).all()
        }

    def _timeline_topic_attribution(
        self,
        ecosystem: str,
        tenant_id: str,
        entity_id: str,
        start: datetime,
        end: datetime,
    ) -> dict[date, Decimal]:
        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable, TopicAttributionFactTable

        date_expr = func.date(TopicAttributionFactTable.timestamp).label("day")
        stmt = (
            select(date_expr, func.sum(cast(TopicAttributionFactTable.amount, String)).label("total"))
            .select_from(TopicAttributionDimensionTable)
            .join(
                TopicAttributionFactTable,
                col(TopicAttributionFactTable.dimension_id) == col(TopicAttributionDimensionTable.dimension_id),
            )
            .where(
                col(TopicAttributionDimensionTable.ecosystem) == ecosystem,
                col(TopicAttributionDimensionTable.tenant_id) == tenant_id,
                col(TopicAttributionDimensionTable.resource_id) == entity_id,
                col(TopicAttributionFactTable.timestamp) >= start,
                col(TopicAttributionFactTable.timestamp) < end,
            )
            .group_by(date_expr)
        )
        return {
            date.fromisoformat(row[0]) if isinstance(row[0], str) else row[0]: Decimal(row[1] or "0")
            for row in self._session.exec(stmt).all()
        }

    def _fill_timeline_gaps(
        self,
        raw: dict[date, Decimal],
        start: datetime,
        end: datetime,
    ) -> list[GraphTimelineData]:
        """Generate one entry per calendar day in [start.date(), end.date()).
        Days with no billing data receive cost=0.
        """
        start_date = start.date()
        return [
            GraphTimelineData(date=d, cost=raw.get(d, Decimal("0")))
            for i in range((end.date() - start_date).days)
            for d in (start_date + timedelta(days=i),)
        ]


def _relevance_score(primary: str, display: str | None, query: str) -> int:
    """Score a search candidate: 0=exact, 1=prefix, 2=substring (case-insensitive)."""
    q = query.lower()
    p = primary.lower()
    d = (display or "").lower()
    if p == q or d == q:
        return 0
    if p.startswith(q) or d.startswith(q):
        return 1
    return 2
