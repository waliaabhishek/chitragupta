from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, or_
from sqlmodel import Session, col, select

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.chargeback import ChargebackRow, CustomTag
    from core.models.identity import Identity
    from core.models.pipeline import PipelineState
    from core.models.resource import Resource

from core.storage.backends.sqlmodel.mappers import (
    billing_to_domain,
    billing_to_table,
    chargeback_to_dimension,
    chargeback_to_domain,
    chargeback_to_fact,
    identity_to_domain,
    identity_to_table,
    pipeline_state_to_domain,
    pipeline_state_to_table,
    resource_to_domain,
    resource_to_table,
    tag_to_domain,
    tag_to_table,
)
from core.storage.backends.sqlmodel.tables import (
    BillingTable,
    ChargebackDimensionTable,
    ChargebackFactTable,
    CustomTagTable,
    IdentityTable,
    PipelineStateTable,
    ResourceTable,
)


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
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, resource: Resource) -> Resource:
        table_obj = resource_to_table(resource)
        merged = self._session.merge(table_obj)
        self._session.flush()
        return resource_to_domain(merged)

    def get(self, ecosystem: str, tenant_id: str, resource_id: str) -> Resource | None:
        row = self._session.get(ResourceTable, (ecosystem, tenant_id, resource_id))
        return resource_to_domain(row) if row else None

    def find_active_at(self, ecosystem: str, tenant_id: str, timestamp: datetime) -> list[Resource]:
        stmt = select(ResourceTable).where(*_temporal_active_at_filter(ResourceTable, ecosystem, tenant_id, timestamp))
        return [resource_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_period(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[Resource]:
        stmt = select(ResourceTable).where(*_temporal_by_period_filter(ResourceTable, ecosystem, tenant_id, start, end))
        return [resource_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_type(self, ecosystem: str, tenant_id: str, resource_type: str) -> list[Resource]:
        stmt = select(ResourceTable).where(
            col(ResourceTable.ecosystem) == ecosystem,
            col(ResourceTable.tenant_id) == tenant_id,
            col(ResourceTable.resource_type) == resource_type,
        )
        return [resource_to_domain(r) for r in self._session.exec(stmt).all()]

    def mark_deleted(self, ecosystem: str, tenant_id: str, resource_id: str, deleted_at: datetime) -> None:
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
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, identity: Identity) -> Identity:
        table_obj = identity_to_table(identity)
        merged = self._session.merge(table_obj)
        self._session.flush()
        return identity_to_domain(merged)

    def get(self, ecosystem: str, tenant_id: str, identity_id: str) -> Identity | None:
        row = self._session.get(IdentityTable, (ecosystem, tenant_id, identity_id))
        return identity_to_domain(row) if row else None

    def find_active_at(self, ecosystem: str, tenant_id: str, timestamp: datetime) -> list[Identity]:
        stmt = select(IdentityTable).where(*_temporal_active_at_filter(IdentityTable, ecosystem, tenant_id, timestamp))
        return [identity_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_period(self, ecosystem: str, tenant_id: str, start: datetime, end: datetime) -> list[Identity]:
        stmt = select(IdentityTable).where(*_temporal_by_period_filter(IdentityTable, ecosystem, tenant_id, start, end))
        return [identity_to_domain(r) for r in self._session.exec(stmt).all()]

    def find_by_type(self, ecosystem: str, tenant_id: str, identity_type: str) -> list[Identity]:
        stmt = select(IdentityTable).where(
            col(IdentityTable.ecosystem) == ecosystem,
            col(IdentityTable.tenant_id) == tenant_id,
            col(IdentityTable.identity_type) == identity_type,
        )
        return [identity_to_domain(r) for r in self._session.exec(stmt).all()]

    def mark_deleted(self, ecosystem: str, tenant_id: str, identity_id: str, deleted_at: datetime) -> None:
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


class SQLModelBillingRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, line: BillingLineItem) -> BillingLineItem:
        table_obj = billing_to_table(line)
        merged = self._session.merge(table_obj)
        self._session.flush()
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

    def increment_allocation_attempts(
        self,
        ecosystem: str,
        tenant_id: str,
        timestamp: datetime,
        resource_id: str,
        product_type: str,
    ) -> int:
        row = self._session.get(BillingTable, (ecosystem, tenant_id, timestamp, resource_id, product_type))
        if row is None:
            msg = (
                f"Billing line not found: ecosystem={ecosystem!r}, tenant_id={tenant_id!r}, "
                f"timestamp={timestamp!r}, resource_id={resource_id!r}, product_type={product_type!r}"
            )
            raise KeyError(msg)
        row.allocation_attempts += 1
        self._session.add(row)
        self._session.flush()
        return row.allocation_attempts

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

    def _get_or_create_dimension(self, row: ChargebackRow) -> int:
        """Get existing dimension by UQ columns, or create a new one."""
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
            assert existing.dimension_id is not None  # auto-incremented PK is always set after flush
            return existing.dimension_id

        dim = chargeback_to_dimension(row)
        self._session.add(dim)
        self._session.flush()
        assert dim.dimension_id is not None  # auto-incremented PK is always set after flush
        return dim.dimension_id

    def upsert(self, row: ChargebackRow) -> ChargebackRow:
        dimension_id = self._get_or_create_dimension(row)
        fact = chargeback_to_fact(row, dimension_id)
        merged = self._session.merge(fact)
        self._session.flush()
        dim = self._session.get(ChargebackDimensionTable, dimension_id)
        assert dim is not None  # dimension was just created/fetched
        return chargeback_to_domain(dim, merged)

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
        # Get dimension IDs for this ecosystem+tenant
        dim_stmt = select(ChargebackDimensionTable.dimension_id).where(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
        )
        dim_ids = list(self._session.exec(dim_stmt).all())
        if not dim_ids:
            return 0
        # Bulk delete facts in the date range for those dimensions
        fact_del = delete(ChargebackFactTable).where(
            col(ChargebackFactTable.dimension_id).in_(dim_ids),
            col(ChargebackFactTable.timestamp) >= start,
            col(ChargebackFactTable.timestamp) < end,
        )
        result = self._session.execute(fact_del)
        self._session.flush()
        return result.rowcount  # type: ignore[attr-defined, no-any-return]  # CursorResult always has rowcount

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


# --- PipelineStateRepository ---


class SQLModelPipelineStateRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def upsert(self, state: PipelineState) -> PipelineState:
        table_obj = pipeline_state_to_table(state)
        merged = self._session.merge(table_obj)
        self._session.flush()
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
        row = self._session.get(PipelineStateTable, (ecosystem, tenant_id, tracking_date))
        if row:
            row.billing_gathered = True
            self._session.add(row)
            self._session.flush()

    def mark_resources_gathered(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        row = self._session.get(PipelineStateTable, (ecosystem, tenant_id, tracking_date))
        if row:
            row.resources_gathered = True
            self._session.add(row)
            self._session.flush()

    def mark_needs_recalculation(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        row = self._session.get(PipelineStateTable, (ecosystem, tenant_id, tracking_date))
        if row:
            row.chargeback_calculated = False
            self._session.add(row)
            self._session.flush()

    def mark_chargeback_calculated(self, ecosystem: str, tenant_id: str, tracking_date: date) -> None:
        row = self._session.get(PipelineStateTable, (ecosystem, tenant_id, tracking_date))
        if row:
            row.chargeback_calculated = True
            self._session.add(row)
            self._session.flush()


# --- TagRepository ---


class SQLModelTagRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def add_tag(self, dimension_id: int, tag_key: str, tag_value: str, created_by: str) -> CustomTag:
        from core.models.chargeback import CustomTag as CustomTagDomain

        domain_tag = CustomTagDomain(
            tag_id=None,
            dimension_id=dimension_id,
            tag_key=tag_key,
            tag_value=tag_value,
            created_by=created_by,
        )
        row = tag_to_table(domain_tag)
        self._session.add(row)
        self._session.flush()
        return tag_to_domain(row)

    def get_tags(self, dimension_id: int) -> list[CustomTag]:
        stmt = select(CustomTagTable).where(col(CustomTagTable.dimension_id) == dimension_id)
        return [tag_to_domain(r) for r in self._session.exec(stmt).all()]

    def delete_tag(self, tag_id: int) -> None:
        row = self._session.get(CustomTagTable, tag_id)
        if row:
            self._session.delete(row)
            self._session.flush()
