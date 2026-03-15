from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.chargeback import AllocationDetail, CostType
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

try:
    from core.models.chargeback import AllocationIssueRow
except ImportError:
    AllocationIssueRow = None  # type: ignore[assignment,misc]  # not yet implemented — red
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
)


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


def _insert_row(
    session: Session,
    *,
    ecosystem: str = "eco",
    tenant_id: str = "t1",
    resource_id: str | None = "r1",
    product_type: str = "kafka",
    identity_id: str = "sa-1",
    cost_type: str = CostType.USAGE.value,
    allocation_detail: str | None,
    timestamp: datetime = datetime(2026, 1, 15, tzinfo=UTC),
    amount: str = "10.00",
) -> None:
    """Insert a dimension + fact pair directly into the session."""
    # Reuse existing dimension if it already exists for this combination
    from sqlmodel import col, select

    existing_dim = session.exec(
        select(ChargebackDimensionTable).where(
            col(ChargebackDimensionTable.ecosystem) == ecosystem,
            col(ChargebackDimensionTable.tenant_id) == tenant_id,
            col(ChargebackDimensionTable.resource_id) == resource_id,
            col(ChargebackDimensionTable.product_type) == product_type,
            col(ChargebackDimensionTable.identity_id) == identity_id,
            col(ChargebackDimensionTable.cost_type) == cost_type,
            col(ChargebackDimensionTable.allocation_detail) == allocation_detail,
        )
    ).first()

    if existing_dim is None:
        dim = ChargebackDimensionTable(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            resource_id=resource_id,
            product_category="compute",
            product_type=product_type,
            identity_id=identity_id,
            cost_type=cost_type,
            allocation_method="direct",
            allocation_detail=allocation_detail,
        )
        session.add(dim)
        session.flush()
        dim_id = dim.dimension_id
    else:
        dim_id = existing_dim.dimension_id

    fact = ChargebackFactTable(
        dimension_id=dim_id,
        timestamp=timestamp,
        amount=amount,
        tags_json="[]",
    )
    session.add(fact)
    session.flush()


class TestFindAllocationIssuesFiltering:
    def test_returns_only_failure_code_rows(self, session: Session) -> None:
        """Success codes are excluded; only failure codes are returned."""
        # success rows — should be excluded
        _insert_row(session, allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION.value, amount="5.00")
        _insert_row(
            session,
            identity_id="sa-2",
            allocation_detail=AllocationDetail.EVEN_SPLIT_ALLOCATION.value,
            amount="8.00",
        )
        # failure row — should be returned
        _insert_row(
            session,
            identity_id="sa-3",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            amount="20.00",
        )
        # NULL allocation_detail — should be excluded
        _insert_row(session, identity_id="sa-4", allocation_detail=None, amount="3.00")
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1")

        assert total == 1
        assert len(items) == 1
        issue: AllocationIssueRow = items[0]
        assert issue.allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED.value
        assert issue.identity_id == "sa-3"

    def test_groups_and_sums_correctly(self, session: Session) -> None:
        """Multiple facts with the same dimension group are summed into a single row."""
        _insert_row(
            session,
            identity_id="sa-1",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            cost_type=CostType.USAGE.value,
            amount="15.00",
        )
        _insert_row(
            session,
            identity_id="sa-1",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 16, tzinfo=UTC),
            cost_type=CostType.USAGE.value,
            amount="10.00",
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1")

        assert total == 1
        issue: AllocationIssueRow = items[0]
        assert issue.usage_cost == Decimal("25.00")
        assert issue.row_count == 2

    def test_separates_usage_and_shared_costs(self, session: Session) -> None:
        """usage_cost and shared_cost are tracked separately."""
        _insert_row(
            session,
            identity_id="sa-1",
            cost_type=CostType.USAGE.value,
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            amount="30.00",
        )
        _insert_row(
            session,
            identity_id="sa-1",
            cost_type=CostType.SHARED.value,
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 16, tzinfo=UTC),
            amount="10.00",
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1")

        assert total == 1
        issue = items[0]
        assert issue.usage_cost == Decimal("30.00")
        assert issue.shared_cost == Decimal("10.00")
        assert issue.total_cost == Decimal("40.00")

    def test_ordered_by_total_cost_desc(self, session: Session) -> None:
        """Results are ordered by total_cost descending."""
        _insert_row(
            session,
            identity_id="sa-cheap",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            amount="5.00",
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
        )
        _insert_row(
            session,
            identity_id="sa-expensive",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            amount="100.00",
            timestamp=datetime(2026, 1, 16, tzinfo=UTC),
        )
        _insert_row(
            session,
            identity_id="sa-mid",
            allocation_detail=AllocationDetail.NO_METRICS_LOCATED.value,
            amount="50.00",
            timestamp=datetime(2026, 1, 17, tzinfo=UTC),
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1")

        assert total == 3
        assert items[0].total_cost >= items[1].total_cost >= items[2].total_cost


class TestFindAllocationIssuesDateFilters:
    def test_respects_start_filter(self, session: Session) -> None:
        """Rows before start date are excluded."""
        _insert_row(
            session,
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 10, tzinfo=UTC),
            amount="10.00",
        )
        _insert_row(
            session,
            identity_id="sa-2",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 20, tzinfo=UTC),
            amount="20.00",
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(
            ecosystem="eco",
            tenant_id="t1",
            start=datetime(2026, 1, 15, tzinfo=UTC),
        )

        assert total == 1
        assert items[0].identity_id == "sa-2"

    def test_respects_end_filter(self, session: Session) -> None:
        """Rows at or after end date are excluded."""
        _insert_row(
            session,
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 10, tzinfo=UTC),
            amount="10.00",
        )
        _insert_row(
            session,
            identity_id="sa-2",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            timestamp=datetime(2026, 1, 20, tzinfo=UTC),
            amount="20.00",
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(
            ecosystem="eco",
            tenant_id="t1",
            end=datetime(2026, 1, 15, tzinfo=UTC),
        )

        assert total == 1
        assert items[0].identity_id == "sa-1"

    def test_respects_identity_id_filter(self, session: Session) -> None:
        """Only rows matching identity_id are returned."""
        for uid in ["sa-a", "sa-b", "sa-c"]:
            _insert_row(
                session,
                identity_id=uid,
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 1, 15, tzinfo=UTC),
                amount="10.00",
            )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="sa-b",
        )

        assert total == 1
        assert items[0].identity_id == "sa-b"

    def test_respects_product_type_filter(self, session: Session) -> None:
        """Only rows matching product_type are returned."""
        for pt in ["kafka", "connector", "ksqldb"]:
            _insert_row(
                session,
                identity_id=f"sa-{pt}",
                product_type=pt,
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 1, 15, tzinfo=UTC),
                amount="10.00",
            )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(
            ecosystem="eco",
            tenant_id="t1",
            product_type="connector",
        )

        assert total == 1
        assert items[0].product_type == "connector"

    def test_respects_resource_id_filter(self, session: Session) -> None:
        """Only rows matching resource_id are returned."""
        for rid in ["r-1", "r-2"]:
            _insert_row(
                session,
                resource_id=rid,
                identity_id=f"sa-{rid}",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 1, 15, tzinfo=UTC),
                amount="10.00",
            )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r-2",
        )

        assert total == 1
        assert items[0].resource_id == "r-2"


class TestFindAllocationIssuesPagination:
    def test_pagination_offset_and_total(self, session: Session) -> None:
        """offset > 0 returns correct slice while total reflects full result count."""
        for i in range(5):
            _insert_row(
                session,
                identity_id=f"sa-{i}",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 1, 15 + i, tzinfo=UTC),
                amount=str(100 - i * 10),
            )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items_page1, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1", limit=2, offset=0)
        items_page2, total2 = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1", limit=2, offset=2)

        assert total == 5
        assert total2 == 5
        assert len(items_page1) == 2
        assert len(items_page2) == 2
        # Pages must be non-overlapping
        ids_p1 = {i.identity_id for i in items_page1}
        ids_p2 = {i.identity_id for i in items_page2}
        assert ids_p1.isdisjoint(ids_p2)

    def test_pagination_limit_zero_offset(self, session: Session) -> None:
        """limit=1 returns only first row."""
        for i in range(3):
            _insert_row(
                session,
                identity_id=f"sa-{i}",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
                timestamp=datetime(2026, 1, 15 + i, tzinfo=UTC),
                amount=str(50 + i),
            )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1", limit=1, offset=0)

        assert total == 3
        assert len(items) == 1

    def test_empty_when_no_failure_rows(self, session: Session) -> None:
        """Returns empty list and total=0 when all rows have success codes."""
        _insert_row(
            session,
            allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION.value,
            amount="10.00",
        )
        session.commit()

        repo = SQLModelChargebackRepository(session)
        items, total = repo.find_allocation_issues(ecosystem="eco", tenant_id="t1")

        assert items == []
        assert total == 0
