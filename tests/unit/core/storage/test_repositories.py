from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from core.models.billing import BillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import Identity
from core.models.pipeline import PipelineState
from core.models.resource import Resource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import (
    SQLModelBillingRepository,
    SQLModelChargebackRepository,
    SQLModelIdentityRepository,
    SQLModelPipelineStateRepository,
    SQLModelResourceRepository,
    SQLModelTagRepository,
)


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


# --- Resource Repository ---


class TestResourceRepository:
    def _make_resource(self, **overrides: Any) -> Resource:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 10, tzinfo=UTC),
            metadata={"cloud": "aws"},
        )
        defaults.update(overrides)
        return Resource(**defaults)

    def test_upsert_and_get(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        r = self._make_resource()
        repo.upsert(r)
        session.commit()
        got = repo.get("eco", "t1", "r1")
        assert got is not None
        assert got.resource_type == "kafka"
        assert got.metadata["cloud"] == "aws"

    def test_get_nonexistent(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        assert repo.get("eco", "t1", "nope") is None

    def test_upsert_updates_existing(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        r = self._make_resource()
        repo.upsert(r)
        session.commit()
        r2 = self._make_resource(display_name="Updated")
        repo.upsert(r2)
        session.commit()
        got = repo.get("eco", "t1", "r1")
        assert got is not None
        assert got.display_name == "Updated"

    def test_find_active_at(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        # Resource created Jan 10, active at Jan 15
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 10, tzinfo=UTC)))
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_active_at_before_creation(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 10, tzinfo=UTC)))
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC))
        assert len(results) == 0

    def test_find_active_at_deleted_before(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 10, tzinfo=UTC),
            )
        )
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 0

    def test_find_active_at_null_created(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=None))
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        assert len(results) == 1

    def test_find_active_at_null_deleted(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=None,
            )
        )
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 6, 1, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_period(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 5, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
            )
        )
        session.commit()
        # Period [Jan 10, Jan 15) — resource overlaps
        results = repo.find_by_period("eco", "t1", datetime(2026, 1, 10, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_period_created_after(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 2, 1, tzinfo=UTC)))
        session.commit()
        results = repo.find_by_period("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC))
        assert len(results) == 0

    def test_find_by_period_deleted_before_start(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2025, 12, 1, tzinfo=UTC),
                deleted_at=datetime(2025, 12, 31, tzinfo=UTC),
            )
        )
        session.commit()
        results = repo.find_by_period("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC))
        assert len(results) == 0

    def test_find_by_period_created_and_deleted_within(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 5, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 10, tzinfo=UTC),
            )
        )
        session.commit()
        results = repo.find_by_period("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_type(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", resource_type="kafka"))
        repo.upsert(self._make_resource(resource_id="r2", resource_type="ksql"))
        session.commit()
        results = repo.find_by_type("eco", "t1", "kafka")
        assert len(results) == 1
        assert results[0].resource_id == "r1"

    def test_mark_deleted(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource())
        session.commit()
        repo.mark_deleted("eco", "t1", "r1", datetime(2026, 2, 1, tzinfo=UTC))
        session.commit()
        got = repo.get("eco", "t1", "r1")
        assert got is not None
        assert got.deleted_at == datetime(2026, 2, 1, tzinfo=UTC)
        assert got.status == ResourceStatus.DELETED

    def test_delete_before(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                resource_id="old",
                deleted_at=datetime(2025, 1, 1, tzinfo=UTC),
            )
        )
        repo.upsert(self._make_resource(resource_id="recent"))
        session.commit()
        count = repo.delete_before("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        session.commit()
        assert count == 1
        assert repo.get("eco", "t1", "old") is None
        assert repo.get("eco", "t1", "recent") is not None


# --- Identity Repository ---


class TestIdentityRepository:
    def _make_identity(self, **overrides: Any) -> Identity:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="u1",
            identity_type="user",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        defaults.update(overrides)
        return Identity(**defaults)

    def test_upsert_and_get(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity())
        session.commit()
        got = repo.get("eco", "t1", "u1")
        assert got is not None
        assert got.identity_type == "user"

    def test_find_active_at(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(created_at=datetime(2026, 1, 1, tzinfo=UTC)))
        session.commit()
        results = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_period(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(
            self._make_identity(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
            )
        )
        session.commit()
        results = repo.find_by_period("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_type(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(identity_id="u1", identity_type="user"))
        repo.upsert(self._make_identity(identity_id="sa1", identity_type="service_account"))
        session.commit()
        results = repo.find_by_type("eco", "t1", "service_account")
        assert len(results) == 1

    def test_mark_deleted(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity())
        session.commit()
        repo.mark_deleted("eco", "t1", "u1", datetime(2026, 2, 1, tzinfo=UTC))
        session.commit()
        got = repo.get("eco", "t1", "u1")
        assert got is not None
        assert got.deleted_at is not None

    def test_delete_before(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(identity_id="old", deleted_at=datetime(2025, 1, 1, tzinfo=UTC)))
        repo.upsert(self._make_identity(identity_id="recent"))
        session.commit()
        count = repo.delete_before("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        session.commit()
        assert count == 1


# --- Billing Repository ---


class TestBillingRepository:
    def _make_billing(self, **overrides: Any) -> BillingLineItem:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            quantity=Decimal("100"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.00"),
        )
        defaults.update(overrides)
        return BillingLineItem(**defaults)

    def test_upsert_and_find_by_date(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing())
        session.commit()
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 1
        assert results[0].total_cost == Decimal("1.00")

    def test_upsert_updates_existing(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing())
        session.commit()
        repo.upsert(self._make_billing(total_cost=Decimal("2.00")))
        session.commit()
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 1
        assert results[0].total_cost == Decimal("2.00")

    def test_find_by_range(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing(timestamp=datetime(2026, 1, 10, tzinfo=UTC)))
        repo.upsert(self._make_billing(timestamp=datetime(2026, 1, 20, tzinfo=UTC), resource_id="r2"))
        session.commit()
        results = repo.find_by_range("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_date_empty(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        results = repo.find_by_date("eco", "t1", date(2026, 3, 1))
        assert results == []

    def test_delete_before(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing(timestamp=datetime(2025, 1, 1, tzinfo=UTC)))
        repo.upsert(self._make_billing(timestamp=datetime(2026, 6, 1, tzinfo=UTC), resource_id="r2"))
        session.commit()
        count = repo.delete_before("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        session.commit()
        assert count == 1


# --- Chargeback Repository ---


class TestChargebackRepository:
    def _make_row(self, **overrides: Any) -> ChargebackRow:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type=CostType.USAGE,
            amount=Decimal("50.00"),
            allocation_method="direct",
            tags=["tag1"],
        )
        defaults.update(overrides)
        return ChargebackRow(**defaults)

    def test_upsert_creates_dimension_and_fact(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        row = self._make_row()
        result = repo.upsert(row)
        session.commit()
        assert result.amount == Decimal("50.00")
        assert result.ecosystem == "eco"

    def test_upsert_reuses_dimension(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        repo = SQLModelChargebackRepository(session)
        row1 = self._make_row(timestamp=datetime(2026, 1, 15, tzinfo=UTC))
        row2 = self._make_row(timestamp=datetime(2026, 1, 16, tzinfo=UTC))
        repo.upsert(row1)
        repo.upsert(row2)
        session.commit()
        results = repo.find_by_range("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC))
        assert len(results) == 2
        # Verify only one dimension was created (same dimension reused for both facts)
        dims = session.exec(select(ChargebackDimensionTable).where(ChargebackDimensionTable.ecosystem == "eco")).all()
        assert len(dims) == 1

    def test_upsert_different_dimensions(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        row1 = self._make_row(identity_id="u1")
        row2 = self._make_row(identity_id="u2")
        repo.upsert(row1)
        repo.upsert(row2)
        session.commit()
        results = repo.find_by_identity("eco", "t1", "u1")
        assert len(results) == 1
        results = repo.find_by_identity("eco", "t1", "u2")
        assert len(results) == 1

    def test_find_by_date(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row())
        session.commit()
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 1

    def test_find_by_range(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row(timestamp=datetime(2026, 1, 10, tzinfo=UTC)))
        repo.upsert(self._make_row(timestamp=datetime(2026, 1, 20, tzinfo=UTC), identity_id="u2"))
        session.commit()
        results = repo.find_by_range("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1

    def test_find_by_date_empty(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        results = repo.find_by_date("eco", "t1", date(2026, 3, 1))
        assert results == []

    def test_delete_by_date(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row())
        session.commit()
        count = repo.delete_by_date("eco", "t1", date(2026, 1, 15))
        session.commit()
        assert count == 1
        assert repo.find_by_date("eco", "t1", date(2026, 1, 15)) == []

    def test_delete_before(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row(timestamp=datetime(2025, 6, 1, tzinfo=UTC)))
        repo.upsert(self._make_row(timestamp=datetime(2026, 6, 1, tzinfo=UTC), identity_id="u2"))
        session.commit()
        count = repo.delete_before("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        session.commit()
        assert count == 1


# --- PipelineState Repository ---


class TestPipelineStateRepository:
    def _make_state(self, **overrides: Any) -> PipelineState:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            tracking_date=date(2026, 1, 15),
        )
        defaults.update(overrides)
        return PipelineState(**defaults)

    def test_upsert_and_get(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state())
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.billing_gathered is False

    def test_find_needing_calculation(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(billing_gathered=True, chargeback_calculated=False))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 16), billing_gathered=False))
        session.commit()
        results = repo.find_needing_calculation("eco", "t1")
        assert len(results) == 1
        assert results[0].tracking_date == date(2026, 1, 15)

    def test_find_by_range(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 10)))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 20)))
        session.commit()
        results = repo.find_by_range("eco", "t1", date(2026, 1, 5), date(2026, 1, 15))
        assert len(results) == 1

    def test_mark_billing_gathered(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state())
        session.commit()
        repo.mark_billing_gathered("eco", "t1", date(2026, 1, 15))
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.billing_gathered is True

    def test_resources_gathered_transition(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(resources_gathered=False))
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.resources_gathered is False
        # Update to gathered
        repo.upsert(self._make_state(resources_gathered=True))
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.resources_gathered is True

    def test_mark_chargeback_calculated(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state())
        session.commit()
        repo.mark_chargeback_calculated("eco", "t1", date(2026, 1, 15))
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.chargeback_calculated is True


# --- Tag Repository ---


class TestTagRepository:
    def test_add_and_get_tags(self, session: Session) -> None:
        # Need a dimension first
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        dim = ChargebackDimensionTable(
            ecosystem="eco",
            tenant_id="t1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type="usage",
        )
        session.add(dim)
        session.flush()

        repo = SQLModelTagRepository(session)
        tag = repo.add_tag(dim.dimension_id, "team", "platform", "admin")  # type: ignore[arg-type]
        session.commit()
        assert tag.tag_id is not None
        assert tag.tag_key == "team"

        tags = repo.get_tags(dim.dimension_id)  # type: ignore[arg-type]
        assert len(tags) == 1

    def test_delete_tag(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        dim = ChargebackDimensionTable(
            ecosystem="eco",
            tenant_id="t1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type="usage",
        )
        session.add(dim)
        session.flush()

        repo = SQLModelTagRepository(session)
        tag = repo.add_tag(dim.dimension_id, "team", "platform", "admin")  # type: ignore[arg-type]
        session.commit()
        repo.delete_tag(tag.tag_id)  # type: ignore[arg-type]
        session.commit()
        tags = repo.get_tags(dim.dimension_id)  # type: ignore[arg-type]
        assert len(tags) == 0

    def test_multiple_tags_per_dimension(self, session: Session) -> None:
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        dim = ChargebackDimensionTable(
            ecosystem="eco",
            tenant_id="t1",
            product_category="compute",
            product_type="kafka",
            identity_id="u1",
            cost_type="usage",
        )
        session.add(dim)
        session.flush()

        repo = SQLModelTagRepository(session)
        repo.add_tag(dim.dimension_id, "team", "platform", "admin")  # type: ignore[arg-type]
        repo.add_tag(dim.dimension_id, "env", "prod", "admin")  # type: ignore[arg-type]
        session.commit()
        tags = repo.get_tags(dim.dimension_id)  # type: ignore[arg-type]
        assert len(tags) == 2
