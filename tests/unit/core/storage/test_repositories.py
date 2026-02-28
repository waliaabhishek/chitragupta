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
    SQLModelPipelineRunRepository,
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
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1
        assert total == 1

    def test_find_active_at_before_creation(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 10, tzinfo=UTC)))
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC))
        assert len(results) == 0
        assert total == 0

    def test_find_active_at_deleted_before(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 10, tzinfo=UTC),
            )
        )
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 0
        assert total == 0

    def test_find_active_at_null_created(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=None))
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        assert len(results) == 1
        assert total == 1

    def test_find_active_at_null_deleted(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=None,
            )
        )
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 6, 1, tzinfo=UTC))
        assert len(results) == 1
        assert total == 1

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
        results, total = repo.find_by_period(
            "eco", "t1", datetime(2026, 1, 10, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC)
        )
        assert len(results) == 1
        assert total == 1

    def test_find_by_period_created_after(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 2, 1, tzinfo=UTC)))
        session.commit()
        results, total = repo.find_by_period(
            "eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC)
        )
        assert len(results) == 0
        assert total == 0

    def test_find_by_period_deleted_before_start(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2025, 12, 1, tzinfo=UTC),
                deleted_at=datetime(2025, 12, 31, tzinfo=UTC),
            )
        )
        session.commit()
        results, total = repo.find_by_period(
            "eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC)
        )
        assert len(results) == 0
        assert total == 0

    def test_find_by_period_created_and_deleted_within(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                created_at=datetime(2026, 1, 5, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 10, tzinfo=UTC),
            )
        )
        session.commit()
        results, total = repo.find_by_period(
            "eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 31, tzinfo=UTC)
        )
        assert len(results) == 1
        assert total == 1

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

    def test_find_active_at_filter_by_resource_type(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", resource_type="kafka"))
        repo.upsert(self._make_resource(resource_id="r2", resource_type="ksql"))
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka")
        assert len(results) == 1
        assert total == 1
        assert results[0].resource_id == "r1"

    def test_find_active_at_limit_offset(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1"))
        repo.upsert(self._make_resource(resource_id="r2"))
        repo.upsert(self._make_resource(resource_id="r3"))
        session.commit()
        ts = datetime(2026, 1, 15, tzinfo=UTC)
        all_results, total = repo.find_active_at("eco", "t1", ts)
        assert total == 3
        page1, _ = repo.find_active_at("eco", "t1", ts, limit=2, offset=0)
        page2, _ = repo.find_active_at("eco", "t1", ts, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 1
        assert {r.resource_id for r in page1 + page2} == {"r1", "r2", "r3"}

    def test_find_by_period_filter_by_resource_type(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", resource_type="kafka"))
        repo.upsert(self._make_resource(resource_id="r2", resource_type="ksql"))
        session.commit()
        results, total = repo.find_by_period(
            "eco",
            "t1",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, tzinfo=UTC),
            resource_type="ksql",
        )
        assert len(results) == 1
        assert total == 1
        assert results[0].resource_id == "r2"

    def test_find_by_period_limit_offset(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(1, 5):
            repo.upsert(self._make_resource(resource_id=f"r{i}"))
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        _, total = repo.find_by_period("eco", "t1", start, end)
        assert total == 4
        page, _ = repo.find_by_period("eco", "t1", start, end, limit=2, offset=0)
        assert len(page) == 2


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
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC))
        assert len(results) == 1
        assert total == 1

    def test_find_by_period(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(
            self._make_identity(
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
            )
        )
        session.commit()
        results, total = repo.find_by_period(
            "eco", "t1", datetime(2026, 1, 5, tzinfo=UTC), datetime(2026, 1, 15, tzinfo=UTC)
        )
        assert len(results) == 1
        assert total == 1

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

    def test_find_active_at_filter_by_identity_type(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(identity_id="u1", identity_type="user"))
        repo.upsert(self._make_identity(identity_id="sa1", identity_type="service_account"))
        session.commit()
        results, total = repo.find_active_at(
            "eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), identity_type="service_account"
        )
        assert len(results) == 1
        assert total == 1
        assert results[0].identity_id == "sa1"

    def test_find_by_period_filter_by_identity_type(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity(identity_id="u1", identity_type="user"))
        repo.upsert(self._make_identity(identity_id="sa1", identity_type="service_account"))
        session.commit()
        results, total = repo.find_by_period(
            "eco",
            "t1",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 2, 1, tzinfo=UTC),
            identity_type="user",
        )
        assert len(results) == 1
        assert total == 1
        assert results[0].identity_id == "u1"

    def test_find_active_at_total_count_independent_of_limit(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        for i in range(1, 4):
            repo.upsert(self._make_identity(identity_id=f"u{i}"))
        session.commit()
        ts = datetime(2026, 1, 15, tzinfo=UTC)
        paged, total = repo.find_active_at("eco", "t1", ts, limit=1, offset=0)
        assert total == 3  # total reflects all matching, not just page
        assert len(paged) == 1


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

    def test_upsert_detects_billing_revision_and_logs_warning(
        self, session: Session, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing(total_cost=Decimal("1.00")))
        session.commit()

        with caplog.at_level(logging.WARNING):
            repo.upsert(self._make_billing(total_cost=Decimal("9.99")))
            session.commit()

        assert "Billing revision detected" in caplog.text
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 1
        assert results[0].total_cost == Decimal("9.99")

    def test_upsert_no_warning_when_same_total_cost(self, session: Session, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing(total_cost=Decimal("1.00")))
        session.commit()

        with caplog.at_level(logging.WARNING):
            repo.upsert(self._make_billing(total_cost=Decimal("1.00")))
            session.commit()

        assert "Billing revision detected" not in caplog.text

    def test_upsert_new_record_no_warning(self, session: Session, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        repo = SQLModelBillingRepository(session)

        with caplog.at_level(logging.WARNING):
            repo.upsert(self._make_billing())
            session.commit()

        assert "Billing revision detected" not in caplog.text


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
        repo.upsert(self._make_state(billing_gathered=True, resources_gathered=True, chargeback_calculated=False))
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

    def test_count_pending(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(billing_gathered=True, resources_gathered=True, chargeback_calculated=False))
        repo.upsert(
            self._make_state(
                tracking_date=date(2026, 1, 16),
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
            )
        )
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 17), billing_gathered=False))
        session.commit()
        assert repo.count_pending("eco", "t1") == 1

    def test_count_calculated(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(chargeback_calculated=True))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 16), chargeback_calculated=True))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 17), chargeback_calculated=False))
        session.commit()
        assert repo.count_calculated("eco", "t1") == 2

    def test_get_last_calculated_date(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 10), chargeback_calculated=True))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 20), chargeback_calculated=True))
        repo.upsert(self._make_state(tracking_date=date(2026, 1, 25), chargeback_calculated=False))
        session.commit()
        result = repo.get_last_calculated_date("eco", "t1")
        assert result == date(2026, 1, 20)

    def test_get_last_calculated_date_none(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        assert repo.get_last_calculated_date("eco", "t1") is None

    def test_mark_needs_recalculation(self, session: Session) -> None:
        """CT-004: mark_needs_recalculation resets chargeback_calculated to False."""
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(chargeback_calculated=True))
        session.commit()

        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.chargeback_calculated is True

        repo.mark_needs_recalculation("eco", "t1", date(2026, 1, 15))
        session.commit()

        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.chargeback_calculated is False

    def test_mark_needs_recalculation_nonexistent_date_is_noop(self, session: Session) -> None:
        """mark_needs_recalculation on missing date does not raise."""
        repo = SQLModelPipelineStateRepository(session)
        # No state inserted — should not raise
        repo.mark_needs_recalculation("eco", "t1", date(2026, 6, 1))
        session.commit()


# --- Paginated Repository Methods ---


class TestResourceFindPaginated:
    def _make_resource(self, **overrides: Any) -> Resource:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 10, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return Resource(**defaults)

    def test_basic_pagination(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(5):
            repo.upsert(self._make_resource(resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=2, offset=0)
        assert total == 5
        assert len(items) == 2

    def test_with_type_filter(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", resource_type="kafka"))
        repo.upsert(self._make_resource(resource_id="r2", resource_type="ksql"))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, resource_type="kafka")
        assert total == 1
        assert items[0].resource_type == "kafka"

    def test_with_status_filter(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", status=ResourceStatus.ACTIVE))
        repo.upsert(self._make_resource(resource_id="r2", status=ResourceStatus.DELETED))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, status="active")
        assert total == 1

    def test_returns_correct_total(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(10):
            repo.upsert(self._make_resource(resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=3, offset=6)
        assert total == 10
        assert len(items) == 3


class TestIdentityFindPaginated:
    def test_basic(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        for i in range(3):
            repo.upsert(
                Identity(
                    ecosystem="eco",
                    tenant_id="t1",
                    identity_id=f"u{i}",
                    identity_type="user",
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                )
            )
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=2, offset=0)
        assert total == 3
        assert len(items) == 2

    def test_with_type_filter(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        repo.upsert(
            Identity(
                ecosystem="eco",
                tenant_id="t1",
                identity_id="u1",
                identity_type="user",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        repo.upsert(
            Identity(
                ecosystem="eco",
                tenant_id="t1",
                identity_id="sa1",
                identity_type="service_account",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, identity_type="service_account")
        assert total == 1
        assert items[0].identity_id == "sa1"


class TestBillingFindByFilters:
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

    def test_all_filters(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        repo.upsert(self._make_billing())
        repo.upsert(self._make_billing(resource_id="r2", product_type="connect"))
        session.commit()
        items, total = repo.find_by_filters(
            "eco",
            "t1",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 1, tzinfo=UTC),
            product_type="kafka",
            resource_id="r1",
        )
        assert total == 1
        assert items[0].resource_id == "r1"

    def test_pagination(self, session: Session) -> None:
        repo = SQLModelBillingRepository(session)
        for i in range(5):
            repo.upsert(self._make_billing(resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_by_filters("eco", "t1", limit=2, offset=0)
        assert total == 5
        assert len(items) == 2


class TestChargebackFindByFilters:
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
            tags=[],
        )
        defaults.update(overrides)
        return ChargebackRow(**defaults)

    def test_all_filters(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row())
        repo.upsert(self._make_row(identity_id="u2", cost_type=CostType.SHARED))
        session.commit()
        items, total = repo.find_by_filters(
            "eco",
            "t1",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 1, tzinfo=UTC),
            identity_id="u1",
            product_type="kafka",
            cost_type="usage",
        )
        assert total == 1
        assert items[0].identity_id == "u1"

    def test_partial_filters(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row())
        repo.upsert(self._make_row(identity_id="u2"))
        session.commit()
        items, total = repo.find_by_filters("eco", "t1", identity_id="u2")
        assert total == 1

    def test_pagination(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        for i in range(5):
            repo.upsert(self._make_row(timestamp=datetime(2026, 1, 15, i, tzinfo=UTC), resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_by_filters("eco", "t1", limit=2, offset=0)
        assert total == 5
        assert len(items) == 2


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

    def test_get_tag_found(self, session: Session) -> None:
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
        tag = repo.add_tag(dim.dimension_id, "env", "prod", "admin")  # type: ignore[arg-type]
        session.commit()
        got = repo.get_tag(tag.tag_id)  # type: ignore[arg-type]
        assert got is not None
        assert got.tag_key == "env"
        assert got.display_name == "prod"
        # tag_value is auto-generated UUID
        assert len(got.tag_value) == 36

    def test_get_tag_not_found(self, session: Session) -> None:
        repo = SQLModelTagRepository(session)
        assert repo.get_tag(99999) is None

    def test_find_tags_for_tenant(self, session: Session) -> None:
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

        items, total = repo.find_tags_for_tenant("eco", "t1")
        assert total == 2
        assert len(items) == 2

    def test_find_tags_for_tenant_empty(self, session: Session) -> None:
        repo = SQLModelTagRepository(session)
        items, total = repo.find_tags_for_tenant("eco", "no-tenant")
        assert total == 0
        assert items == []


# --- Chargeback Repository: get_dimension + aggregate ---


class TestChargebackRepositoryExtensions:
    def _make_chargeback(self, **overrides: Any) -> ChargebackRow:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 15, tzinfo=UTC),
            resource_id="r1",
            product_category="compute",
            product_type="kafka",
            identity_id="user-1",
            cost_type=CostType.USAGE,
            amount=Decimal("10.00"),
            allocation_method="direct",
            allocation_detail=None,
            tags=[],
            metadata={},
        )
        defaults.update(overrides)
        return ChargebackRow(**defaults)

    def test_get_dimension_found(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_chargeback())
        session.commit()
        dim = repo.get_dimension(1)
        assert dim is not None
        assert dim.ecosystem == "eco"
        assert dim.tenant_id == "t1"
        assert dim.identity_id == "user-1"

    def test_get_dimension_not_found(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        assert repo.get_dimension(99999) is None

    def test_aggregate_single_group(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        for i, uid in enumerate(["user-1", "user-1", "user-2"]):
            repo.upsert(
                self._make_chargeback(
                    timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                    resource_id=f"r-{i}",
                    identity_id=uid,
                )
            )
        session.commit()

        rows = repo.aggregate(
            ecosystem="eco",
            tenant_id="t1",
            group_by=["identity_id"],
            time_bucket="day",
            start=datetime(2026, 2, 1, tzinfo=UTC),
            end=datetime(2026, 3, 1, tzinfo=UTC),
        )
        assert len(rows) >= 1
        dims = {r.dimensions["identity_id"] for r in rows}
        assert "user-1" in dims
        assert "user-2" in dims

    def test_aggregate_multi_group(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_chargeback())
        session.commit()

        rows = repo.aggregate(
            ecosystem="eco",
            tenant_id="t1",
            group_by=["identity_id", "product_type"],
            time_bucket="day",
            start=datetime(2026, 2, 1, tzinfo=UTC),
            end=datetime(2026, 3, 1, tzinfo=UTC),
        )
        assert len(rows) == 1
        assert "identity_id" in rows[0].dimensions
        assert "product_type" in rows[0].dimensions

    def test_aggregate_time_buckets(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_chargeback())
        session.commit()

        for bucket in ["hour", "day", "week", "month"]:
            rows = repo.aggregate(
                ecosystem="eco",
                tenant_id="t1",
                group_by=["identity_id"],
                time_bucket=bucket,
                start=datetime(2026, 2, 1, tzinfo=UTC),
                end=datetime(2026, 3, 1, tzinfo=UTC),
            )
            assert len(rows) >= 1, f"No results for time_bucket={bucket}"

    def test_aggregate_empty(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        rows = repo.aggregate(
            ecosystem="eco",
            tenant_id="t1",
            group_by=["identity_id"],
            time_bucket="day",
        )
        assert rows == []


# --- PipelineRun Repository ---


class TestPipelineRunRepository:
    def test_create_run_sets_status_running(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        run = repo.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()
        assert run.id is not None
        assert run.tenant_name == "my-tenant"
        assert run.status == "running"
        assert run.ended_at is None
        assert run.dates_gathered == 0

    def test_get_run_found(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        run = repo.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()
        got = repo.get_run(run.id)  # type: ignore[arg-type]
        assert got is not None
        assert got.id == run.id
        assert got.tenant_name == "my-tenant"

    def test_get_run_not_found(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        assert repo.get_run(99999) is None

    def test_update_run_persists_changes(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        run = repo.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()

        run.status = "completed"
        run.ended_at = datetime(2026, 2, 26, 11, 0, tzinfo=UTC)
        run.dates_gathered = 5
        run.dates_calculated = 3
        run.rows_written = 100
        updated = repo.update_run(run)
        session.commit()

        assert updated.status == "completed"
        assert updated.dates_gathered == 5
        assert updated.rows_written == 100
        assert updated.ended_at is not None

    def test_update_run_failed_state(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        run = repo.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()

        run.status = "failed"
        run.ended_at = datetime(2026, 2, 26, 10, 30, tzinfo=UTC)
        run.error_message = "Pipeline execution failed"
        updated = repo.update_run(run)
        session.commit()

        assert updated.status == "failed"
        assert updated.error_message == "Pipeline execution failed"

    def test_get_latest_run_returns_most_recent(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        repo.create_run("my-tenant", datetime(2026, 2, 24, 10, 0, tzinfo=UTC))
        run2 = repo.create_run("my-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()

        latest = repo.get_latest_run("my-tenant")
        assert latest is not None
        assert latest.id == run2.id

    def test_get_latest_run_none_when_empty(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        assert repo.get_latest_run("no-such-tenant") is None

    def test_list_runs_for_tenant(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        for day in [24, 25, 26]:
            repo.create_run("my-tenant", datetime(2026, 2, day, 10, 0, tzinfo=UTC))
        repo.create_run("other-tenant", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()

        runs = repo.list_runs_for_tenant("my-tenant")
        assert len(runs) == 3
        # Ordered descending by started_at
        assert runs[0].started_at > runs[1].started_at

    def test_list_runs_for_tenant_limit(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        for day in range(1, 11):
            repo.create_run("my-tenant", datetime(2026, 2, 1, day, 0, tzinfo=UTC))
        session.commit()

        runs = repo.list_runs_for_tenant("my-tenant", limit=5)
        assert len(runs) == 5

    def test_tenant_isolation(self, session: Session) -> None:
        repo = SQLModelPipelineRunRepository(session)
        repo.create_run("tenant-a", datetime(2026, 2, 26, 10, 0, tzinfo=UTC))
        session.commit()

        assert repo.get_latest_run("tenant-b") is None
        assert repo.list_runs_for_tenant("tenant-b") == []
