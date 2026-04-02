from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from core.models.billing import BillingLineItem, CoreBillingLineItem
from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity, Identity
from core.models.pipeline import PipelineState
from core.models.resource import CoreResource, Resource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import (
    SQLModelBillingRepository,
    SQLModelChargebackRepository,
    SQLModelEntityTagRepository,
    SQLModelIdentityRepository,
    SQLModelPipelineRunRepository,
    SQLModelPipelineStateRepository,
    SQLModelResourceRepository,
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
        return CoreResource(**defaults)

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
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka")
        assert len(results) == 1
        assert total == 1

    def test_find_active_at_before_creation(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 10, tzinfo=UTC)))
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 5, tzinfo=UTC), resource_type="kafka")
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
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka")
        assert len(results) == 0
        assert total == 0

    def test_find_active_at_null_created(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=None))
        session.commit()
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC), resource_type="kafka")
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
        results, total = repo.find_active_at("eco", "t1", datetime(2026, 6, 1, tzinfo=UTC), resource_type="kafka")
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
            "eco",
            "t1",
            datetime(2026, 1, 10, tzinfo=UTC),
            datetime(2026, 1, 15, tzinfo=UTC),
            resource_type="kafka",
        )
        assert len(results) == 1
        assert total == 1

    def test_find_by_period_created_after(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 2, 1, tzinfo=UTC)))
        session.commit()
        results, total = repo.find_by_period(
            "eco",
            "t1",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 31, tzinfo=UTC),
            resource_type="kafka",
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
            "eco",
            "t1",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 31, tzinfo=UTC),
            resource_type="kafka",
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
            "eco",
            "t1",
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 31, tzinfo=UTC),
            resource_type="kafka",
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
        all_results, total = repo.find_active_at("eco", "t1", ts, resource_type="kafka")
        assert total == 3
        page1, _ = repo.find_active_at("eco", "t1", ts, resource_type="kafka", limit=2, offset=0)
        page2, _ = repo.find_active_at("eco", "t1", ts, resource_type="kafka", limit=2, offset=2)
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
        _, total = repo.find_by_period("eco", "t1", start, end, resource_type="kafka")
        assert total == 4
        page, _ = repo.find_by_period("eco", "t1", start, end, resource_type="kafka", limit=2, offset=0)
        assert len(page) == 2

    def test_find_by_period_metadata_filter_single_key_returns_matching(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                resource_id="r-pool1", resource_type="flink_statement", metadata={"compute_pool_id": "pool-1"}
            )
        )
        repo.upsert(
            self._make_resource(
                resource_id="r-pool2", resource_type="flink_statement", metadata={"compute_pool_id": "pool-2"}
            )
        )
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period(
            "eco", "t1", start, end, resource_type="flink_statement", metadata_filter={"compute_pool_id": "pool-1"}
        )
        assert total == 1
        assert len(results) == 1
        assert results[0].resource_id == "r-pool1"

    def test_find_by_period_metadata_filter_none_returns_all(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                resource_id="r-pool1", resource_type="flink_statement", metadata={"compute_pool_id": "pool-1"}
            )
        )
        repo.upsert(
            self._make_resource(
                resource_id="r-pool2", resource_type="flink_statement", metadata={"compute_pool_id": "pool-2"}
            )
        )
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period(
            "eco", "t1", start, end, resource_type="flink_statement", metadata_filter=None
        )
        assert total == 2
        assert len(results) == 2

    def test_find_by_period_metadata_filter_no_match_returns_empty(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        repo.upsert(
            self._make_resource(
                resource_id="r-pool1", resource_type="flink_statement", metadata={"compute_pool_id": "pool-1"}
            )
        )
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period(
            "eco",
            "t1",
            start,
            end,
            resource_type="flink_statement",
            metadata_filter={"compute_pool_id": "pool-nonexistent"},
        )
        assert results == []
        assert total == 0

    def test_find_by_period_metadata_filter_multi_key_ands_conditions(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        # Matches both k1 and k2
        repo.upsert(self._make_resource(resource_id="r-both", metadata={"k1": "v1", "k2": "v2"}))
        # Matches only k1
        repo.upsert(self._make_resource(resource_id="r-k1only", metadata={"k1": "v1", "k2": "other"}))
        # Matches only k2
        repo.upsert(self._make_resource(resource_id="r-k2only", metadata={"k1": "other", "k2": "v2"}))
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period(
            "eco", "t1", start, end, resource_type="kafka", metadata_filter={"k1": "v1", "k2": "v2"}
        )
        assert total == 1
        assert len(results) == 1
        assert results[0].resource_id == "r-both"

    # TASK-179: find_by_period parent_id filter tests

    def test_find_by_period_parent_id_filters_to_matching_parent(self, session: Session) -> None:
        """find_by_period(parent_id='lkc-abc') returns only resources with that parent_id."""
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r-match", parent_id="lkc-abc"))
        repo.upsert(self._make_resource(resource_id="r-other", parent_id="lkc-xyz"))
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period("eco", "t1", start, end, resource_type="kafka", parent_id="lkc-abc")
        assert total == 1
        assert len(results) == 1
        assert results[0].resource_id == "r-match"

    def test_find_by_period_parent_id_excludes_other_parents(self, session: Session) -> None:
        """find_by_period(parent_id='lkc-abc') returns empty when only other parent_ids exist."""
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r-other", parent_id="lkc-xyz"))
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period("eco", "t1", start, end, resource_type="kafka", parent_id="lkc-abc")
        assert results == []
        assert total == 0

    def test_find_by_period_parent_id_none_returns_all_parents(self, session: Session) -> None:
        """find_by_period(parent_id=None) returns all resources regardless of parent_id."""
        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(resource_id="r1", parent_id="lkc-abc"))
        repo.upsert(self._make_resource(resource_id="r2", parent_id="lkc-xyz"))
        session.commit()
        start, end = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC)
        results, total = repo.find_by_period("eco", "t1", start, end, resource_type="kafka", parent_id=None)
        assert total == 2
        assert len(results) == 2

    def test_find_by_period_parent_id_deleted_after_start_included(self, session: Session) -> None:
        """Topic deleted after b_start (deleted_at >= b_start) is included by temporal filter."""
        repo = SQLModelResourceRepository(session)
        b_start = datetime(2026, 3, 1, tzinfo=UTC)
        b_end = datetime(2026, 3, 2, tzinfo=UTC)
        repo.upsert(
            self._make_resource(
                resource_id="r-deleted-in-window",
                parent_id="lkc-abc",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                deleted_at=datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC),  # deleted_at >= b_start
            )
        )
        session.commit()
        results, total = repo.find_by_period("eco", "t1", b_start, b_end, resource_type="kafka", parent_id="lkc-abc")
        assert len(results) == 1
        assert results[0].resource_id == "r-deleted-in-window"

    def test_find_by_period_parent_id_created_after_end_excluded(self, session: Session) -> None:
        """Topic created after b_end (created_at >= b_end) is excluded by temporal filter."""
        repo = SQLModelResourceRepository(session)
        b_start = datetime(2026, 3, 1, tzinfo=UTC)
        b_end = datetime(2026, 3, 2, tzinfo=UTC)
        repo.upsert(
            self._make_resource(
                resource_id="r-created-after",
                parent_id="lkc-abc",
                created_at=datetime(2026, 3, 3, tzinfo=UTC),  # created_at >= b_end
            )
        )
        session.commit()
        results, total = repo.find_by_period("eco", "t1", b_start, b_end, resource_type="kafka", parent_id="lkc-abc")
        assert results == []
        assert total == 0

    def test_find_by_period_parent_id_topic_never_in_cluster_excluded(self, session: Session) -> None:
        """Topic with different cluster parent_id is always excluded regardless of temporal state."""
        repo = SQLModelResourceRepository(session)
        b_start = datetime(2026, 3, 1, tzinfo=UTC)
        b_end = datetime(2026, 3, 2, tzinfo=UTC)
        repo.upsert(
            self._make_resource(
                resource_id="r-wrong-cluster",
                parent_id="lkc-other",  # different cluster
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        session.commit()
        results, total = repo.find_by_period("eco", "t1", b_start, b_end, resource_type="kafka", parent_id="lkc-abc")
        assert results == []
        assert total == 0


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
        return CoreIdentity(**defaults)

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
        return CoreBillingLineItem(**defaults)

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

    def test_upsert_distinct_product_category_rows_preserved(self, session: Session) -> None:
        """Two BillingLineItems identical on 5-field key but differing product_category must produce 2 rows."""
        repo = SQLModelBillingRepository(session)
        line_a = self._make_billing(product_category="Apache Kafka", total_cost=Decimal("100.00"))
        line_b = self._make_billing(product_category="Kafka Connect", total_cost=Decimal("25.00"))
        repo.upsert(line_a)
        session.commit()
        repo.upsert(line_b)
        session.commit()
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 2
        costs = {r.total_cost for r in results}
        assert costs == {Decimal("100.00"), Decimal("25.00")}

    def test_upsert_billing_revision_detected_same_category_different_cost(
        self, session: Session, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Upserting same 6-field key with different total_cost emits warning and keeps exactly one row."""
        import logging

        repo = SQLModelBillingRepository(session)
        line_v1 = self._make_billing(product_category="Apache Kafka", total_cost=Decimal("100.00"))
        line_v2 = self._make_billing(product_category="Apache Kafka", total_cost=Decimal("50.00"))
        repo.upsert(line_v1)
        session.commit()
        with caplog.at_level(logging.WARNING):
            repo.upsert(line_v2)
            session.commit()
        assert "Billing revision detected" in caplog.text
        results = repo.find_by_date("eco", "t1", date(2026, 1, 15))
        assert len(results) == 1
        assert results[0].total_cost == Decimal("50.00")

    def test_increment_allocation_attempts_success(self, session: Session) -> None:
        """increment_allocation_attempts(line) returns 1 on first call, 2 on second."""
        repo = SQLModelBillingRepository(session)
        line = self._make_billing()
        repo.upsert(line)
        session.commit()
        result1 = repo.increment_allocation_attempts(line)
        session.commit()
        assert result1 == 1
        result2 = repo.increment_allocation_attempts(line)
        session.commit()
        assert result2 == 2

    def test_increment_allocation_attempts_not_found_raises_key_error(self, session: Session) -> None:
        """increment_allocation_attempts(line) raises KeyError for a non-existent line.

        The error message must contain all 6 key fields: ecosystem, tenant_id, timestamp,
        resource_id, product_type, and product_category.
        """
        repo = SQLModelBillingRepository(session)
        line = self._make_billing()
        with pytest.raises(KeyError) as exc_info:
            repo.increment_allocation_attempts(line)
        error_msg = str(exc_info.value)
        assert "eco" in error_msg
        assert "t1" in error_msg
        assert "r1" in error_msg
        assert "kafka" in error_msg
        assert "compute" in error_msg
        assert "2026" in error_msg


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

    def test_delete_by_date_normal_case(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row(identity_id="u1", timestamp=datetime(2026, 1, 15, tzinfo=UTC)))
        repo.upsert(self._make_row(identity_id="u2", timestamp=datetime(2026, 1, 20, tzinfo=UTC)))
        session.commit()
        count = repo.delete_by_date("eco", "t1", date(2026, 1, 15))
        session.commit()
        assert count == 1
        assert repo.find_by_date("eco", "t1", date(2026, 1, 15)) == []
        assert len(repo.find_by_date("eco", "t1", date(2026, 1, 20))) == 1

    def test_delete_by_date_zero_dimensions(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        count = repo.delete_by_date("eco", "t1", date(2026, 1, 15))
        assert count == 0

    def test_delete_by_date_zero_facts_in_range(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row(timestamp=datetime(2026, 1, 20, tzinfo=UTC)))
        session.commit()
        count = repo.delete_by_date("eco", "t1", date(2026, 1, 15))
        session.commit()
        assert count == 0
        assert len(repo.find_by_date("eco", "t1", date(2026, 1, 20))) == 1

    def test_delete_by_date_large_dimension_set(self, session: Session) -> None:
        from sqlalchemy.exc import OperationalError

        repo = SQLModelChargebackRepository(session)
        n = 1001
        for i in range(n):
            repo.upsert(self._make_row(identity_id=f"u{i}", timestamp=datetime(2026, 1, 15, tzinfo=UTC)))
        session.commit()
        try:
            count = repo.delete_by_date("eco", "t1", date(2026, 1, 15))
        except OperationalError:
            pytest.fail("delete_by_date raised OperationalError with >999 dimensions")
        session.commit()
        assert count == n

    def test_delete_before(self, session: Session) -> None:
        repo = SQLModelChargebackRepository(session)
        repo.upsert(self._make_row(timestamp=datetime(2025, 6, 1, tzinfo=UTC)))
        repo.upsert(self._make_row(timestamp=datetime(2026, 6, 1, tzinfo=UTC), identity_id="u2"))
        session.commit()
        count = repo.delete_before("eco", "t1", datetime(2026, 1, 1, tzinfo=UTC))
        session.commit()
        assert count == 1


# --- Chargeback Repository Dimension Cache ---


class TestChargebackRepositoryDimensionCache:
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

    def test_dimension_cache_hit_no_extra_db_select(self, session: Session) -> None:
        """Second upsert with identical dimension fields uses cache — no extra SELECT issued."""
        from unittest.mock import patch

        repo = SQLModelChargebackRepository(session)
        row1 = self._make_row(timestamp=datetime(2026, 1, 15, tzinfo=UTC))
        row2 = self._make_row(timestamp=datetime(2026, 1, 16, tzinfo=UTC))

        original_exec = session.exec
        dim_selects: list[Any] = []

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            stmt_str = str(stmt)
            if "chargeback_dimensions" in stmt_str.lower():
                dim_selects.append(stmt_str)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            repo.upsert(row1)
            repo.upsert(row2)

        assert len(repo._dimension_cache) == 1
        assert len(dim_selects) == 1  # cache hit on 2nd upsert: no 2nd SELECT

    def test_dimension_cache_miss_creates_and_caches(self, session: Session) -> None:
        """Cache miss: first upsert creates dim, flushes, and caches it; subsequent call hits cache."""
        repo = SQLModelChargebackRepository(session)
        row = self._make_row()

        assert len(repo._dimension_cache) == 0

        repo.upsert(row)

        assert len(repo._dimension_cache) == 1

        # Second upsert with same dim fields — cache already warm
        row2 = self._make_row(timestamp=datetime(2026, 1, 16, tzinfo=UTC))
        repo.upsert(row2)

        assert len(repo._dimension_cache) == 1  # still only one dimension

    def test_upsert_returns_correct_chargeback_row_on_cache_miss(self, session: Session) -> None:
        """upsert() returns ChargebackRow with all dimension fields populated on cache miss."""
        repo = SQLModelChargebackRepository(session)
        row = self._make_row()

        result = repo.upsert(row)

        assert result.ecosystem == "eco"
        assert result.tenant_id == "t1"
        assert result.resource_id == "r1"
        assert result.product_category == "compute"
        assert result.product_type == "kafka"
        assert result.identity_id == "u1"
        assert result.cost_type == CostType.USAGE
        assert result.amount == Decimal("50.00")
        assert result.allocation_method == "direct"

    def test_upsert_returns_correct_chargeback_row_on_cache_hit(self, session: Session) -> None:
        """upsert() returns ChargebackRow with all dimension fields populated on cache hit."""
        repo = SQLModelChargebackRepository(session)
        row1 = self._make_row(timestamp=datetime(2026, 1, 15, tzinfo=UTC))
        row2 = self._make_row(timestamp=datetime(2026, 1, 16, tzinfo=UTC))

        repo.upsert(row1)
        result = repo.upsert(row2)  # cache hit path

        assert result.ecosystem == "eco"
        assert result.tenant_id == "t1"
        assert result.resource_id == "r1"
        assert result.product_category == "compute"
        assert result.product_type == "kafka"
        assert result.identity_id == "u1"
        assert result.cost_type == CostType.USAGE
        assert result.amount == Decimal("50.00")
        assert result.allocation_method == "direct"

    def test_dimension_cache_hit_returns_same_python_object(self, session: Session) -> None:
        """chargeback_to_domain receives the same ChargebackDimensionTable object on cache hit (identity check)."""
        repo = SQLModelChargebackRepository(session)
        row1 = self._make_row(timestamp=datetime(2026, 1, 15, tzinfo=UTC))
        row2 = self._make_row(timestamp=datetime(2026, 1, 16, tzinfo=UTC))

        repo.upsert(row1)
        key = repo._make_dimension_key(row1)
        dim1 = repo._dimension_cache[key]

        repo.upsert(row2)
        dim2 = repo._dimension_cache[key]

        assert dim1 is dim2  # exact same Python object — not a re-fetched copy

    def test_nullable_fields_produce_correct_cache_keys(self, session: Session) -> None:
        """resource_id=None and allocation_detail=None produce correct, non-colliding cache keys."""
        repo = SQLModelChargebackRepository(session)

        row_none_resource = self._make_row(resource_id=None, identity_id="u1")
        row_none_detail = self._make_row(resource_id="r1", allocation_detail=None, identity_id="u2")

        key1 = repo._make_dimension_key(row_none_resource)
        key2 = repo._make_dimension_key(row_none_detail)

        assert key1 != key2
        assert key1[2] is None  # resource_id at index 2 in the key tuple
        assert key2[-1] is None  # allocation_detail is last in the key tuple

    def test_cache_scope_independent_per_instance(self, session: Session) -> None:
        """Two separate SQLModelChargebackRepository instances have independent _dimension_cache."""
        repo1 = SQLModelChargebackRepository(session)
        repo2 = SQLModelChargebackRepository(session)

        row = self._make_row()
        repo1.upsert(row)
        session.commit()

        assert len(repo1._dimension_cache) == 1
        assert len(repo2._dimension_cache) == 0  # repo2 cache is independent


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

    def test_mark_resources_gathered(self, session: Session) -> None:
        repo = SQLModelPipelineStateRepository(session)
        repo.upsert(self._make_state(resources_gathered=False))
        session.commit()
        repo.mark_resources_gathered("eco", "t1", date(2026, 1, 15))
        session.commit()
        got = repo.get("eco", "t1", date(2026, 1, 15))
        assert got is not None
        assert got.resources_gathered is True

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
        return CoreResource(**defaults)

    def test_basic_pagination(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(5):
            repo.upsert(self._make_resource(resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=2, offset=0, resource_type="kafka")
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
        items, total = repo.find_paginated("eco", "t1", limit=10, offset=0, resource_type="kafka", status="active")
        assert total == 1

    def test_returns_correct_total(self, session: Session) -> None:
        repo = SQLModelResourceRepository(session)
        for i in range(10):
            repo.upsert(self._make_resource(resource_id=f"r{i}"))
        session.commit()
        items, total = repo.find_paginated("eco", "t1", limit=3, offset=6, resource_type="kafka")
        assert total == 10
        assert len(items) == 3


class TestIdentityFindPaginated:
    def test_basic(self, session: Session) -> None:
        repo = SQLModelIdentityRepository(session)
        for i in range(3):
            repo.upsert(
                CoreIdentity(
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
            CoreIdentity(
                ecosystem="eco",
                tenant_id="t1",
                identity_id="u1",
                identity_type="user",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
        repo.upsert(
            CoreIdentity(
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
        return CoreBillingLineItem(**defaults)

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
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()
        assert tag.tag_id is not None
        assert tag.tag_key == "team"
        assert tag.tag_value == "platform"

        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 1

    def test_delete_tag(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        session.commit()
        assert tag.tag_id is not None
        repo.delete_tag(tag.tag_id)
        session.commit()
        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 0

    def test_multiple_tags_per_entity(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()
        tags = repo.get_tags("t1", "resource", "r1")
        assert len(tags) == 2

    def test_update_tag(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        tag = repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()
        assert tag.tag_id is not None
        updated = repo.update_tag(tag.tag_id, "staging")
        session.commit()
        assert updated.tag_value == "staging"
        assert updated.tag_key == "env"

    def test_find_tags_for_tenant(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        repo.add_tag("t1", "resource", "r1", "team", "platform", "admin")
        repo.add_tag("t1", "resource", "r1", "env", "prod", "admin")
        session.commit()

        items, total = repo.find_tags_for_tenant(tenant_id="t1")
        assert total == 2
        assert len(items) == 2

    def test_find_tags_for_tenant_empty(self, session: Session) -> None:
        repo = SQLModelEntityTagRepository(session)
        items, total = repo.find_tags_for_tenant(tenant_id="no-tenant")
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


# --- count parameter (task-043) ---


class TestResourceRepositoryCountParam:
    """Tests for the count: bool = True parameter on SQLModelResourceRepository."""

    def _make_resource(self, **overrides: Any) -> Resource:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return CoreResource(**defaults)

    def test_find_active_at_count_false_returns_zero_total(self, session: Session) -> None:
        """find_active_at(count=False) returns (items, 0) without issuing SELECT COUNT(*)."""
        from unittest.mock import patch

        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource())
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_active_at(
                "eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka", count=False
            )

        assert total == 0
        assert len(results) == 1
        assert len(exec_calls) == 1  # only main SELECT, no COUNT query

    def test_find_active_at_count_true_returns_actual_total(self, session: Session) -> None:
        """find_active_at(count=True) returns actual count — existing behaviour preserved."""
        from unittest.mock import patch

        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource())
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_active_at(
                "eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), resource_type="kafka", count=True
            )

        assert total == 1
        assert len(results) == 1
        assert len(exec_calls) == 2  # COUNT query + main SELECT

    def test_find_by_period_count_false_returns_zero_total(self, session: Session) -> None:
        """find_by_period(count=False) returns (items, 0) without issuing SELECT COUNT(*)."""
        from unittest.mock import patch

        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 5, tzinfo=UTC)))
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_by_period(
                "eco",
                "t1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                resource_type="kafka",
                count=False,
            )

        assert total == 0
        assert len(results) == 1
        assert len(exec_calls) == 1  # only main SELECT, no COUNT query

    def test_find_by_period_count_true_returns_actual_total(self, session: Session) -> None:
        """find_by_period(count=True) returns actual count — existing behaviour preserved."""
        from unittest.mock import patch

        repo = SQLModelResourceRepository(session)
        repo.upsert(self._make_resource(created_at=datetime(2026, 1, 5, tzinfo=UTC)))
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_by_period(
                "eco",
                "t1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                resource_type="kafka",
                count=True,
            )

        assert total == 1
        assert len(results) == 1
        assert len(exec_calls) == 2  # COUNT query + main SELECT


class TestIdentityRepositoryCountParam:
    """Tests for the count: bool = True parameter on SQLModelIdentityRepository."""

    def _make_identity(self, **overrides: Any) -> Identity:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="u1",
            identity_type="user",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        defaults.update(overrides)
        return CoreIdentity(**defaults)

    def test_find_active_at_count_false_returns_zero_total(self, session: Session) -> None:
        """find_active_at(count=False) returns (items, 0) without issuing SELECT COUNT(*)."""
        from unittest.mock import patch

        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity())
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), count=False)

        assert total == 0
        assert len(results) == 1
        assert len(exec_calls) == 1  # only main SELECT, no COUNT query

    def test_find_active_at_count_true_returns_actual_total(self, session: Session) -> None:
        """find_active_at(count=True) returns actual count — existing behaviour preserved."""
        from unittest.mock import patch

        repo = SQLModelIdentityRepository(session)
        repo.upsert(self._make_identity())
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_active_at("eco", "t1", datetime(2026, 1, 15, tzinfo=UTC), count=True)

        assert total == 1
        assert len(results) == 1
        assert len(exec_calls) == 2  # COUNT query + main SELECT

    def test_find_by_period_count_false_returns_zero_total(self, session: Session) -> None:
        """find_by_period(count=False) returns (items, 0) without issuing SELECT COUNT(*)."""
        from unittest.mock import patch

        repo = SQLModelIdentityRepository(session)
        repo.upsert(
            self._make_identity(
                created_at=datetime(2026, 1, 5, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
            )
        )
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_by_period(
                "eco",
                "t1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                count=False,
            )

        assert total == 0
        assert len(results) == 1
        assert len(exec_calls) == 1  # only main SELECT, no COUNT query

    def test_find_by_period_count_true_returns_actual_total(self, session: Session) -> None:
        """find_by_period(count=True) returns actual count — existing behaviour preserved."""
        from unittest.mock import patch

        repo = SQLModelIdentityRepository(session)
        repo.upsert(
            self._make_identity(
                created_at=datetime(2026, 1, 5, tzinfo=UTC),
                deleted_at=datetime(2026, 1, 20, tzinfo=UTC),
            )
        )
        session.commit()

        exec_calls: list[Any] = []
        original_exec = session.exec

        def tracking_exec(stmt: Any, *args: Any, **kwargs: Any) -> Any:
            exec_calls.append(stmt)
            return original_exec(stmt, *args, **kwargs)

        with patch.object(session, "exec", side_effect=tracking_exec):
            results, total = repo.find_by_period(
                "eco",
                "t1",
                datetime(2026, 1, 1, tzinfo=UTC),
                datetime(2026, 2, 1, tzinfo=UTC),
                count=True,
            )

        assert total == 1
        assert len(results) == 1
        assert len(exec_calls) == 2  # COUNT query + main SELECT


# ---------------------------------------------------------------------------
# Cache behaviour — PERF-M7
# Strategy: `session.expire_all()` clears SQLAlchemy's identity map so that
# only the repository-level TTLCache can serve a repeated get().  When the
# TTLCache is present `session.get` is never invoked on a cache hit
# (call_count stays at 0); without it the count would increment on every call.
# ---------------------------------------------------------------------------


class TestIdentityRepositoryCache:
    def _make_identity(self, **overrides: Any) -> CoreIdentity:
        defaults: dict[str, Any] = dict(
            ecosystem="eco",
            tenant_id="t1",
            identity_id="sa-001",
            identity_type="service_account",
            display_name="Test SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return CoreIdentity(**defaults)

    def test_identity_get_same_key_twice_issues_one_db_call(self, session: Session) -> None:
        """Verification #1: same key twice issues exactly one session.get() call."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(self._make_identity())
        session.commit()

        # First call: DB hit; populates repo-level cache.
        repo.get("eco", "t1", "sa-001")
        # Clear SQLAlchemy identity map so only TTLCache can serve the second call.
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            # With TTLCache: session.get not invoked (count == 0).
            # Without TTLCache: it would be invoked (count == 1) — RED state.
            assert mock_get.call_count == 0

    def test_identity_get_nonexistent_caches_none(self, session: Session) -> None:
        """Verification #2: None result is cached; second call skips DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)

        result1 = repo.get("eco", "t1", "nonexistent")
        assert result1 is None

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result2 = repo.get("eco", "t1", "nonexistent")
            assert result2 is None
            assert mock_get.call_count == 0

    def test_identity_upsert_invalidates_cache(self, session: Session) -> None:
        """Verification #3: upsert() invalidates cache; subsequent get() hits DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(self._make_identity(display_name="Original"))
        session.commit()

        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        # Confirm cache live (pre-condition).
        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0

        repo.upsert(self._make_identity(display_name="Updated"))
        session.commit()
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # cache invalidated → DB hit

        assert result is not None
        assert result.display_name == "Updated"

    def test_identity_mark_deleted_invalidates_cache(self, session: Session) -> None:
        """Verification #4: mark_deleted() invalidates cache; subsequent get() hits DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(self._make_identity())
        session.commit()

        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0

        repo.mark_deleted("eco", "t1", "sa-001", datetime(2026, 2, 1, tzinfo=UTC))
        session.commit()
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # cache invalidated → DB hit

    def test_identity_cache_entry_expires_after_ttl(self, session: Session) -> None:
        """Verification #5: after cache_ttl_seconds, get() re-queries the DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=0.05)
        repo.upsert(self._make_identity())
        session.commit()

        repo.get("eco", "t1", "sa-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 0  # within TTL: cache hit

        time.sleep(0.15)

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # TTL expired → DB hit

    def test_identity_cache_evicts_lru_when_full(self, session: Session) -> None:
        """Verification #6: LRU entry evicted when maxsize exceeded; evicted key re-queries DB."""
        repo = SQLModelIdentityRepository(session, cache_maxsize=2, cache_ttl_seconds=300.0)

        for i in range(1, 4):
            repo.upsert(self._make_identity(identity_id=f"sa-{i:03d}"))
        session.commit()

        # Fill cache: sa-001 (LRU), sa-002 (MRU).
        repo.get("eco", "t1", "sa-001")
        repo.get("eco", "t1", "sa-002")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            repo.get("eco", "t1", "sa-002")
            assert mock_get.call_count == 0

        # sa-003 fills cache → evicts sa-001 (LRU, accessed least recently).
        repo.get("eco", "t1", "sa-003")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # evicted → DB hit required

    def test_two_identity_repo_instances_have_independent_caches(self, session: Session) -> None:
        """Verification #9: two instances must not share cache state."""
        repo1 = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo2 = SQLModelIdentityRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)

        repo1.upsert(self._make_identity())
        session.commit()

        repo1.get("eco", "t1", "sa-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo2.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # repo2 has no cache entry → DB hit

            repo1.get("eco", "t1", "sa-001")
            assert mock_get.call_count == 1  # repo1 cache intact → no extra DB call


class TestResourceRepositoryCache:
    def _make_resource(self, **overrides: Any) -> CoreResource:
        defaults: dict[str, Any] = dict(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="lkc-001",
            resource_type="kafka",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            metadata={},
        )
        defaults.update(overrides)
        return CoreResource(**defaults)

    def test_resource_get_same_key_twice_issues_one_db_call(self, session: Session) -> None:
        """Verification #7: resources.get() same key twice → one session.get() call."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(self._make_resource())
        session.commit()

        repo.get("eco", "t1", "lkc-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 0  # cache hit

        assert result is not None
        assert result.resource_id == "lkc-001"

    def test_resource_upsert_invalidates_cache(self, session: Session) -> None:
        """Verification #8: resources.upsert() invalidates cache entry."""
        repo = SQLModelResourceRepository(session, cache_maxsize=100, cache_ttl_seconds=300.0)
        repo.upsert(self._make_resource(display_name="Original"))
        session.commit()

        repo.get("eco", "t1", "lkc-001")
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 0

        repo.upsert(self._make_resource(display_name="Updated"))
        session.commit()
        session.expire_all()

        with patch.object(session, "get", wraps=session.get) as mock_get:
            result = repo.get("eco", "t1", "lkc-001")
            assert mock_get.call_count == 1  # cache invalidated → DB hit

        assert result is not None
        assert result.display_name == "Updated"
