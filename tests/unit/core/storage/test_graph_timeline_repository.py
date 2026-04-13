from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.graph import GraphTimelineData
from core.storage.backends.sqlmodel.base_tables import IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository, SQLModelGraphRepository
from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
    TopicAttributionDimensionTable,
    TopicAttributionFactTable,
)

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-test"

_CREATED = datetime(2026, 1, 1, tzinfo=UTC)
PERIOD_START = datetime(2026, 4, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 4, 15, tzinfo=UTC)  # 14 days exclusive


@pytest.fixture
def engine() -> Generator[Any]:
    eng = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(eng)
    yield eng
    eng.dispose(close=True)


@pytest.fixture
def session(engine: Any) -> Generator[Session]:
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session: Session) -> SQLModelGraphRepository:
    tags_repo = SQLModelEntityTagRepository(session)
    return SQLModelGraphRepository(session, tags_repo)


def _resource(
    resource_id: str, resource_type: str = "kafka_cluster", parent_id: str | None = "env-abc"
) -> ResourceTable:
    return ResourceTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=resource_id,
        parent_id=parent_id,
        status="active",
        cloud=None,
        region=None,
        created_at=_CREATED,
        deleted_at=None,
    )


def _identity(identity_id: str, identity_type: str = "service_account") -> IdentityTable:
    return IdentityTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=identity_id,
        created_at=_CREATED,
        deleted_at=None,
    )


def _dim(
    dimension_id: int,
    resource_id: str = "",
    env_id: str = "",
    identity_id: str = "",
) -> ChargebackDimensionTable:
    return ChargebackDimensionTable(
        dimension_id=dimension_id,
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        identity_id=identity_id,
        env_id=env_id,
        product_category="KAFKA",
        product_type="KAFKA_NUM_CKUS",
        cost_type="usage",
        allocation_method=None,
        allocation_detail=None,
    )


def _fact(dimension_id: int, amount: str, ts: datetime) -> ChargebackFactTable:
    return ChargebackFactTable(timestamp=ts, dimension_id=dimension_id, amount=amount)


def _topic_dim(dimension_id: int, resource_id: str, cluster_id: str, topic_name: str) -> TopicAttributionDimensionTable:
    return TopicAttributionDimensionTable(
        dimension_id=dimension_id,
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        env_id="env-abc",
        cluster_resource_id=cluster_id,
        topic_name=topic_name,
        resource_id=resource_id,
        product_category="KAFKA",
        product_type="KAFKA_TOPIC",
        attribution_method="proportional",
    )


def _topic_fact(dimension_id: int, amount: str, ts: datetime) -> TopicAttributionFactTable:
    return TopicAttributionFactTable(timestamp=ts, dimension_id=dimension_id, amount=amount)


class TestGraphTimelineRepositoryResource:
    def test_timeline_resource_returns_daily_points(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V13: cluster resource with chargeback facts → daily cost points."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.add(_dim(1, resource_id="lkc-abc", env_id="env-abc"))
        session.add(_fact(1, "30.00", ts=datetime(2026, 4, 5, tzinfo=UTC)))
        session.add(_fact(1, "20.00", ts=datetime(2026, 4, 10, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-abc", PERIOD_START, PERIOD_END)

        assert isinstance(result, list)
        assert all(isinstance(p, GraphTimelineData) for p in result)
        costs_by_date = {p.date: p.cost for p in result}
        assert costs_by_date.get(date(2026, 4, 5)) == Decimal("30.00")
        assert costs_by_date.get(date(2026, 4, 10)) == Decimal("20.00")

    def test_timeline_resource_result_is_sorted_by_date(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V13: timeline points are in ascending date order (gap filling guarantees it)."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.add(_dim(1, resource_id="lkc-abc", env_id="env-abc"))
        session.add(_fact(1, "10.00", ts=datetime(2026, 4, 5, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-abc", PERIOD_START, PERIOD_END)

        dates = [p.date for p in result]
        assert dates == sorted(dates)


class TestGraphTimelineRepositoryTopic:
    def test_timeline_topic_uses_topic_attribution_facts(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V14: topic resource → costs from topic_attribution_facts (not chargeback_facts)."""
        topic_resource_id = "lkc-abc:topic:orders"
        session.add(_resource(topic_resource_id, "topic", parent_id="lkc-abc"))
        session.add(_topic_dim(1, topic_resource_id, "lkc-abc", "orders"))
        session.add(_topic_fact(1, "25.00", ts=datetime(2026, 4, 3, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, topic_resource_id, PERIOD_START, PERIOD_END)

        costs_by_date = {p.date: p.cost for p in result}
        assert costs_by_date.get(date(2026, 4, 3)) == Decimal("25.00")


class TestGraphTimelineRepositoryEnvironment:
    def test_timeline_environment_uses_env_id_grouping(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V15: environment node timeline uses env_id grouping (not resource_id)."""
        session.add(_resource("env-abc", "environment", parent_id=None))
        # Charge billed via env_id=env-abc, resource_id is a cluster
        session.add(_dim(2, resource_id="lkc-xyz", env_id="env-abc"))
        session.add(_fact(2, "45.00", ts=datetime(2026, 4, 7, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "env-abc", PERIOD_START, PERIOD_END)

        costs_by_date = {p.date: p.cost for p in result}
        assert costs_by_date.get(date(2026, 4, 7)) == Decimal("45.00")


class TestGraphTimelineRepositoryIdentity:
    def test_timeline_identity_uses_identity_id_grouping(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V16: identity timeline uses identity_id grouping from chargeback_facts."""
        session.add(_identity("sa-001"))
        session.add(_dim(3, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(3, "15.00", ts=datetime(2026, 4, 2, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "sa-001", PERIOD_START, PERIOD_END)

        costs_by_date = {p.date: p.cost for p in result}
        assert costs_by_date.get(date(2026, 4, 2)) == Decimal("15.00")


class TestGraphTimelineRepositoryGapFilling:
    def test_timeline_gap_filling_returns_one_entry_per_day(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V17: 14-day range [2026-04-01, 2026-04-15) → exactly 14 entries returned."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.add(_dim(4, resource_id="lkc-abc", env_id="env-abc"))
        # Only 3 days have billing data
        for day in [3, 7, 11]:
            session.add(_fact(4, "10.00", ts=datetime(2026, 4, day, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-abc", PERIOD_START, PERIOD_END)

        assert len(result) == 14

    def test_timeline_gap_days_have_cost_zero(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V17: days with no billing data are present with cost=0."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.add(_dim(4, resource_id="lkc-abc", env_id="env-abc"))
        session.add(_fact(4, "10.00", ts=datetime(2026, 4, 5, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-abc", PERIOD_START, PERIOD_END)

        costs_by_date = {p.date: p.cost for p in result}
        # All 14 days present
        assert len(costs_by_date) == 14
        # Day 5 has data; day 1 does not
        assert costs_by_date[date(2026, 4, 5)] == Decimal("10.00")
        assert costs_by_date[date(2026, 4, 1)] == Decimal("0")

    def test_timeline_covers_full_range_start_to_end_exclusive(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap filling covers [start.date(), end.date()) exclusive — end day not included."""
        session.add(_resource("lkc-abc", "kafka_cluster"))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-abc", PERIOD_START, PERIOD_END)

        dates = {p.date for p in result}
        assert date(2026, 4, 1) in dates  # first day included
        assert date(2026, 4, 14) in dates  # last day in range (end=Apr 15, exclusive)
        assert date(2026, 4, 15) not in dates  # end day excluded


class TestGraphTimelineRepositoryMissingEntity:
    def test_timeline_missing_entity_raises_key_error(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V18: entity_id not found in resources or identities → raises KeyError."""
        with pytest.raises(KeyError):
            repo.get_timeline(ECOSYSTEM, TENANT_ID, "nonexistent-entity", PERIOD_START, PERIOD_END)

    def test_timeline_key_error_contains_entity_id(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """KeyError message contains the missing entity_id."""
        with pytest.raises(KeyError, match="nonexistent-entity"):
            repo.get_timeline(ECOSYSTEM, TENANT_ID, "nonexistent-entity", PERIOD_START, PERIOD_END)


class TestGraphTimelineRepositoryTenantIsolation:
    def test_timeline_does_not_return_other_tenant_entity_costs(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V19: costs for same entity_id belonging to another tenant are excluded."""
        # Add entity for TENANT_ID with no cost
        session.add(_resource("lkc-shared-id", "kafka_cluster"))

        # Add dimension and fact for a DIFFERENT tenant using same entity_id string
        other_dim = ChargebackDimensionTable(
            dimension_id=99,
            ecosystem=ECOSYSTEM,
            tenant_id="other-tenant",
            resource_id="lkc-shared-id",
            identity_id="",
            env_id="env-other",
            product_category="KAFKA",
            product_type="KAFKA_NUM_CKUS",
            cost_type="usage",
            allocation_method=None,
            allocation_detail=None,
        )
        session.add(other_dim)
        session.add(_fact(99, "999.00", ts=datetime(2026, 4, 5, tzinfo=UTC)))
        session.commit()

        result = repo.get_timeline(ECOSYSTEM, TENANT_ID, "lkc-shared-id", PERIOD_START, PERIOD_END)

        # All costs should be 0 since the facts belong to other-tenant
        total = sum(p.cost for p in result)
        assert total == Decimal("0")
