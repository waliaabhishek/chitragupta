from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.graph import GraphDiffNodeData
from core.storage.backends.sqlmodel.base_tables import ResourceTable
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository, SQLModelGraphRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-test"

# Two non-overlapping billing periods for diff tests
FROM_START = datetime(2026, 3, 1, tzinfo=UTC)
FROM_END = datetime(2026, 4, 1, tzinfo=UTC)
TO_START = datetime(2026, 4, 1, tzinfo=UTC)
TO_END = datetime(2026, 5, 1, tzinfo=UTC)

_CREATED_EARLY = datetime(2026, 1, 1, tzinfo=UTC)
_CREATED_AFTER_FROM = datetime(2026, 4, 15, tzinfo=UTC)  # created after FROM_END → absent in before view
_DELETED_AFTER_FROM = datetime(
    2026, 4, 15, tzinfo=UTC
)  # deleted after FROM_END but before TO_END → absent in after view


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


def _env(
    env_id: str,
    created_at: datetime = _CREATED_EARLY,
    deleted_at: datetime | None = None,
) -> ResourceTable:
    return ResourceTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=env_id,
        resource_type="environment",
        display_name=env_id,
        parent_id=None,
        status="active" if deleted_at is None else "deleted",
        cloud=None,
        region=None,
        created_at=created_at,
        deleted_at=deleted_at,
    )


def _dim(dimension_id: int, env_id: str, resource_id: str = "lkc-placeholder") -> ChargebackDimensionTable:
    return ChargebackDimensionTable(
        dimension_id=dimension_id,
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        identity_id="",
        env_id=env_id,
        product_category="KAFKA",
        product_type="KAFKA_NUM_CKUS",
        cost_type="usage",
        allocation_method=None,
        allocation_detail=None,
    )


def _fact(dimension_id: int, amount: str, ts: datetime) -> ChargebackFactTable:
    return ChargebackFactTable(timestamp=ts, dimension_id=dimension_id, amount=amount)


class TestGraphDiffRepositoryNewEntity:
    def test_diff_new_entity_has_status_new_and_cost_before_zero(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V9: entity only in 'to' window → status=new, cost_before=0."""
        # env-new created after FROM_END → absent in before view, present in after
        session.add(_env("env-new", created_at=_CREATED_AFTER_FROM))
        session.add(_dim(1, "env-new"))
        session.add(_fact(1, "80.00", ts=datetime(2026, 4, 20, tzinfo=UTC)))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        new_nodes = [n for n in diff if n.id == "env-new"]
        assert len(new_nodes) == 1
        node = new_nodes[0]
        assert node.status == "new"
        assert node.cost_before == Decimal("0")
        assert node.cost_after == Decimal("80.00")

    def test_diff_new_entity_pct_change_is_none(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V11: new entity (cost_before=0) → pct_change=None."""
        session.add(_env("env-new", created_at=_CREATED_AFTER_FROM))
        session.add(_dim(1, "env-new"))
        session.add(_fact(1, "50.00", ts=datetime(2026, 4, 20, tzinfo=UTC)))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        new_nodes = [n for n in diff if n.id == "env-new"]
        assert len(new_nodes) == 1
        assert new_nodes[0].pct_change is None


class TestGraphDiffRepositoryDeletedEntity:
    def test_diff_deleted_entity_has_status_deleted_and_cost_after_zero(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V10: entity only in 'from' window → status=deleted, cost_after=0."""
        # env-deleted: present at FROM_END (deleted_at > FROM_END), absent at TO_END (deleted_at <= TO_END)
        session.add(_env("env-deleted", deleted_at=_DELETED_AFTER_FROM))
        session.add(_dim(2, "env-deleted"))
        session.add(_fact(2, "60.00", ts=datetime(2026, 3, 10, tzinfo=UTC)))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        deleted_nodes = [n for n in diff if n.id == "env-deleted"]
        assert len(deleted_nodes) == 1
        node = deleted_nodes[0]
        assert node.status == "deleted"
        assert node.cost_after == Decimal("0")
        assert node.cost_before == Decimal("60.00")

    def test_diff_deleted_entity_pct_change_is_none(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V11: deleted entity always has pct_change=None (design decision)."""
        session.add(_env("env-deleted", deleted_at=_DELETED_AFTER_FROM))
        session.add(_dim(2, "env-deleted"))
        session.add(_fact(2, "60.00", ts=datetime(2026, 3, 10, tzinfo=UTC)))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        deleted_nodes = [n for n in diff if n.id == "env-deleted"]
        assert deleted_nodes[0].pct_change is None


class TestGraphDiffRepositoryChangedEntity:
    def test_diff_changed_entity_has_correct_cost_delta_and_pct(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Entity in both windows with different costs → status='changed', correct cost_delta and pct_change."""
        session.add(_env("env-both"))
        session.add(_dim(3, "env-both"))
        session.add(_fact(3, "100.00", ts=datetime(2026, 3, 15, tzinfo=UTC)))  # in before period
        session.add(_fact(3, "150.00", ts=datetime(2026, 4, 10, tzinfo=UTC)))  # in after period
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        both_nodes = [n for n in diff if n.id == "env-both"]
        assert len(both_nodes) == 1
        node = both_nodes[0]
        assert node.status == "changed"
        assert node.cost_before == Decimal("100.00")
        assert node.cost_after == Decimal("150.00")
        assert node.cost_delta == Decimal("50.00")
        assert node.pct_change == Decimal("50.00")  # (150-100)/100 * 100

    def test_diff_unchanged_entity_has_status_unchanged(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Entity with same cost in both windows → status='unchanged', cost_delta=0."""
        session.add(_env("env-stable"))
        session.add(_dim(4, "env-stable"))
        session.add(_fact(4, "100.00", ts=datetime(2026, 3, 15, tzinfo=UTC)))  # before
        session.add(_fact(4, "100.00", ts=datetime(2026, 4, 10, tzinfo=UTC)))  # after (same amount)
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        stable_nodes = [n for n in diff if n.id == "env-stable"]
        assert len(stable_nodes) == 1
        node = stable_nodes[0]
        assert node.status == "unchanged"
        assert node.cost_delta == Decimal("0")


class TestGraphDiffRepositoryRootView:
    def test_diff_root_view_focus_none_returns_diff_node_data_list(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V12: focus_id=None passes through to find_neighborhood; returns list of GraphDiffNodeData."""
        session.add(_env("env-abc"))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        assert isinstance(diff, list)
        assert all(isinstance(n, GraphDiffNodeData) for n in diff)

    def test_diff_root_view_includes_environment_nodes(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V12: root view diff contains environment-level nodes."""
        session.add(_env("env-abc"))
        session.commit()

        diff = repo.diff_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, FROM_START, FROM_END, TO_START, TO_END)

        env_nodes = [n for n in diff if n.resource_type == "environment"]
        assert len(env_nodes) >= 1
