from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

from core.models.graph import CrossReferenceGroup
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

AT = datetime(2026, 3, 15, tzinfo=UTC)
PERIOD_START = datetime(2026, 3, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 4, 1, tzinfo=UTC)

_CREATED = datetime(2026, 1, 1, tzinfo=UTC)


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


@pytest.fixture
def tags_repo(session: Session) -> SQLModelEntityTagRepository:
    return SQLModelEntityTagRepository(session)


def _resource(
    resource_id: str,
    resource_type: str,
    parent_id: str | None = None,
    created_at: datetime | None = None,
    deleted_at: datetime | None = None,
    display_name: str | None = None,
    status: str = "active",
) -> ResourceTable:
    return ResourceTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=display_name or resource_id,
        parent_id=parent_id,
        status=status,
        cloud=None,
        region=None,
        created_at=created_at or _CREATED,
        deleted_at=deleted_at,
    )


def _identity(
    identity_id: str,
    identity_type: str = "service_account",
    created_at: datetime | None = None,
    deleted_at: datetime | None = None,
) -> IdentityTable:
    return IdentityTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=identity_id,
        created_at=created_at or _CREATED,
        deleted_at=deleted_at,
    )


def _dim(
    dimension_id: int,
    resource_id: str,
    env_id: str,
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


def _fact(
    dimension_id: int,
    amount: str,
    ts: datetime | None = None,
) -> ChargebackFactTable:
    return ChargebackFactTable(
        timestamp=ts or datetime(2026, 3, 10, tzinfo=UTC),
        dimension_id=dimension_id,
        amount=amount,
    )


class TestGraphRepositoryRootView:
    def test_root_view_returns_environment_and_tenant_nodes(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 1: focus_id=None → environment nodes + synthetic tenant node."""
        session.add(_resource("env-abc", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "environment" in node_types
        assert "tenant" in node_types
        node_ids = {n.id for n in result.nodes}
        assert "env-abc" in node_ids
        assert TENANT_ID in node_ids

    def test_root_view_edges_are_parent_type_tenant_to_env(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 1: root view edges relationship_type=parent, source=tenant_id, target=env_id."""
        session.add(_resource("env-abc", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        assert len(result.edges) == 1
        edge = result.edges[0]
        assert edge.relationship_type.value == "parent"
        assert edge.source == TENANT_ID
        assert edge.target == "env-abc"

    def test_root_cost_uses_env_id_grouping_not_resource_id(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 2: env node cost aggregated via env_id column (not resource_id)."""
        session.add(_resource("env-abc", "environment"))
        # Charge billed to env_id="env-abc", resource_id is a cluster (not the env)
        session.add(_dim(1, resource_id="lkc-xyz", env_id="env-abc"))
        session.add(_fact(1, "75.50"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.cost == Decimal("75.50")

    def test_root_view_excludes_non_environment_resources(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Root view only exposes environment nodes, not clusters or topics."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "lkc-abc" not in node_ids

    def test_root_view_multiple_envs_multiple_edges(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Root view with multiple environments produces one edge per environment."""
        session.add(_resource("env-1", "environment"))
        session.add(_resource("env-2", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_nodes = [n for n in result.nodes if n.resource_type == "environment"]
        assert len(env_nodes) == 2
        assert len(result.edges) == 2
        for edge in result.edges:
            assert edge.source == TENANT_ID
            assert edge.target in {"env-1", "env-2"}


class TestGraphRepositoryEnvironmentFocus:
    def test_env_focus_returns_env_and_direct_children_only(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 5: depth=1 → env + direct children; grandchildren excluded."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "env-abc" in node_ids
        assert "lkc-abc" in node_ids
        assert "lkc-abc/topic/orders" not in node_ids

    def test_env_focus_edges_direction_parent_to_child(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 5/7: parent edge source=env_id, target=cluster_id."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 1, AT, PERIOD_START, PERIOD_END)

        parent_edges = [e for e in result.edges if e.relationship_type.value == "parent"]
        assert len(parent_edges) == 1
        assert parent_edges[0].source == "env-abc"
        assert parent_edges[0].target == "lkc-abc"

    def test_env_focus_cost_uses_env_id_grouping_for_focus_node(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Environment focus node cost uses env_id aggregation (not resource_id)."""
        session.add(_resource("env-abc", "environment"))
        # Charge billed via env_id, not resource_id=env-abc
        session.add(_dim(10, resource_id="lkc-xyz", env_id="env-abc"))
        session.add(_fact(10, "200.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.cost == Decimal("200.00")


class TestGraphRepositoryClusterFocus:
    def test_cluster_focus_returns_cluster_topics_and_identities(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 6: cluster focus → cluster + topic children + identity nodes charged to cluster."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc"))
        session.add(_identity("sa-001"))
        session.add(_dim(20, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(20, "30.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "lkc-abc" in node_ids
        assert "lkc-abc/topic/orders" in node_ids
        assert "sa-001" in node_ids

    def test_cluster_focus_charge_edges_source_cluster_target_identity(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 6: charge edge source=cluster_id, target=identity_id."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-001"))
        session.add(_dim(21, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(21, "15.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        charge_edges = [e for e in result.edges if e.relationship_type.value == "charge"]
        assert len(charge_edges) == 1
        assert charge_edges[0].source == "lkc-abc"
        assert charge_edges[0].target == "sa-001"

    def test_cluster_focus_parent_edges_source_cluster_target_topic(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 6/7: parent edges source=cluster_id, target=topic_id."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        parent_edges = [e for e in result.edges if e.relationship_type.value == "parent"]
        assert len(parent_edges) == 1
        assert parent_edges[0].source == "lkc-abc"
        assert parent_edges[0].target == "lkc-abc/topic/orders"


class TestGraphRepositoryTemporalFiltering:
    def test_deleted_before_at_is_excluded(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 8: deleted_at < at → entity excluded from root view."""
        session.add(_resource("env-deleted", "environment", deleted_at=datetime(2026, 3, 1, tzinfo=UTC)))
        session.add(_resource("env-alive", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "env-deleted" not in node_ids
        assert "env-alive" in node_ids

    def test_created_after_at_is_excluded(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 8: created_at > at → entity excluded."""
        session.add(_resource("env-future", "environment", created_at=datetime(2026, 4, 1, tzinfo=UTC)))
        session.add(_resource("env-alive", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "env-future" not in node_ids
        assert "env-alive" in node_ids

    def test_entity_alive_at_at_with_no_charges_has_cost_zero(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 8/9: entity alive at at with no chargeback_facts → cost=0, not omitted."""
        session.add(_resource("env-abc", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.cost == Decimal("0")

    def test_identity_deleted_before_at_excluded_even_with_charges(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 11: identity deleted_at < at is excluded even if charges exist in period."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-deleted", deleted_at=datetime(2026, 3, 1, tzinfo=UTC)))
        session.add(_dim(30, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-deleted"))
        session.add(_fact(30, "100.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "sa-deleted" not in node_ids

    def test_charges_outside_period_not_counted(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Charges timestamped outside [period_start, period_end) are excluded from cost."""
        session.add(_resource("env-abc", "environment"))
        session.add(_dim(31, resource_id="lkc-xyz", env_id="env-abc"))
        # Fact is in February, period is March → excluded
        session.add(_fact(31, "500.00", ts=datetime(2026, 2, 28, tzinfo=UTC)))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.cost == Decimal("0")


class TestGraphRepositoryCrossReferences:
    def test_identity_charged_in_multiple_clusters_has_correct_cross_references(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 10: identity charged in 3 clusters → cross_references is a list[CrossReferenceGroup] (not current)."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-1", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-2", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-3", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-001"))
        session.add(_dim(40, resource_id="lkc-1", env_id="env-abc", identity_id="sa-001"))
        session.add(_dim(41, resource_id="lkc-2", env_id="env-abc", identity_id="sa-001"))
        session.add(_dim(42, resource_id="lkc-3", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(40, "10.00"))
        session.add(_fact(41, "20.00"))
        session.add(_fact(42, "30.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-1", 1, AT, PERIOD_START, PERIOD_END)

        identity_node = next(n for n in result.nodes if n.id == "sa-001")
        # Must return list[CrossReferenceGroup], not list[str]
        assert len(identity_node.cross_references) == 1  # single group: kafka_cluster
        group = identity_node.cross_references[0]
        assert isinstance(group, CrossReferenceGroup)
        assert group.resource_type == "kafka_cluster"
        assert group.total_count == 2
        assert len(group.items) == 2
        # items sorted by cost descending: lkc-3 (30.00) first, lkc-2 (20.00) second
        assert group.items[0].id == "lkc-3"
        assert group.items[0].cost == Decimal("30.00")
        assert group.items[1].id == "lkc-2"
        assert group.items[1].cost == Decimal("20.00")
        # current cluster excluded
        assert not any(item.id == "lkc-1" for item in group.items)

    def test_identity_with_no_cross_refs_has_empty_list(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 10: pre-initialized cross_ref_map — identity with no other clusters gets []."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-only"))
        session.add(_dim(50, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-only"))
        session.add(_fact(50, "10.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        identity_node = next(n for n in result.nodes if n.id == "sa-only")
        assert identity_node.cross_references == []

    def test_fetch_cross_references_empty_identity_ids_returns_empty_dict(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """_fetch_cross_references([]) → empty dict immediately, no DB query needed."""
        result = repo._fetch_cross_references(ECOSYSTEM, TENANT_ID, [], "lkc-1", PERIOD_START, PERIOD_END)
        assert result == {}

    def test_cross_reference_items_sorted_by_cost_descending(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Items within a CrossReferenceGroup are ordered by cost DESC."""
        session.add(_resource("env-x", "environment"))
        session.add(_resource("lkc-focus", "kafka_cluster", parent_id="env-x"))
        session.add(_resource("lkc-cheap", "kafka_cluster", parent_id="env-x"))
        session.add(_resource("lkc-mid", "kafka_cluster", parent_id="env-x"))
        session.add(_resource("lkc-expensive", "kafka_cluster", parent_id="env-x"))
        session.add(_identity("sa-sorted"))
        session.add(_dim(60, resource_id="lkc-focus", env_id="env-x", identity_id="sa-sorted"))
        session.add(_dim(61, resource_id="lkc-cheap", env_id="env-x", identity_id="sa-sorted"))
        session.add(_dim(62, resource_id="lkc-mid", env_id="env-x", identity_id="sa-sorted"))
        session.add(_dim(63, resource_id="lkc-expensive", env_id="env-x", identity_id="sa-sorted"))
        session.add(_fact(60, "5.00"))
        session.add(_fact(61, "1.00"))
        session.add(_fact(62, "50.00"))
        session.add(_fact(63, "200.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-focus", 1, AT, PERIOD_START, PERIOD_END)

        identity_node = next(n for n in result.nodes if n.id == "sa-sorted")
        assert len(identity_node.cross_references) == 1
        group = identity_node.cross_references[0]
        assert isinstance(group, CrossReferenceGroup)
        costs = [item.cost for item in group.items]
        assert costs == sorted(costs, reverse=True)
        assert group.items[0].id == "lkc-expensive"

    def test_identity_with_single_resource_type_returns_single_group_with_no_overflow(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Identity cross-refs in 1 resource type, count ≤ 5 → single group, total_count == len(items)."""
        session.add(_resource("env-y", "environment"))
        session.add(_resource("lkc-main", "kafka_cluster", parent_id="env-y"))
        session.add(_resource("lkc-a", "kafka_cluster", parent_id="env-y"))
        session.add(_resource("lkc-b", "kafka_cluster", parent_id="env-y"))
        session.add(_identity("sa-few"))
        session.add(_dim(70, resource_id="lkc-main", env_id="env-y", identity_id="sa-few"))
        session.add(_dim(71, resource_id="lkc-a", env_id="env-y", identity_id="sa-few"))
        session.add(_dim(72, resource_id="lkc-b", env_id="env-y", identity_id="sa-few"))
        session.add(_fact(70, "10.00"))
        session.add(_fact(71, "20.00"))
        session.add(_fact(72, "30.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-main", 1, AT, PERIOD_START, PERIOD_END)

        identity_node = next(n for n in result.nodes if n.id == "sa-few")
        assert len(identity_node.cross_references) == 1
        group = identity_node.cross_references[0]
        assert isinstance(group, CrossReferenceGroup)
        # ≤ 5 items → total_count equals items.length (no overflow)
        assert group.total_count == len(group.items)
        assert group.total_count == 2  # lkc-a and lkc-b (lkc-main excluded)


class TestGraphRepositoryTagResolution:
    def test_resource_tags_resolved(
        self, session: Session, repo: SQLModelGraphRepository, tags_repo: SQLModelEntityTagRepository
    ) -> None:
        """Test 12: resource with entity_tags rows → tags dict populated."""
        session.add(_resource("env-abc", "environment"))
        tags_repo.add_tag(TENANT_ID, "resource", "env-abc", "team", "platform", "admin")
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.tags == {"team": "platform"}

    def test_resource_with_no_tags_returns_empty_dict(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 12: resource with no tags → tags={}."""
        session.add(_resource("env-abc", "environment"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.tags == {}

    def test_multiple_tags_resolved(
        self, session: Session, repo: SQLModelGraphRepository, tags_repo: SQLModelEntityTagRepository
    ) -> None:
        """Multiple tags on same resource → all resolved."""
        session.add(_resource("env-abc", "environment"))
        tags_repo.add_tag(TENANT_ID, "resource", "env-abc", "team", "platform", "admin")
        tags_repo.add_tag(TENANT_ID, "resource", "env-abc", "env", "prod", "admin")
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, None, 1, AT, PERIOD_START, PERIOD_END)

        env_node = next(n for n in result.nodes if n.id == "env-abc")
        assert env_node.tags == {"team": "platform", "env": "prod"}


class TestGraphRepositoryInvalidFocus:
    def test_unknown_focus_id_raises_key_error(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 13: focus_id not found in resources → KeyError raised (route converts to 404)."""
        with pytest.raises(KeyError, match="does-not-exist"):
            repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "does-not-exist", 1, AT, PERIOD_START, PERIOD_END)


class TestGraphRepositoryBFSDepth:
    def test_bfs_depth_2_returns_two_levels(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 16: depth=2 → env + clusters (depth 1) + topics (depth 2)."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 2, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "env-abc" in node_ids
        assert "lkc-abc" in node_ids
        assert "lkc-abc/topic/orders" in node_ids

    def test_bfs_depth_1_excludes_grandchildren(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """Test 5/16: depth=1 stops at direct children; grandchildren excluded."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "lkc-abc/topic/orders" not in node_ids

    def test_bfs_terminates_early_when_no_children_at_level(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Test 16: BFS with depth=3 but only 1 level of children terminates without error."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        # No topic children — BFS stops at depth 1
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "env-abc", 3, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "env-abc" in node_ids
        assert "lkc-abc" in node_ids


# ---------------------------------------------------------------------------
# Helpers for Gap A / Gap B tests
# ---------------------------------------------------------------------------


def _topic_dim(
    dimension_id: int,
    resource_id: str,
    cluster_id: str,
    topic_name: str,
) -> TopicAttributionDimensionTable:
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


def _topic_fact(
    dimension_id: int,
    amount: str,
    ts: datetime | None = None,
) -> TopicAttributionFactTable:
    return TopicAttributionFactTable(
        timestamp=ts or datetime(2026, 3, 10, tzinfo=UTC),
        dimension_id=dimension_id,
        amount=amount,
    )


# ---------------------------------------------------------------------------
# Gap A — Identity Focus (_identity_view)
# ---------------------------------------------------------------------------


class TestGraphRepositoryIdentityFocus:
    def test_identity_focus_returns_identity_node_and_charged_clusters(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap A: identity focus returns identity node + clusters it's charged in."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-001"))
        session.add(_dim(100, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(100, "50.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "sa-001", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "sa-001" in node_ids
        assert "lkc-abc" in node_ids

    def test_identity_focus_charge_edges_have_per_cluster_costs(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap A: charge edges carry per-cluster cost from chargeback_facts."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-1", "kafka_cluster", parent_id="env-abc"))
        session.add(_resource("lkc-2", "kafka_cluster", parent_id="env-abc"))
        session.add(_identity("sa-001"))
        session.add(_dim(101, resource_id="lkc-1", env_id="env-abc", identity_id="sa-001"))
        session.add(_dim(102, resource_id="lkc-2", env_id="env-abc", identity_id="sa-001"))
        session.add(_fact(101, "30.00"))
        session.add(_fact(102, "70.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "sa-001", 1, AT, PERIOD_START, PERIOD_END)

        charge_edges = [e for e in result.edges if e.relationship_type.value == "charge"]
        costs_by_cluster = {e.source: e.cost for e in charge_edges}
        assert costs_by_cluster["lkc-1"] == Decimal("30.00")
        assert costs_by_cluster["lkc-2"] == Decimal("70.00")

    def test_identity_focus_with_no_chargeback_data_returns_identity_node_only(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap A: identity with no chargeback_dimension rows → identity node only, no cluster nodes."""
        session.add(_identity("sa-lonely"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "sa-lonely", 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        assert "sa-lonely" in node_ids
        assert len(node_ids) == 1
        assert result.edges == []

    def test_identity_focus_unknown_identity_raises_key_error(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap A: focus_id not in ResourceTable or IdentityTable → KeyError (404)."""
        with pytest.raises(KeyError, match="sa-ghost"):
            repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "sa-ghost", 1, AT, PERIOD_START, PERIOD_END)

    def test_identity_focus_deleted_identity_has_deleted_status(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap A: identity with deleted_at set → identity node status='deleted'."""
        deleted_at = datetime(2026, 2, 1, tzinfo=UTC)
        session.add(_identity("sa-old", deleted_at=deleted_at))
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        session.add(_dim(103, resource_id="lkc-abc", env_id="env-abc", identity_id="sa-old"))
        session.add(_fact(103, "25.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "sa-old", 1, AT, PERIOD_START, PERIOD_END)

        identity_node = next(n for n in result.nodes if n.id == "sa-old")
        assert identity_node.status == "deleted"


# ---------------------------------------------------------------------------
# Gap B — Topic Attribution Edges in _cluster_view
# ---------------------------------------------------------------------------


class TestGraphRepositoryTopicAttributionEdges:
    def test_cluster_view_includes_attribution_edges_when_ta_facts_exist(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap B: cluster view emits EdgeType.attribution edges when topic_attribution_facts exist."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        topic_id = "lkc-abc/topic/orders"
        session.add(_resource(topic_id, "kafka_topic", parent_id="lkc-abc"))
        session.add(_topic_dim(200, resource_id=topic_id, cluster_id="lkc-abc", topic_name="orders"))
        session.add(_topic_fact(200, "40.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        attribution_edges = [e for e in result.edges if e.relationship_type.value == "attribution"]
        assert len(attribution_edges) == 1
        assert attribution_edges[0].source == "lkc-abc"
        assert attribution_edges[0].target == topic_id

    def test_attribution_edge_costs_match_aggregated_ta_facts(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap B: attribution edge cost = sum of topic_attribution_facts for that topic."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        topic_id = "lkc-abc/topic/payments"
        session.add(_resource(topic_id, "kafka_topic", parent_id="lkc-abc"))
        session.add(_topic_dim(201, resource_id=topic_id, cluster_id="lkc-abc", topic_name="payments"))
        session.add(_topic_fact(201, "15.00"))
        session.add(_topic_fact(201, "25.00", ts=datetime(2026, 3, 11, tzinfo=UTC)))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        attribution_edges = [e for e in result.edges if e.relationship_type.value == "attribution"]
        assert len(attribution_edges) == 1
        assert attribution_edges[0].cost == Decimal("40.00")

    def test_topic_node_cost_updated_from_attribution_data(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap B: topic node cost comes from ta_cost_map, not chargeback (which gives 0)."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        topic_id = "lkc-abc/topic/events"
        session.add(_resource(topic_id, "kafka_topic", parent_id="lkc-abc"))
        session.add(_topic_dim(202, resource_id=topic_id, cluster_id="lkc-abc", topic_name="events"))
        session.add(_topic_fact(202, "55.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        topic_node = next(n for n in result.nodes if n.id == topic_id)
        assert topic_node.cost == Decimal("55.00")

    def test_cluster_view_with_no_ta_data_has_no_attribution_edges_and_topic_cost_zero(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap B: no topic_attribution_facts → no attribution edges, topic node cost=0."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        topic_id = "lkc-abc/topic/logs"
        session.add(_resource(topic_id, "kafka_topic", parent_id="lkc-abc"))
        # No topic attribution dimension or fact rows
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        attribution_edges = [e for e in result.edges if e.relationship_type.value == "attribution"]
        assert attribution_edges == []

        topic_node = next(n for n in result.nodes if n.id == topic_id)
        assert topic_node.cost == Decimal("0")

    def test_attribution_edges_only_emitted_for_topics_in_resources_table(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """Gap B: ta_cost_map entry for a topic not in resource_nodes → no attribution edge emitted."""
        session.add(_resource("env-abc", "environment"))
        session.add(_resource("lkc-abc", "kafka_cluster", parent_id="env-abc"))
        real_topic_id = "lkc-abc/topic/real"
        session.add(_resource(real_topic_id, "kafka_topic", parent_id="lkc-abc"))
        ghost_topic_id = "lkc-abc/topic/ghost"
        session.add(_topic_dim(203, resource_id=real_topic_id, cluster_id="lkc-abc", topic_name="real"))
        session.add(_topic_dim(204, resource_id=ghost_topic_id, cluster_id="lkc-abc", topic_name="ghost"))
        session.add(_topic_fact(203, "10.00"))
        session.add(_topic_fact(204, "20.00"))
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, "lkc-abc", 1, AT, PERIOD_START, PERIOD_END)

        attribution_edges = [e for e in result.edges if e.relationship_type.value == "attribution"]
        edge_targets = {e.target for e in attribution_edges}
        assert real_topic_id in edge_targets
        assert ghost_topic_id not in edge_targets
