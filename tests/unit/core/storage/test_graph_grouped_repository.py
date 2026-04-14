from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine

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
ENV_ID = "env-abc"
CLUSTER_ID = "lkc-grp"

AT = datetime(2026, 3, 15, tzinfo=UTC)
PERIOD_START = datetime(2026, 3, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 4, 1, tzinfo=UTC)

_CREATED = datetime(2026, 1, 1, tzinfo=UTC)

_CLUSTER_GROUP_THRESHOLD = 20
_CLUSTER_TOP_N = 5
_CLUSTER_EXPAND_CAP = 200

_SYNTHETIC_TYPES = {"topic_group", "identity_group", "zero_cost_summary", "capped_summary"}


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


# ---------------------------------------------------------------------------
# Row-building helpers
# ---------------------------------------------------------------------------


def _resource(
    resource_id: str,
    resource_type: str,
    parent_id: str | None = None,
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


def _identity(identity_id: str) -> IdentityTable:
    return IdentityTable(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        identity_id=identity_id,
        identity_type="service_account",
        display_name=identity_id,
        created_at=_CREATED,
        deleted_at=None,
    )


def _dim(dimension_id: int, resource_id: str, identity_id: str = "") -> ChargebackDimensionTable:
    return ChargebackDimensionTable(
        dimension_id=dimension_id,
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        resource_id=resource_id,
        identity_id=identity_id,
        env_id=ENV_ID,
        product_category="KAFKA",
        product_type="KAFKA_NUM_CKUS",
        cost_type="usage",
        allocation_method=None,
        allocation_detail=None,
    )


def _fact(dimension_id: int, amount: str) -> ChargebackFactTable:
    return ChargebackFactTable(
        timestamp=datetime(2026, 3, 10, tzinfo=UTC),
        dimension_id=dimension_id,
        amount=amount,
    )


def _topic_dim(dimension_id: int, topic_id: str, cluster_id: str, topic_name: str) -> TopicAttributionDimensionTable:
    return TopicAttributionDimensionTable(
        dimension_id=dimension_id,
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        env_id=ENV_ID,
        cluster_resource_id=cluster_id,
        topic_name=topic_name,
        resource_id=topic_id,
        product_category="KAFKA",
        product_type="KAFKA_TOPIC",
        attribution_method="proportional",
    )


def _topic_fact(dimension_id: int, amount: str) -> TopicAttributionFactTable:
    return TopicAttributionFactTable(
        timestamp=datetime(2026, 3, 10, tzinfo=UTC),
        dimension_id=dimension_id,
        amount=amount,
    )


# ---------------------------------------------------------------------------
# Scenario setup helpers
# ---------------------------------------------------------------------------


def _add_cluster(session: Session, cluster_id: str = CLUSTER_ID) -> None:
    session.add(_resource(ENV_ID, "environment"))
    session.add(_resource(cluster_id, "kafka_cluster", parent_id=ENV_ID))


def _add_topics(
    session: Session,
    cluster_id: str,
    n_nonzero: int,
    n_zero: int = 0,
    base_dim_id: int = 1000,
) -> tuple[list[str], list[str]]:
    """Add nonzero + zero-cost topics to cluster. Returns (nonzero_ids, zero_cost_ids)."""
    nonzero_ids: list[str] = []
    zero_ids: list[str] = []
    for i in range(n_nonzero):
        topic_id = f"{cluster_id}/topic/nz{i:04d}"
        session.add(_resource(topic_id, "kafka_topic", parent_id=cluster_id))
        dim_id = base_dim_id + i
        topic_name = f"nz{i:04d}"
        session.add(_topic_dim(dim_id, topic_id=topic_id, cluster_id=cluster_id, topic_name=topic_name))
        # Ascending costs so the top-N are predictable (highest indices = highest cost)
        session.add(_topic_fact(dim_id, str(Decimal("1.00") + Decimal(i))))
        nonzero_ids.append(topic_id)
    for k in range(n_zero):
        topic_id = f"{cluster_id}/topic/zc{k:04d}"
        session.add(_resource(topic_id, "kafka_topic", parent_id=cluster_id))
        # No topic_dim / topic_fact → attribution cost = 0
        zero_ids.append(topic_id)
    return nonzero_ids, zero_ids


def _add_identities(
    session: Session,
    cluster_id: str,
    n_nonzero: int,
    n_zero: int = 0,
    base_dim_id: int = 2000,
) -> tuple[list[str], list[str]]:
    """Add non-zero and zero-cost identities charged to cluster."""
    nonzero_ids: list[str] = []
    zero_ids: list[str] = []
    for j in range(n_nonzero):
        identity_id = f"sa-nz-{j:05d}"
        session.add(_identity(identity_id))
        dim_id = base_dim_id + j
        session.add(_dim(dim_id, resource_id=cluster_id, identity_id=identity_id))
        session.add(_fact(dim_id, str(Decimal("1.00") + Decimal(j))))
        nonzero_ids.append(identity_id)
    for k in range(n_zero):
        identity_id = f"sa-zc-{k:05d}"
        session.add(_identity(identity_id))
        dim_id = base_dim_id + n_nonzero + k
        session.add(_dim(dim_id, resource_id=cluster_id, identity_id=identity_id))
        # No _fact → cost = 0
        zero_ids.append(identity_id)
    return nonzero_ids, zero_ids


# ---------------------------------------------------------------------------
# V1: Small cluster passes through unchanged
# ---------------------------------------------------------------------------


class TestSmallClusterPassthrough:
    def test_small_cluster_no_group_nodes(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V1: ≤ threshold topics AND identities → no synthetic group nodes in response."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=10, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=10, base_dim_id=200)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "topic_group" not in node_types
        assert "identity_group" not in node_types
        # cluster + 10 topics + 10 identities
        assert len(result.nodes) == 21
        # After implementation: child_count=None on all regular nodes (fails before feature lands
        # because GraphNodeData has no child_count field yet → AttributeError)
        assert all(n.child_count is None for n in result.nodes)


# ---------------------------------------------------------------------------
# V2: Topic group activates; identity group does not
# ---------------------------------------------------------------------------


class TestTopicGroupOnlyActivates:
    def test_topic_group_activates_identity_group_does_not(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V2: 25 topics (> threshold), 5 identities (≤ threshold) → topic_group only."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=25, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=5, base_dim_id=400)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        topic_group_nodes = [n for n in result.nodes if n.resource_type == "topic_group"]
        assert len(topic_group_nodes) == 1
        assert topic_group_nodes[0].child_count == 25
        # Costs: 1.00, 2.00, ..., 25.00 → sum = 325.00 (GIT-001)
        assert topic_group_nodes[0].child_total_cost == Decimal("325.00")

        individual_topics = [n for n in result.nodes if n.resource_type == "kafka_topic"]
        assert len(individual_topics) == _CLUSTER_TOP_N
        # Top-5 by cost DESC = nz0020..nz0024 (costs 21.00-25.00) (GIT-004)
        individual_topic_ids = {n.id for n in individual_topics}
        expected_top5 = {f"{CLUSTER_ID}/topic/nz{i:04d}" for i in range(20, 25)}
        assert individual_topic_ids == expected_top5

        node_types = {n.resource_type for n in result.nodes}
        assert "identity_group" not in node_types

        individual_identities = [n for n in result.nodes if n.resource_type == "service_account"]
        assert len(individual_identities) == 5


# ---------------------------------------------------------------------------
# V3: Both groups large → no dangling edges
# ---------------------------------------------------------------------------


class TestBothGroupsLarge:
    def test_both_groups_large_no_dangling_edges(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V3: 25 topics, 25 identities → both group nodes; no dangling edges."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=25, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=25, base_dim_id=400)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "topic_group" in node_types

        identity_group_nodes = [n for n in result.nodes if n.resource_type == "identity_group"]
        assert len(identity_group_nodes) == 1
        assert identity_group_nodes[0].child_count == 25
        # Costs: 1.00, 2.00, ..., 25.00 → sum = 325.00 (GIT-001)
        assert identity_group_nodes[0].child_total_cost == Decimal("325.00")

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# GIT-003: Threshold boundary — exactly 20 vs 21 topics
# ---------------------------------------------------------------------------


class TestThresholdBoundary:
    def test_threshold_boundary_20_topics_no_grouping(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """GIT-003: exactly 20 topics (= threshold, NOT >) → no topic_group node."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=20, base_dim_id=100)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "topic_group" not in node_types
        # cluster + 20 topics
        assert len(result.nodes) == 21
        # After implementation: child_count=None on all nodes (currently AttributeError → RED)
        assert all(n.child_count is None for n in result.nodes)

    def test_threshold_boundary_21_topics_triggers_grouping(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """GIT-003: 21 topics (> threshold) → topic_group node with child_count=21."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=21, base_dim_id=100)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        topic_group_nodes = [n for n in result.nodes if n.resource_type == "topic_group"]
        assert len(topic_group_nodes) == 1
        assert topic_group_nodes[0].child_count == 21


# ---------------------------------------------------------------------------
# V4 / V5: expand=topics
# ---------------------------------------------------------------------------


class TestExpandTopics:
    def test_expand_topics_nonzero_individuals_zero_collapsed(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V4: expand=topics, 20 non-zero + 5 zero-cost topics, 25 identities."""
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=20, n_zero=5, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=25, base_dim_id=400)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END, expand="topics"
        )

        individual_topics = [n for n in result.nodes if n.resource_type == "kafka_topic"]
        assert len(individual_topics) == 20

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 1
        assert zero_cost_nodes[0].child_count == 5
        assert zero_cost_nodes[0].display_name == "5 others at $0"
        assert zero_cost_nodes[0].child_total_cost == Decimal("0")  # GIT-001

        node_types = {n.resource_type for n in result.nodes}
        assert "identity_group" in node_types
        assert "service_account" not in node_types

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"

    def test_expand_topics_all_zero_display_name(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V5: expand=topics, all 10 topics $0 → zero_cost_summary uses 'N topics ($0)' form."""
        _add_cluster(session)
        # 10 zero-cost topics (no attribution data)
        for i in range(10):
            session.add(_resource(f"{CLUSTER_ID}/topic/zc{i:04d}", "kafka_topic", parent_id=CLUSTER_ID))
        _add_identities(session, CLUSTER_ID, n_nonzero=25, base_dim_id=400)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END, expand="topics"
        )

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 1
        assert zero_cost_nodes[0].display_name == "10 topics ($0)"


# ---------------------------------------------------------------------------
# V6 / V7: expand=identities
# ---------------------------------------------------------------------------


class TestExpandIdentities:
    def test_expand_identities_cap_200_with_capped_summary(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V6: 250 non-zero identities → 200 individuals + capped_summary(50); no zero_cost node."""
        _add_cluster(session)
        session.add(_resource(f"{CLUSTER_ID}/topic/t0001", "kafka_topic", parent_id=CLUSTER_ID))
        _add_identities(session, CLUSTER_ID, n_nonzero=250, base_dim_id=1000)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END, expand="identities"
        )

        individual_identities = [n for n in result.nodes if n.resource_type == "service_account"]
        assert len(individual_identities) == _CLUSTER_EXPAND_CAP

        capped_nodes = [n for n in result.nodes if n.resource_type == "capped_summary"]
        assert len(capped_nodes) == 1
        assert capped_nodes[0].child_count == 50
        assert capped_nodes[0].display_name == "50 more identities"
        # Identities sorted DESC by cost (250.00 first). Top 200 = j=49..249 (costs 50.00..250.00).
        # Overflow = j=0..49 (costs 1.00..50.00). Sum = 1+2+...+50 = 1275 (GIT-002)
        assert capped_nodes[0].cost == Decimal("1275.00")

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 0

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"

    def test_expand_identities_zero_cost_and_cap_combined(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V7: 220 non-zero + 40 zero-cost → 200 individuals + capped(20) + zero_cost(40)."""
        _add_cluster(session)
        session.add(_resource(f"{CLUSTER_ID}/topic/t0001", "kafka_topic", parent_id=CLUSTER_ID))
        # 220 non-zero: cap=200, overflow=20; 40 zero-cost
        _add_identities(session, CLUSTER_ID, n_nonzero=220, n_zero=40, base_dim_id=1000)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END, expand="identities"
        )

        individual_identities = [n for n in result.nodes if n.resource_type == "service_account"]
        assert len(individual_identities) == _CLUSTER_EXPAND_CAP

        capped_nodes = [n for n in result.nodes if n.resource_type == "capped_summary"]
        assert len(capped_nodes) == 1
        assert capped_nodes[0].child_count == 20

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 1
        assert zero_cost_nodes[0].child_count == 40


# ---------------------------------------------------------------------------
# V8: diff_neighborhood bypasses grouping
# ---------------------------------------------------------------------------


class TestDiffNeighborhoodBypassesGrouping:
    def test_diff_neighborhood_returns_no_synthetic_group_nodes(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V8: diff_neighborhood with >threshold cluster returns real nodes only, no group nodes.

        Note: this test validates behavior that is already correct today (no grouping exists).
        After the feature is implemented, it guards that _force_full=True bypasses the new
        grouping logic so diffs remain accurate.
        """
        _add_cluster(session)
        _add_topics(session, CLUSTER_ID, n_nonzero=25, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=25, base_dim_id=400)
        session.commit()

        from_start = datetime(2026, 2, 1, tzinfo=UTC)
        from_end = datetime(2026, 3, 1, tzinfo=UTC)

        diff_result = repo.diff_neighborhood(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            focus_id=CLUSTER_ID,
            depth=1,
            from_start=from_start,
            from_end=from_end,
            to_start=PERIOD_START,
            to_end=PERIOD_END,
        )

        diff_types = {n.resource_type for n in diff_result}
        assert "topic_group" not in diff_types
        assert "identity_group" not in diff_types
        assert "zero_cost_summary" not in diff_types
        assert "capped_summary" not in diff_types


# ---------------------------------------------------------------------------
# V9: Regular nodes have child_count=None / child_total_cost=None
# ---------------------------------------------------------------------------


class TestRegularNodesHaveNullGroupFields:
    def test_regular_nodes_have_null_child_count_and_child_total_cost(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V9: non-synthetic nodes in grouped response have child_count=None and child_total_cost=None."""
        _add_cluster(session)
        # 25 topics → triggers topic_group; 5 identities → returned individually
        _add_topics(session, CLUSTER_ID, n_nonzero=25, base_dim_id=100)
        _add_identities(session, CLUSTER_ID, n_nonzero=5, base_dim_id=400)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, CLUSTER_ID, 1, AT, PERIOD_START, PERIOD_END)

        regular_nodes = [n for n in result.nodes if n.resource_type not in _SYNTHETIC_TYPES]
        assert len(regular_nodes) > 0  # sanity
        for node in regular_nodes:
            assert node.child_count is None, f"{node.id}: expected child_count=None, got {node.child_count!r}"
            assert node.child_total_cost is None, (
                f"{node.id}: expected child_total_cost=None, got {node.child_total_cost!r}"
            )
