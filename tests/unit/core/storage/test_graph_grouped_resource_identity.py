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
)

ECOSYSTEM = "confluent_cloud"
TENANT_ID = "org-test"
ENV_ID = "env-rgi"
IDENTITY_ID = "sa-rgi-focus"

AT = datetime(2026, 3, 15, tzinfo=UTC)
PERIOD_START = datetime(2026, 3, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 4, 1, tzinfo=UTC)

_CREATED = datetime(2026, 1, 1, tzinfo=UTC)

_CLUSTER_GROUP_THRESHOLD = 20
_CLUSTER_TOP_N = 5

_SYNTHETIC_TYPES = {"resource_group", "cluster_group", "zero_cost_summary", "capped_summary"}


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


# ---------------------------------------------------------------------------
# Scenario setup helpers
# ---------------------------------------------------------------------------


def _add_environment_with_child_clusters(
    session: Session,
    n_nonzero: int,
    n_zero: int = 0,
    base_dim_id: int = 3000,
    env_id: str = ENV_ID,
) -> tuple[list[str], list[str]]:
    """Add environment focus + N child kafka_cluster resources with costs.

    Returns (nonzero_cluster_ids, zero_cost_cluster_ids).
    Ascending costs so top-N are predictable (highest index = highest cost).
    """
    session.add(_resource(env_id, "environment"))
    nonzero_ids: list[str] = []
    zero_ids: list[str] = []
    for i in range(n_nonzero):
        child_id = f"{env_id}/cluster/nz{i:04d}"
        session.add(_resource(child_id, "kafka_cluster", parent_id=env_id))
        dim_id = base_dim_id + i
        session.add(_dim(dim_id, resource_id=child_id))
        session.add(_fact(dim_id, str(Decimal("1.00") + Decimal(i))))
        nonzero_ids.append(child_id)
    for k in range(n_zero):
        child_id = f"{env_id}/cluster/zc{k:04d}"
        session.add(_resource(child_id, "kafka_cluster", parent_id=env_id))
        # No fact → cost = 0
        zero_ids.append(child_id)
    return nonzero_ids, zero_ids


def _add_identity_charged_across_clusters(
    session: Session,
    identity_id: str,
    n_nonzero: int,
    n_zero: int = 0,
    base_dim_id: int = 4000,
) -> tuple[list[str], list[str]]:
    """Add identity focus + N clusters it's charged across.

    Returns (nonzero_cluster_ids, zero_cost_cluster_ids).
    Ascending costs so top-N are predictable (highest index = highest cost).
    """
    session.add(_identity(identity_id))
    env_id = f"env-for-{identity_id}"
    session.add(_resource(env_id, "environment"))
    nonzero_ids: list[str] = []
    zero_ids: list[str] = []
    for i in range(n_nonzero):
        cluster_id = f"lkc-rgi-nz{i:04d}"
        session.add(_resource(cluster_id, "kafka_cluster", parent_id=env_id))
        dim_id = base_dim_id + i
        session.add(_dim(dim_id, resource_id=cluster_id, identity_id=identity_id))
        session.add(_fact(dim_id, str(Decimal("1.00") + Decimal(i))))
        nonzero_ids.append(cluster_id)
    for k in range(n_zero):
        cluster_id = f"lkc-rgi-zc{k:04d}"
        session.add(_resource(cluster_id, "kafka_cluster", parent_id=env_id))
        dim_id = base_dim_id + n_nonzero + k
        session.add(_dim(dim_id, resource_id=cluster_id, identity_id=identity_id))
        # No fact → cost = 0
        zero_ids.append(cluster_id)
    return nonzero_ids, zero_ids


# ---------------------------------------------------------------------------
# V1: Resource view small count passes through
# ---------------------------------------------------------------------------


class TestResourceViewSmallCountPassthrough:
    def test_resource_view_small_count_no_resource_group_node(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V1: 10 child resources (≤ threshold) → no resource_group synthetic node returned."""
        _add_environment_with_child_clusters(session, n_nonzero=10, base_dim_id=3100)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "resource_group" not in node_types
        # environment + 10 children
        assert len(result.nodes) == 11


# ---------------------------------------------------------------------------
# V2: Resource view grouped mode
# ---------------------------------------------------------------------------


class TestResourceViewGroupedMode:
    def test_resource_view_grouped_mode_produces_resource_group_with_top_n(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V2: 25 child resources → resource_group node with child_count=25, child_total_cost=sum,
        plus exactly 5 (_CLUSTER_TOP_N) individual top-cost nodes.
        """
        _add_environment_with_child_clusters(session, n_nonzero=25, base_dim_id=3200)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END)

        resource_group_nodes = [n for n in result.nodes if n.resource_type == "resource_group"]
        assert len(resource_group_nodes) == 1

        rg = resource_group_nodes[0]
        assert rg.child_count == 25
        # Costs: 1.00, 2.00, ..., 25.00 → sum = 325.00
        assert rg.child_total_cost == Decimal("325.00")

        individual_children = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_children) == _CLUSTER_TOP_N

        # Top-5 by cost DESC = nz0020..nz0024 (costs 21.00-25.00)
        individual_ids = {n.id for n in individual_children}
        expected_top5 = {f"{ENV_ID}/cluster/nz{i:04d}" for i in range(20, 25)}
        assert individual_ids == expected_top5

    def test_resource_view_no_dangling_edges_in_grouped_mode(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V4: grouped mode — all edge source/target IDs exist in node IDs."""
        _add_environment_with_child_clusters(session, n_nonzero=25, base_dim_id=3300)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# V3: Resource view expand=resources
# ---------------------------------------------------------------------------


class TestResourceViewExpandMode:
    def test_resource_view_expand_resources_nonzero_individual_zero_collapsed(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V3: expand=resources, 20 non-zero + 5 zero-cost children → 20 individuals + zero_cost_summary."""
        _add_environment_with_child_clusters(session, n_nonzero=20, n_zero=5, base_dim_id=3400)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END, expand="resources"
        )

        individual_children = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_children) == 20

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 1
        assert zero_cost_nodes[0].child_count == 5

        node_types = {n.resource_type for n in result.nodes}
        assert "resource_group" not in node_types

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# V5: Identity view small count passes through
# ---------------------------------------------------------------------------


class TestIdentityViewSmallCountPassthrough:
    def test_identity_view_small_count_no_cluster_group_node(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V5: identity charged in 10 clusters (≤ threshold) → no cluster_group node."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=10, base_dim_id=4100)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "cluster_group" not in node_types


# ---------------------------------------------------------------------------
# V6: Identity view grouped mode
# ---------------------------------------------------------------------------


class TestIdentityViewGroupedMode:
    def test_identity_view_grouped_mode_produces_cluster_group_with_top_n(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V6: identity charged in 25 clusters → cluster_group with child_count=25 + top-5."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=25, base_dim_id=4200)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END)

        cluster_group_nodes = [n for n in result.nodes if n.resource_type == "cluster_group"]
        assert len(cluster_group_nodes) == 1

        cg = cluster_group_nodes[0]
        assert cg.child_count == 25
        # Costs: 1.00, 2.00, ..., 25.00 → sum = 325.00
        assert cg.child_total_cost == Decimal("325.00")

        individual_clusters = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_clusters) == _CLUSTER_TOP_N

        # Top-5 by cost DESC = nz0020..nz0024 (costs 21.00-25.00)
        individual_ids = {n.id for n in individual_clusters}
        expected_top5 = {f"lkc-rgi-nz{i:04d}" for i in range(20, 25)}
        assert individual_ids == expected_top5

    def test_identity_view_no_dangling_edges_in_grouped_mode(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V8: identity grouped mode — all edge source/target IDs exist in node IDs."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=25, base_dim_id=4300)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# V7: Identity view expand=clusters
# ---------------------------------------------------------------------------


class TestIdentityViewExpandMode:
    def test_identity_view_expand_clusters_nonzero_individual_zero_collapsed(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V7: expand=clusters, 20 non-zero + 5 zero-cost clusters → 20 individuals + zero_cost_summary."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=20, n_zero=5, base_dim_id=4400)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END, expand="clusters"
        )

        individual_clusters = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_clusters) == 20

        zero_cost_nodes = [n for n in result.nodes if n.resource_type == "zero_cost_summary"]
        assert len(zero_cost_nodes) == 1
        assert zero_cost_nodes[0].child_count == 5

        node_types = {n.resource_type for n in result.nodes}
        assert "cluster_group" not in node_types

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# V9a: diff_neighborhood bypasses grouping for resource view
# ---------------------------------------------------------------------------


class TestDiffNeighborhoodBypassesResourceGrouping:
    def test_diff_neighborhood_resource_view_returns_no_synthetic_nodes(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V9: diff_neighborhood with 25-child environment focus → no synthetic nodes.

        Validates _force_full=True bypasses resource_group grouping.
        """
        _add_environment_with_child_clusters(session, n_nonzero=25, base_dim_id=3500)
        session.commit()

        from_start = datetime(2026, 2, 1, tzinfo=UTC)
        from_end = datetime(2026, 3, 1, tzinfo=UTC)

        diff_result = repo.diff_neighborhood(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            focus_id=ENV_ID,
            depth=1,
            from_start=from_start,
            from_end=from_end,
            to_start=PERIOD_START,
            to_end=PERIOD_END,
        )

        diff_types = {n.resource_type for n in diff_result}
        assert "resource_group" not in diff_types
        assert "zero_cost_summary" not in diff_types
        assert "capped_summary" not in diff_types


# ---------------------------------------------------------------------------
# V9b: diff_neighborhood bypasses grouping for identity view (GIT-001)
# ---------------------------------------------------------------------------


class TestDiffNeighborhoodBypassesIdentityGrouping:
    def test_diff_neighborhood_identity_view_returns_no_synthetic_nodes(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V9b: diff_neighborhood with 25-cluster identity focus → no synthetic nodes.

        Validates _force_full=True bypasses cluster_group grouping in _identity_view.
        """
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=25, base_dim_id=4800)
        session.commit()

        from_start = datetime(2026, 2, 1, tzinfo=UTC)
        from_end = datetime(2026, 3, 1, tzinfo=UTC)

        diff_result = repo.diff_neighborhood(
            ecosystem=ECOSYSTEM,
            tenant_id=TENANT_ID,
            focus_id=IDENTITY_ID,
            depth=1,
            from_start=from_start,
            from_end=from_end,
            to_start=PERIOD_START,
            to_end=PERIOD_END,
        )

        diff_types = {n.resource_type for n in diff_result}
        assert "cluster_group" not in diff_types
        assert "zero_cost_summary" not in diff_types
        assert "capped_summary" not in diff_types


# ---------------------------------------------------------------------------
# GIT-002/003: Capped overflow paths (monkeypatch _CLUSTER_EXPAND_CAP)
# ---------------------------------------------------------------------------


class TestResourceViewCappedOverflow:
    def test_resource_view_expand_capped_overflow_produces_capped_summary(
        self, session: Session, repo: SQLModelGraphRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GIT-002: expand=resources with cap=3, 25 non-zero children → 3 individuals + capped_summary(22)."""
        import core.storage.backends.sqlmodel.repositories as repo_module

        monkeypatch.setattr(repo_module, "_CLUSTER_EXPAND_CAP", 3)

        # 25 non-zero > threshold(20) → expand path with cap=3
        _add_environment_with_child_clusters(session, n_nonzero=25, base_dim_id=3850)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END, expand="resources"
        )

        individual_children = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_children) == 3

        capped_nodes = [n for n in result.nodes if n.resource_type == "capped_summary"]
        assert len(capped_nodes) == 1
        assert capped_nodes[0].child_count == 22  # 25 - 3 = 22 overflow

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


class TestIdentityViewCappedOverflow:
    def test_identity_view_expand_capped_overflow_produces_capped_summary(
        self, session: Session, repo: SQLModelGraphRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GIT-003: expand=clusters with cap=3, 25 non-zero clusters → 3 individuals + capped_summary(22)."""
        import core.storage.backends.sqlmodel.repositories as repo_module

        monkeypatch.setattr(repo_module, "_CLUSTER_EXPAND_CAP", 3)

        # 25 non-zero > threshold(20) → expand path with cap=3
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=25, base_dim_id=4850)
        session.commit()

        result = repo.find_neighborhood(
            ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END, expand="clusters"
        )

        individual_clusters = [n for n in result.nodes if n.resource_type == "kafka_cluster"]
        assert len(individual_clusters) == 3

        capped_nodes = [n for n in result.nodes if n.resource_type == "capped_summary"]
        assert len(capped_nodes) == 1
        assert capped_nodes[0].child_count == 22  # 25 - 3 = 22 overflow

        node_ids = {n.id for n in result.nodes}
        for edge in result.edges:
            assert edge.source in node_ids, f"dangling edge source: {edge.source!r}"
            assert edge.target in node_ids, f"dangling edge target: {edge.target!r}"


# ---------------------------------------------------------------------------
# V11: Threshold boundary for resource and identity views
# ---------------------------------------------------------------------------


class TestThresholdBoundaryResourceView:
    def test_resource_view_exactly_20_children_no_grouping(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V11a: exactly 20 child resources (= threshold, NOT >) → no resource_group node."""
        _add_environment_with_child_clusters(session, n_nonzero=20, base_dim_id=3600)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "resource_group" not in node_types
        # environment + 20 children
        assert len(result.nodes) == 21

    def test_resource_view_21_children_triggers_grouping(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V11b: 21 child resources (> threshold) → resource_group node with child_count=21."""
        _add_environment_with_child_clusters(session, n_nonzero=21, base_dim_id=3700)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, ENV_ID, 1, AT, PERIOD_START, PERIOD_END)

        resource_group_nodes = [n for n in result.nodes if n.resource_type == "resource_group"]
        assert len(resource_group_nodes) == 1
        assert resource_group_nodes[0].child_count == 21


class TestThresholdBoundaryIdentityView:
    def test_identity_view_exactly_20_clusters_no_grouping(
        self, session: Session, repo: SQLModelGraphRepository
    ) -> None:
        """V11c: identity charged in exactly 20 clusters (= threshold) → no cluster_group node."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=20, base_dim_id=4500)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END)

        node_types = {n.resource_type for n in result.nodes}
        assert "cluster_group" not in node_types

    def test_identity_view_21_clusters_triggers_grouping(self, session: Session, repo: SQLModelGraphRepository) -> None:
        """V11d: identity charged in 21 clusters (> threshold) → cluster_group with child_count=21."""
        _add_identity_charged_across_clusters(session, IDENTITY_ID, n_nonzero=21, base_dim_id=4600)
        session.commit()

        result = repo.find_neighborhood(ECOSYSTEM, TENANT_ID, IDENTITY_ID, 1, AT, PERIOD_START, PERIOD_END)

        cluster_group_nodes = [n for n in result.nodes if n.resource_type == "cluster_group"]
        assert len(cluster_group_nodes) == 1
        assert cluster_group_nodes[0].child_count == 21
