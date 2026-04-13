from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.models.graph import EdgeType, GraphEdgeData, GraphNeighborhood, GraphNodeData


def _make_node(node_id: str = "node-1") -> GraphNodeData:
    return GraphNodeData(
        id=node_id,
        resource_type="environment",
        display_name=None,
        cost=Decimal("0"),
        created_at=None,
        deleted_at=None,
        tags={},
        parent_id=None,
        cloud=None,
        region=None,
        status="active",
    )


class TestEdgeType:
    def test_parent_value(self) -> None:
        assert EdgeType.parent == "parent"

    def test_charge_value(self) -> None:
        assert EdgeType.charge == "charge"

    def test_attribution_value(self) -> None:
        assert EdgeType.attribution == "attribution"

    def test_is_str_subclass(self) -> None:
        assert isinstance(EdgeType.parent, str)
        assert isinstance(EdgeType.charge, str)


class TestGraphNodeData:
    def test_construction_with_all_fields(self) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        node = GraphNodeData(
            id="env-1",
            resource_type="environment",
            display_name="My Env",
            cost=Decimal("100.00"),
            created_at=ts,
            deleted_at=None,
            tags={"team": "platform"},
            parent_id=None,
            cloud="aws",
            region="us-east-1",
            status="active",
        )
        assert node.id == "env-1"
        assert node.resource_type == "environment"
        assert node.display_name == "My Env"
        assert node.cost == Decimal("100.00")
        assert node.created_at == ts
        assert node.deleted_at is None
        assert node.tags == {"team": "platform"}
        assert node.cloud == "aws"
        assert node.region == "us-east-1"
        assert node.status == "active"

    def test_cross_references_defaults_to_empty_list(self) -> None:
        node = _make_node()
        assert node.cross_references == []

    def test_cross_references_independent_per_instance(self) -> None:
        """field(default_factory=list) gives each instance its own list."""
        n1 = _make_node("a")
        n2 = _make_node("b")
        n1.cross_references.append("x")
        assert n2.cross_references == []

    def test_cross_references_explicit_value(self) -> None:
        node = GraphNodeData(
            id="sa-1",
            resource_type="service_account",
            display_name=None,
            cost=Decimal("0"),
            created_at=None,
            deleted_at=None,
            tags={},
            parent_id=None,
            cloud=None,
            region=None,
            status="active",
            cross_references=["lkc-2", "lkc-3"],
        )
        assert node.cross_references == ["lkc-2", "lkc-3"]


class TestGraphEdgeData:
    def test_parent_edge_construction(self) -> None:
        edge = GraphEdgeData(source="env-1", target="lkc-1", relationship_type=EdgeType.parent)
        assert edge.source == "env-1"
        assert edge.target == "lkc-1"
        assert edge.relationship_type == EdgeType.parent
        assert edge.cost is None

    def test_charge_edge_with_cost(self) -> None:
        edge = GraphEdgeData(
            source="lkc-1",
            target="sa-1",
            relationship_type=EdgeType.charge,
            cost=Decimal("50.00"),
        )
        assert edge.cost == Decimal("50.00")
        assert edge.relationship_type == EdgeType.charge

    def test_attribution_edge_type(self) -> None:
        edge = GraphEdgeData(source="a", target="b", relationship_type=EdgeType.attribution)
        assert edge.relationship_type == EdgeType.attribution


class TestGraphNeighborhood:
    def test_empty_neighborhood(self) -> None:
        n = GraphNeighborhood(nodes=[], edges=[])
        assert n.nodes == []
        assert n.edges == []

    def test_neighborhood_with_nodes_and_edges(self) -> None:
        node = _make_node("env-1")
        edge = GraphEdgeData(source="tenant-1", target="env-1", relationship_type=EdgeType.parent)
        n = GraphNeighborhood(nodes=[node], edges=[edge])
        assert len(n.nodes) == 1
        assert len(n.edges) == 1
        assert n.nodes[0].id == "env-1"
        assert n.edges[0].source == "tenant-1"
