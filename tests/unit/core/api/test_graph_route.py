from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.api.schemas import CrossReferenceGroupSchema
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.graph import (
    CrossReferenceGroup,
    CrossReferenceItem,
    EdgeType,
    GraphEdgeData,
    GraphNeighborhood,
    GraphNodeData,
)


def _make_settings() -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={
            "prod": TenantConfig(
                tenant_id="prod",
                ecosystem="eco",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        },
    )


@contextmanager
def _app_with_mock_uow(mock_uow: MagicMock) -> Iterator[TestClient]:
    settings = _make_settings()
    app = create_app(settings)

    def _uow_override() -> Iterator[MagicMock]:
        yield mock_uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _make_node(
    node_id: str,
    resource_type: str,
    cost: Decimal = Decimal("0"),
    parent_id: str | None = None,
    cross_references: list[CrossReferenceGroup] | None = None,
) -> GraphNodeData:
    return GraphNodeData(
        id=node_id,
        resource_type=resource_type,
        display_name=node_id,
        cost=cost,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        deleted_at=None,
        tags={},
        parent_id=parent_id,
        cloud=None,
        region=None,
        status="active",
        cross_references=cross_references or [],
    )


class TestGraphRouteRootView:
    def test_root_view_returns_environment_and_tenant_nodes(self) -> None:
        """Test 1: root view contains resource_type=environment + resource_type=tenant nodes."""
        mock_uow = MagicMock()
        env_node = _make_node("env-abc", "environment", cost=Decimal("100.00"))
        tenant_node = _make_node("prod", "tenant", cost=Decimal("100.00"))
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(
            nodes=[tenant_node, env_node],
            edges=[GraphEdgeData(source="prod", target="env-abc", relationship_type=EdgeType.parent)],
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        assert resp.status_code == 200
        data = resp.json()
        node_types = {n["resource_type"] for n in data["nodes"]}
        assert "environment" in node_types
        assert "tenant" in node_types

    def test_root_view_edges_have_parent_relationship_with_correct_direction(self) -> None:
        """Test 1: root view edges relationship_type=parent, source=tenant_id, target=env_id."""
        mock_uow = MagicMock()
        env_node = _make_node("env-abc", "environment")
        tenant_node = _make_node("prod", "tenant")
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(
            nodes=[tenant_node, env_node],
            edges=[GraphEdgeData(source="prod", target="env-abc", relationship_type=EdgeType.parent)],
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        data = resp.json()
        assert len(data["edges"]) == 1
        edge = data["edges"][0]
        assert edge["relationship_type"] == "parent"
        assert edge["source"] == "prod"
        assert edge["target"] == "env-abc"


class TestGraphRouteBillingPeriodDefaults:
    def test_billing_period_defaults_to_march_when_at_is_march(self) -> None:
        """Test 3: at=2026-03-15 → period_start=2026-03-01, period_end=2026-04-01."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["period_start"] == datetime(2026, 3, 1, tzinfo=UTC)
        assert kwargs["period_end"] == datetime(2026, 4, 1, tzinfo=UTC)

    def test_billing_period_handles_december_year_rollover(self) -> None:
        """Test 3: at=2026-12-20 → period_start=2026-12-01, period_end=2027-01-01."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph", params={"at": "2026-12-20T00:00:00Z"})

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["period_start"] == datetime(2026, 12, 1, tzinfo=UTC)
        assert kwargs["period_end"] == datetime(2027, 1, 1, tzinfo=UTC)

    def test_explicit_date_override_ignores_at_for_billing_period(self) -> None:
        """Test 4: start_date/end_date override at-month default."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get(
                "/api/v1/tenants/prod/graph",
                params={
                    "at": "2026-06-15T00:00:00Z",
                    "start_date": "2026-01-01",
                    "end_date": "2026-03-31",
                },
            )

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["period_start"] == datetime(2026, 1, 1, tzinfo=UTC)
        # end_date=2026-03-31 → exclusive end = 2026-04-01 (resolve_date_range convention)
        assert kwargs["period_end"] == datetime(2026, 4, 1, tzinfo=UTC)

    def test_at_none_defaults_to_now_and_returns_200(self) -> None:
        """at=None → route uses datetime.now(UTC); find_neighborhood called with at≈now; 200 response."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        before = datetime.now(UTC)
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph")
        after = datetime.now(UTC)

        assert resp.status_code == 200
        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        at_used = kwargs["at"]
        assert before - timedelta(seconds=1) <= at_used <= after + timedelta(seconds=1)


class TestGraphRouteEnvironmentFocus:
    def test_environment_focus_returns_env_and_children(self) -> None:
        """Test 5: env focus depth=1 → env node + direct children; edges source=env, target=child."""
        mock_uow = MagicMock()
        env_node = _make_node("env-abc", "environment")
        cluster_node = _make_node("lkc-abc", "kafka_cluster", parent_id="env-abc")
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(
            nodes=[env_node, cluster_node],
            edges=[GraphEdgeData(source="env-abc", target="lkc-abc", relationship_type=EdgeType.parent)],
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/graph",
                params={"focus": "env-abc", "depth": "1", "at": "2026-03-15T00:00:00Z"},
            )

        assert resp.status_code == 200
        data = resp.json()
        node_ids = {n["id"] for n in data["nodes"]}
        assert "env-abc" in node_ids
        assert "lkc-abc" in node_ids

        edge = data["edges"][0]
        assert edge["relationship_type"] == "parent"
        assert edge["source"] == "env-abc"
        assert edge["target"] == "lkc-abc"

    def test_environment_focus_passes_focus_id_to_repo(self) -> None:
        """Test 5: route forwards focus param to find_neighborhood as focus_id."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get(
                "/api/v1/tenants/prod/graph",
                params={"focus": "env-abc", "at": "2026-03-15T00:00:00Z"},
            )

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["focus_id"] == "env-abc"

    def test_root_view_passes_focus_id_none(self) -> None:
        """No focus param → focus_id=None passed to repo."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["focus_id"] is None


class TestGraphRouteClusterFocus:
    def test_cluster_focus_returns_cluster_topics_and_identities(self) -> None:
        """Test 6: cluster focus → cluster + topic children + identity nodes."""
        mock_uow = MagicMock()
        cluster_node = _make_node("lkc-abc", "kafka_cluster")
        topic_node = _make_node("lkc-abc/topic/orders", "kafka_topic", parent_id="lkc-abc")
        identity_node = _make_node("sa-001", "service_account")
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(
            nodes=[cluster_node, topic_node, identity_node],
            edges=[
                GraphEdgeData(source="lkc-abc", target="lkc-abc/topic/orders", relationship_type=EdgeType.parent),
                GraphEdgeData(
                    source="lkc-abc",
                    target="sa-001",
                    relationship_type=EdgeType.charge,
                    cost=Decimal("25.00"),
                ),
            ],
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/graph",
                params={"focus": "lkc-abc", "depth": "1", "at": "2026-03-15T00:00:00Z"},
            )

        assert resp.status_code == 200
        data = resp.json()
        node_types = {n["resource_type"] for n in data["nodes"]}
        assert "kafka_cluster" in node_types
        assert "kafka_topic" in node_types
        assert "service_account" in node_types

        edge_types = {e["relationship_type"] for e in data["edges"]}
        assert "parent" in edge_types
        assert "charge" in edge_types

        charge_edges = [e for e in data["edges"] if e["relationship_type"] == "charge"]
        assert len(charge_edges) == 1
        assert charge_edges[0]["source"] == "lkc-abc"
        assert charge_edges[0]["target"] == "sa-001"


class TestGraphRouteEdgeDirectionUniformity:
    def test_all_parent_edges_source_is_parent_target_is_child(self) -> None:
        """Test 7: all relationship_type=parent edges have source=parent, target=child."""
        mock_uow = MagicMock()
        tenant_node = _make_node("prod", "tenant")
        env_node = _make_node("env-abc", "environment")
        cluster_node = _make_node("lkc-abc", "kafka_cluster", parent_id="env-abc")
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(
            nodes=[tenant_node, env_node, cluster_node],
            edges=[
                GraphEdgeData(source="prod", target="env-abc", relationship_type=EdgeType.parent),
                GraphEdgeData(source="env-abc", target="lkc-abc", relationship_type=EdgeType.parent),
            ],
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        data = resp.json()
        parent_edges = [e for e in data["edges"] if e["relationship_type"] == "parent"]
        assert len(parent_edges) == 2

        edge_map = {e["source"]: e["target"] for e in parent_edges}
        assert edge_map["prod"] == "env-abc"
        assert edge_map["env-abc"] == "lkc-abc"


class TestGraphRouteErrorHandling:
    def test_invalid_focus_returns_404_with_detail(self) -> None:
        """Test 13: KeyError from repo → HTTP 404, detail contains unknown id."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.side_effect = KeyError("does-not-exist")
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/graph",
                params={"focus": "does-not-exist", "at": "2026-03-15T00:00:00Z"},
            )

        assert resp.status_code == 404
        assert "does-not-exist" in resp.json()["detail"]

    def test_tz_naive_datetime_returns_400(self) -> None:
        """Test 14: at=2026-03-15T00:00:00 (no Z) → HTTP 400."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00"})

        assert resp.status_code == 400

    def test_unparseable_datetime_returns_422(self) -> None:
        """Test 15: at=not-a-date → HTTP 422 (FastAPI validation)."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "not-a-date"})

        assert resp.status_code == 422

    def test_unknown_tenant_returns_404(self) -> None:
        """Unknown tenant name → 404."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/graph")

        assert resp.status_code == 404

    def test_route_passes_ecosystem_and_tenant_id_to_repo(self) -> None:
        """Route forwards ecosystem and tenant_id from TenantConfig to repo."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        kwargs = mock_uow.graph.find_neighborhood.call_args.kwargs
        assert kwargs["ecosystem"] == "eco"
        assert kwargs["tenant_id"] == "prod"


# ---------------------------------------------------------------------------
# V6: API route serializes CrossReferenceGroup correctly
# ---------------------------------------------------------------------------


class TestCrossReferenceGroupSerialization:
    def test_cross_reference_group_schema_round_trips_from_dataclass(self) -> None:
        """CrossReferenceGroupSchema(from_attributes=True) can be built from CrossReferenceGroup."""
        item = CrossReferenceItem(
            id="lkc-abc",
            resource_type="kafka_cluster",
            display_name="prod-cluster",
            cost=Decimal("123.45"),
        )
        group = CrossReferenceGroup(resource_type="kafka_cluster", items=[item], total_count=10)
        schema = CrossReferenceGroupSchema.model_validate(group)
        assert schema.resource_type == "kafka_cluster"
        assert schema.total_count == 10
        assert len(schema.items) == 1
        assert schema.items[0].id == "lkc-abc"
        assert schema.items[0].display_name == "prod-cluster"
        assert schema.items[0].cost == Decimal("123.45")

    def test_api_response_cross_references_serialized_as_list_of_groups(self) -> None:
        """GET /graph → cross_references in response is list[CrossReferenceGroup] JSON."""
        item = CrossReferenceItem(
            id="lfcp-001",
            resource_type="flink_compute_pool",
            display_name=None,
            cost=Decimal("50.00"),
        )
        group = CrossReferenceGroup(resource_type="flink_compute_pool", items=[item], total_count=5)
        sa_node = _make_node("sa-001", "service_account", cross_references=[group])
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[sa_node], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph", params={"at": "2026-03-15T00:00:00Z"})

        assert resp.status_code == 200
        data = resp.json()
        sa = next(n for n in data["nodes"] if n["id"] == "sa-001")
        xrefs = sa["cross_references"]
        assert isinstance(xrefs, list)
        assert len(xrefs) == 1
        xref = xrefs[0]
        assert xref["resource_type"] == "flink_compute_pool"
        assert xref["total_count"] == 5
        assert len(xref["items"]) == 1
        assert xref["items"][0]["id"] == "lfcp-001"
        assert xref["items"][0]["display_name"] is None
