from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.api.schemas import GraphNode
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.graph import GraphNeighborhood, GraphNodeData


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


# ---------------------------------------------------------------------------
# V10: GraphNode schema round-trips child_count / child_total_cost
# ---------------------------------------------------------------------------


class TestGraphNodeSchemaGroupFields:
    def test_graph_node_schema_round_trips_child_count_and_child_total_cost(self) -> None:
        """V10: GraphNodeData(child_count=3, child_total_cost=5.00) survives GraphNode mapping."""
        data = GraphNodeData(
            id="lkc-abc:topic_group",
            resource_type="topic_group",
            display_name="25 topics",
            cost=Decimal("0"),
            created_at=None,
            deleted_at=None,
            tags={},
            parent_id="lkc-abc",
            cloud=None,
            region=None,
            status="active",
            child_count=3,
            child_total_cost=Decimal("5.00"),
        )
        node = GraphNode(
            id=data.id,
            resource_type=data.resource_type,
            display_name=data.display_name,
            cost=data.cost,
            created_at=data.created_at,
            deleted_at=data.deleted_at,
            tags=data.tags,
            parent_id=data.parent_id,
            cloud=data.cloud,
            region=data.region,
            status=data.status,
            cross_references=data.cross_references,
            child_count=data.child_count,
            child_total_cost=data.child_total_cost,
        )
        assert node.child_count == 3
        assert node.child_total_cost == Decimal("5.00")


# ---------------------------------------------------------------------------
# V11: Route rejects invalid expand value with 422
# ---------------------------------------------------------------------------


class TestGraphRouteExpandValidation:
    def test_route_rejects_invalid_expand_value_with_422(self) -> None:
        """V11: expand=foobar (not in Literal['topics','identities']) → FastAPI returns 422."""
        mock_uow = MagicMock()
        mock_uow.graph.find_neighborhood.return_value = GraphNeighborhood(nodes=[], edges=[])
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/graph",
                params={"focus": "lkc-abc", "expand": "foobar", "at": "2026-03-15T00:00:00Z"},
            )
        assert resp.status_code == 422
