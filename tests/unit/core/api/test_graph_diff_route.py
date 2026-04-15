from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.graph import GraphDiffNodeData


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


_DIFF_PARAMS = {
    "from_start": "2026-03-01",
    "from_end": "2026-03-31",
    "to_start": "2026-04-01",
    "to_end": "2026-04-13",
}


def _make_diff_node(
    entity_id: str,
    resource_type: str = "kafka_cluster",
    cost_before: str = "100.00",
    cost_after: str = "120.00",
    status: str = "changed",
    pct_change: str | None = "20.00",
    parent_id: str | None = "env-abc",
) -> GraphDiffNodeData:
    cb = Decimal(cost_before)
    ca = Decimal(cost_after)
    return GraphDiffNodeData(
        id=entity_id,
        resource_type=resource_type,
        display_name=entity_id,
        parent_id=parent_id,
        cost_before=cb,
        cost_after=ca,
        cost_delta=ca - cb,
        pct_change=Decimal(pct_change) if pct_change is not None else None,
        status=status,
    )


class TestGraphDiffRouteBasic:
    def test_diff_returns_nodes_http_200(self) -> None:
        """V8: GET /graph/diff with valid date ranges returns nodes, HTTP 200."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = [
            _make_diff_node("lkc-abc"),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data
        assert len(data["nodes"]) == 1

    def test_diff_node_has_all_required_fields(self) -> None:
        """V8: each node has all required diff fields."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = [
            _make_diff_node("lkc-abc", cost_before="100.00", cost_after="120.00", pct_change="20.00", status="changed"),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        node = resp.json()["nodes"][0]
        assert node["id"] == "lkc-abc"
        assert node["resource_type"] == "kafka_cluster"
        assert node["cost_before"] == "100.00"
        assert node["cost_after"] == "120.00"
        assert node["cost_delta"] == "20.00"  # 120 - 100
        assert node["pct_change"] == "20.00"
        assert node["status"] == "changed"

    def test_diff_passes_date_ranges_to_repo(self) -> None:
        """Route forwards from_start, from_end, to_start, to_end as datetimes to repo."""
        from datetime import UTC, datetime

        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        kwargs = mock_uow.graph.diff_neighborhood.call_args.kwargs
        assert kwargs["from_start"] == datetime(2026, 3, 1, tzinfo=UTC)
        assert kwargs["from_end"] == datetime(2026, 4, 1, tzinfo=UTC)  # exclusive end
        assert kwargs["to_start"] == datetime(2026, 4, 1, tzinfo=UTC)
        assert kwargs["to_end"] == datetime(2026, 4, 14, tzinfo=UTC)  # exclusive end


class TestGraphDiffRouteNewAndDeletedEntities:
    def test_diff_new_entity_has_status_new_cost_before_zero(self) -> None:
        """V9: entity only in 'to' window → status=new, cost_before=0, pct_change=null."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = [
            GraphDiffNodeData(
                id="lkc-new",
                resource_type="kafka_cluster",
                display_name="lkc-new",
                parent_id="env-abc",
                cost_before=Decimal("0"),
                cost_after=Decimal("50.00"),
                cost_delta=Decimal("50.00"),
                pct_change=None,
                status="new",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        node = resp.json()["nodes"][0]
        assert node["status"] == "new"
        assert node["cost_before"] == "0"
        assert node["pct_change"] is None

    def test_diff_deleted_entity_has_status_deleted_cost_after_zero(self) -> None:
        """V10: entity only in 'from' window → status=deleted, cost_after=0, pct_change=null."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = [
            GraphDiffNodeData(
                id="lkc-old",
                resource_type="kafka_cluster",
                display_name="lkc-old",
                parent_id="env-abc",
                cost_before=Decimal("75.00"),
                cost_after=Decimal("0"),
                cost_delta=Decimal("-75.00"),
                pct_change=None,
                status="deleted",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        node = resp.json()["nodes"][0]
        assert node["status"] == "deleted"
        assert node["cost_after"] == "0"
        assert node["pct_change"] is None

    def test_diff_pct_change_null_when_cost_before_zero(self) -> None:
        """V11: cost_before=0 always produces pct_change=null."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = [
            GraphDiffNodeData(
                id="lkc-xyz",
                resource_type="kafka_cluster",
                display_name="lkc-xyz",
                parent_id=None,
                cost_before=Decimal("0"),
                cost_after=Decimal("999.99"),
                cost_delta=Decimal("999.99"),
                pct_change=None,
                status="new",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        node = resp.json()["nodes"][0]
        assert node["pct_change"] is None


class TestGraphDiffRouteRootView:
    def test_diff_root_view_no_focus_param(self) -> None:
        """V12: no focus param → focus_id=None passed to repo, environment-level diff."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=_DIFF_PARAMS)

        assert resp.status_code == 200
        kwargs = mock_uow.graph.diff_neighborhood.call_args.kwargs
        assert kwargs["focus_id"] is None

    def test_diff_focus_param_passed_to_repo(self) -> None:
        """Route forwards focus param as focus_id to repo."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.return_value = []
        params = {**_DIFF_PARAMS, "focus": "lkc-abc"}
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/diff", params=params)

        kwargs = mock_uow.graph.diff_neighborhood.call_args.kwargs
        assert kwargs["focus_id"] == "lkc-abc"


class TestGraphDiffRouteErrorHandling:
    def test_diff_unknown_focus_returns_404(self) -> None:
        """KeyError from repo → HTTP 404."""
        mock_uow = MagicMock()
        mock_uow.graph.diff_neighborhood.side_effect = KeyError("does-not-exist")
        params = {**_DIFF_PARAMS, "focus": "does-not-exist"}
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff", params=params)

        assert resp.status_code == 404
        assert "does-not-exist" in resp.json()["detail"]

    def test_diff_missing_date_params_returns_422(self) -> None:
        """V20: missing from_start/from_end/to_start/to_end → HTTP 422."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/diff")

        assert resp.status_code == 422

    def test_diff_unknown_tenant_returns_404(self) -> None:
        """V19: unknown tenant name → 404."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/graph/diff", params=_DIFF_PARAMS)

        assert resp.status_code == 404
