from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.graph import GraphSearchResultData


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


def _make_search_result(
    entity_id: str,
    resource_type: str = "kafka_cluster",
    display_name: str | None = None,
    parent_id: str | None = "env-abc",
    status: str = "active",
) -> GraphSearchResultData:
    return GraphSearchResultData(
        id=entity_id,
        resource_type=resource_type,
        display_name=display_name or entity_id,
        parent_id=parent_id,
        status=status,
    )


class TestGraphSearchRouteReturnsResults:
    def test_search_returns_results_http_200(self) -> None:
        """V1: GET /graph/search?q=kafka returns results list, HTTP 200."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            _make_search_result("lkc-kafka-prod"),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "kafka"})

        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert len(data["results"]) == 1

    def test_search_passes_query_to_repo(self) -> None:
        """Route forwards q param to search_entities as query argument."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/search", params={"q": "kafka"})

        kwargs = mock_uow.graph.search_entities.call_args.kwargs
        assert kwargs["query"] == "kafka"

    def test_search_passes_ecosystem_and_tenant_id_to_repo(self) -> None:
        """Route forwards ecosystem and tenant_id from TenantConfig."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/search", params={"q": "x"})

        kwargs = mock_uow.graph.search_entities.call_args.kwargs
        assert kwargs["ecosystem"] == "eco"
        assert kwargs["tenant_id"] == "prod"


class TestGraphSearchRouteResultFields:
    def test_search_result_has_all_required_fields(self) -> None:
        """V3: each result has id, resource_type, display_name, parent_id, status."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            _make_search_result(
                "lkc-abc",
                resource_type="kafka_cluster",
                display_name="My Cluster",
                parent_id="env-abc",
                status="active",
            ),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "lkc"})

        result = resp.json()["results"][0]
        assert result["id"] == "lkc-abc"
        assert result["resource_type"] == "kafka_cluster"
        assert result["display_name"] == "My Cluster"
        assert result["parent_id"] == "env-abc"
        assert result["status"] == "active"

    def test_search_identity_result_has_parent_id_null(self) -> None:
        """V7: identity search result has parent_id=null (IdentityTable has no parent_id)."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            GraphSearchResultData(
                id="sa-001",
                resource_type="service_account",
                display_name="sa-001",
                parent_id=None,
                status="active",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "sa"})

        result = resp.json()["results"][0]
        assert result["parent_id"] is None


class TestGraphSearchRouteEmptyResult:
    def test_search_empty_returns_empty_list_http_200(self) -> None:
        """V6: no match → {"results": []}, HTTP 200."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "xyznonexistent"})

        assert resp.status_code == 200
        assert resp.json() == {"results": []}


class TestGraphSearchRouteValidation:
    def test_search_missing_q_returns_422(self) -> None:
        """V20: missing required q param → HTTP 422 from FastAPI validation."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search")

        assert resp.status_code == 422

    def test_search_empty_q_returns_422(self) -> None:
        """V20: q with min_length=1; empty string → HTTP 422."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": ""})

        assert resp.status_code == 422

    def test_search_unknown_tenant_returns_404(self) -> None:
        """V19: unknown tenant name → 404."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/graph/search", params={"q": "kafka"})

        assert resp.status_code == 404
