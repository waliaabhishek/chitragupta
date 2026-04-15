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


class TestGraphSearchReturnsParentDisplayName:
    def test_graph_search_returns_parent_display_name(self) -> None:
        """API response includes parent_display_name field resolved from backend."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            GraphSearchResultData(
                id="lkc-abc",
                resource_type="kafka_cluster",
                display_name="Kafka Prod",
                parent_id="env-abc",
                parent_display_name="ACME Env",
                status="active",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "lkc"})

        assert resp.status_code == 200
        result = resp.json()["results"][0]
        assert result["parent_display_name"] == "ACME Env"

    def test_graph_search_parent_display_name_null_for_identity(self) -> None:
        """Identity results have parent_display_name=null in API response."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            GraphSearchResultData(
                id="sa-001",
                resource_type="service_account",
                display_name="sa-001",
                parent_id=None,
                parent_display_name=None,
                status="active",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "sa"})

        result = resp.json()["results"][0]
        assert result["parent_display_name"] is None

    def test_graph_search_parent_display_name_null_when_parent_not_found(self) -> None:
        """Resource with parent_id but no resolved parent gets parent_display_name=null."""
        mock_uow = MagicMock()
        mock_uow.graph.search_entities.return_value = [
            GraphSearchResultData(
                id="lkc-orphan",
                resource_type="kafka_cluster",
                display_name="Orphan Cluster",
                parent_id="env-missing",
                parent_display_name=None,
                status="active",
            )
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/search", params={"q": "lkc-orphan"})

        result = resp.json()["results"][0]
        assert result["parent_id"] == "env-missing"
        assert result["parent_display_name"] is None
