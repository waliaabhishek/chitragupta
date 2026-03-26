from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig

if TYPE_CHECKING:
    from core.models.identity import CoreIdentity
    from core.models.resource import CoreResource


def _make_settings() -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={
            "t": TenantConfig(
                tenant_id="t",
                ecosystem="eco",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        },
    )


@contextmanager
def _app_with_mock_uow(mock_uow: MagicMock) -> Iterator[TestClient]:
    """Create TestClient with a dependency_overrides-based unit-of-work mock."""
    settings = _make_settings()
    app = create_app(settings)

    def _uow_override() -> Iterator[MagicMock]:
        yield mock_uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _make_identity_uow(items: list[CoreIdentity] | None = None) -> MagicMock:
    """Return a mock UoW whose identities.find_paginated returns (items, total)."""
    items = items or []
    mock_uow = MagicMock()
    mock_uow.identities.find_paginated.return_value = (items, len(items))
    mock_uow.identities.find_active_at.return_value = (items, len(items))
    mock_uow.identities.find_by_period.return_value = (items, len(items))
    return mock_uow


def _make_resource_uow(items: list[CoreResource] | None = None) -> MagicMock:
    """Return a mock UoW whose resources.find_paginated returns (items, total)."""
    items = items or []
    mock_uow = MagicMock()
    mock_uow.resources.find_paginated.return_value = (items, len(items))
    mock_uow.resources.find_active_at.return_value = (items, len(items))
    mock_uow.resources.find_by_period.return_value = (items, len(items))
    return mock_uow


# ---------------------------------------------------------------------------
# list_identities — new query params accepted and forwarded
# ---------------------------------------------------------------------------


class TestListIdentitiesSearchParam:
    def test_search_param_forwarded_to_find_paginated(self) -> None:
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/identities", params={"search": "alice"})

        assert resp.status_code == 200
        mock_uow.identities.find_paginated.assert_called_once()
        _, kwargs = mock_uow.identities.find_paginated.call_args
        assert kwargs.get("search") == "alice"

    def test_sort_by_param_forwarded(self) -> None:
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/identities",
                params={"sort_by": "display_name", "sort_order": "desc"},
            )

        assert resp.status_code == 200
        mock_uow.identities.find_paginated.assert_called_once()
        _, kwargs = mock_uow.identities.find_paginated.call_args
        assert kwargs.get("sort_by") == "display_name"
        assert kwargs.get("sort_order") == "desc"

    def test_tag_key_param_forwarded_with_tags_repo(self) -> None:
        """When tag_key is set, tags_repo=uow.tags must be passed."""
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/identities",
                params={"tag_key": "cost_center", "tag_value": "eng"},
            )

        assert resp.status_code == 200
        mock_uow.identities.find_paginated.assert_called_once()
        _, kwargs = mock_uow.identities.find_paginated.call_args
        assert kwargs.get("tag_key") == "cost_center"
        assert kwargs.get("tag_value") == "eng"
        assert kwargs.get("tags_repo") is mock_uow.tags

    def test_no_tag_key_passes_tags_repo_none(self) -> None:
        """When tag_key is absent, tags_repo=None must be passed."""
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/identities")

        assert resp.status_code == 200
        mock_uow.identities.find_paginated.assert_called_once()
        _, kwargs = mock_uow.identities.find_paginated.call_args
        assert kwargs.get("tags_repo") is None

    def test_all_new_params_accepted_no_422(self) -> None:
        """Route must accept all new params without returning 422."""
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/identities",
                params={
                    "search": "test",
                    "sort_by": "identity_id",
                    "sort_order": "desc",
                    "tag_key": "env",
                    "tag_value": "prod",
                },
            )

        assert resp.status_code == 200

    def test_search_none_when_not_provided(self) -> None:
        """When search is absent, find_paginated gets search=None."""
        mock_uow = _make_identity_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/identities")

        assert resp.status_code == 200
        _, kwargs = mock_uow.identities.find_paginated.call_args
        assert kwargs.get("search") is None


# ---------------------------------------------------------------------------
# list_resources — new query params accepted and forwarded
# ---------------------------------------------------------------------------


class TestListResourcesSearchParam:
    def test_search_param_forwarded_to_find_paginated(self) -> None:
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/resources", params={"search": "kafka"})

        assert resp.status_code == 200
        mock_uow.resources.find_paginated.assert_called_once()
        _, kwargs = mock_uow.resources.find_paginated.call_args
        assert kwargs.get("search") == "kafka"

    def test_sort_by_param_forwarded(self) -> None:
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/resources",
                params={"sort_by": "display_name", "sort_order": "desc"},
            )

        assert resp.status_code == 200
        mock_uow.resources.find_paginated.assert_called_once()
        _, kwargs = mock_uow.resources.find_paginated.call_args
        assert kwargs.get("sort_by") == "display_name"
        assert kwargs.get("sort_order") == "desc"

    def test_tag_key_param_forwarded_with_tags_repo(self) -> None:
        """When tag_key is set, tags_repo=uow.tags must be passed."""
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/resources",
                params={"tag_key": "env", "tag_value": "prod"},
            )

        assert resp.status_code == 200
        mock_uow.resources.find_paginated.assert_called_once()
        _, kwargs = mock_uow.resources.find_paginated.call_args
        assert kwargs.get("tag_key") == "env"
        assert kwargs.get("tag_value") == "prod"
        assert kwargs.get("tags_repo") is mock_uow.tags

    def test_no_tag_key_passes_tags_repo_none(self) -> None:
        """When tag_key is absent, tags_repo=None must be passed."""
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/resources")

        assert resp.status_code == 200
        mock_uow.resources.find_paginated.assert_called_once()
        _, kwargs = mock_uow.resources.find_paginated.call_args
        assert kwargs.get("tags_repo") is None

    def test_all_new_params_accepted_no_422(self) -> None:
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/resources",
                params={
                    "search": "db",
                    "sort_by": "display_name",
                    "sort_order": "desc",
                    "tag_key": "env",
                    "tag_value": "prod",
                },
            )

        assert resp.status_code == 200

    def test_search_none_when_not_provided(self) -> None:
        mock_uow = _make_resource_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/resources")

        assert resp.status_code == 200
        _, kwargs = mock_uow.resources.find_paginated.call_args
        assert kwargs.get("search") is None
