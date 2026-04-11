from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig


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
    settings = _make_settings()
    app = create_app(settings)

    def _uow_override() -> Iterator[MagicMock]:
        yield mock_uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"), TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _make_ta_uow() -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.topic_attributions.find_by_filters.return_value = ([], 0)
    mock_uow.topic_attributions.iter_by_filters.return_value = iter([])
    return mock_uow


# ---------------------------------------------------------------------------
# Verification case 6: Protocol backward-compat + tag params forwarding
# ---------------------------------------------------------------------------


class TestTopicAttributionListTagParamsForwarded:
    def test_tag_key_forwarded_to_find_by_filters_with_tags_repo(self) -> None:
        """tag_key must be forwarded to find_by_filters; tags_repo=uow.tags always set."""
        mock_uow = _make_ta_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/topic-attributions",
                params={"tag_key": "owner"},
            )

        assert resp.status_code == 200
        mock_uow.topic_attributions.find_by_filters.assert_called_once()
        call_kwargs = mock_uow.topic_attributions.find_by_filters.call_args.kwargs
        assert call_kwargs.get("tag_key") == "owner"
        assert call_kwargs.get("tag_value") is None
        assert call_kwargs.get("tags_repo") is mock_uow.tags

    def test_tag_key_and_value_forwarded_to_find_by_filters(self) -> None:
        """Both tag_key and tag_value are forwarded; tags_repo=uow.tags always set."""
        mock_uow = _make_ta_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/topic-attributions",
                params={"tag_key": "owner", "tag_value": "alice"},
            )

        assert resp.status_code == 200
        mock_uow.topic_attributions.find_by_filters.assert_called_once()
        call_kwargs = mock_uow.topic_attributions.find_by_filters.call_args.kwargs
        assert call_kwargs.get("tag_key") == "owner"
        assert call_kwargs.get("tag_value") == "alice"
        assert call_kwargs.get("tags_repo") is mock_uow.tags

    def test_no_tag_params_tags_repo_still_passed(self) -> None:
        """When no tag params supplied, tags_repo=uow.tags is still forwarded."""
        mock_uow = _make_ta_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/topic-attributions")

        assert resp.status_code == 200
        mock_uow.topic_attributions.find_by_filters.assert_called_once()
        call_kwargs = mock_uow.topic_attributions.find_by_filters.call_args.kwargs
        assert call_kwargs.get("tag_key") is None
        assert call_kwargs.get("tag_value") is None
        assert call_kwargs.get("tags_repo") is mock_uow.tags

    def test_tag_key_and_value_params_accepted_no_422(self) -> None:
        """Route must accept tag_key and tag_value without returning 422."""
        mock_uow = _make_ta_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/t/topic-attributions",
                params={"tag_key": "env", "tag_value": "prod"},
            )

        assert resp.status_code == 200

    def test_backward_compat_no_tag_params_returns_200(self) -> None:
        """Protocol backward-compat: calling without tag params still returns 200."""
        mock_uow = _make_ta_uow()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/t/topic-attributions")

        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data
