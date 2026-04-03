from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from core.api.schemas import TenantReadiness, TenantStatusDetailResponse, TenantStatusSummary
from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
from core.metrics.config import MetricsConnectionConfig
from plugins.confluent_cloud.config import CCloudCredentials, CCloudPluginConfig, TopicAttributionConfig


def _make_ccloud_plugin_config(enabled: bool) -> CCloudPluginConfig:
    return CCloudPluginConfig(
        ccloud_api=CCloudCredentials(key="test-key", secret="test-secret"),  # type: ignore[arg-type]
        topic_attribution=TopicAttributionConfig(enabled=enabled),
        metrics=MetricsConnectionConfig(url="http://prometheus:9090") if enabled else None,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_backend(count: int = 0) -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.pipeline_state.count_pending.return_value = 0
    mock_uow.pipeline_state.count_calculated.return_value = count
    mock_uow.pipeline_state.get_last_calculated_date.return_value = None
    mock_uow.__enter__ = MagicMock(return_value=mock_uow)
    mock_uow.__exit__ = MagicMock(return_value=False)
    mock_backend = MagicMock()
    mock_backend.create_read_only_unit_of_work.return_value = mock_uow
    return mock_backend


def _make_readiness_backend(status: str = "completed") -> MagicMock:
    run = MagicMock()
    run.status = status
    run.stage = None
    run.current_date = None
    run.started_at = datetime(2026, 4, 1, 10, 0, 0)
    run.ended_at = datetime(2026, 4, 1, 10, 5, 0)
    mock_uow = MagicMock()
    mock_uow.pipeline_runs.get_latest_run.return_value = run
    mock_uow.pipeline_state.count_calculated.return_value = 1
    mock_uow.__enter__ = MagicMock(return_value=mock_uow)
    mock_uow.__exit__ = MagicMock(return_value=False)
    mock_backend = MagicMock()
    mock_backend.create_unit_of_work.return_value = mock_uow
    mock_backend.create_read_only_unit_of_work.return_value = mock_uow
    return mock_backend


def _make_settings_with_tenant(
    plugin_settings: PluginSettingsBase | None = None,
) -> AppSettings:
    return AppSettings(
        tenants={
            "acme": TenantConfig(
                tenant_id="t-001",
                ecosystem="ccloud",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
                **({"plugin_settings": plugin_settings} if plugin_settings else {}),
            )
        }
    )


# ---------------------------------------------------------------------------
# Schema serialization tests
# ---------------------------------------------------------------------------


class TestTenantStatusSummaryTopicAttributionEnabled:
    def test_tenant_status_summary_includes_topic_attribution_enabled_false(self) -> None:
        """TenantStatusSummary with topic_attribution_enabled=False serialises correctly."""
        summary = TenantStatusSummary(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            dates_pending=0,
            dates_calculated=10,
            last_calculated_date=None,
            topic_attribution_enabled=False,
        )
        data = summary.model_dump()
        assert "topic_attribution_enabled" in data
        assert data["topic_attribution_enabled"] is False

    def test_tenant_status_summary_includes_topic_attribution_enabled_true(self) -> None:
        """TenantStatusSummary with topic_attribution_enabled=True serialises correctly."""
        summary = TenantStatusSummary(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            dates_pending=0,
            dates_calculated=10,
            last_calculated_date=None,
            topic_attribution_enabled=True,
        )
        data = summary.model_dump()
        assert "topic_attribution_enabled" in data
        assert data["topic_attribution_enabled"] is True


class TestTenantStatusDetailResponseTopicAttributionEnabled:
    def test_tenant_status_detail_response_includes_topic_attribution_enabled(self) -> None:
        """TenantStatusDetailResponse with topic_attribution_enabled=False serialises correctly."""
        detail = TenantStatusDetailResponse(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            states=[],
            topic_attribution_enabled=False,
        )
        data = detail.model_dump()
        assert "topic_attribution_enabled" in data
        assert data["topic_attribution_enabled"] is False


class TestTenantReadinessTopicAttributionEnabled:
    def test_tenant_readiness_includes_topic_attribution_enabled(self) -> None:
        """TenantReadiness with topic_attribution_enabled=False serialises correctly."""
        readiness = TenantReadiness(
            tenant_name="acme",
            tables_ready=True,
            has_data=True,
            pipeline_running=False,
            pipeline_stage=None,
            pipeline_current_date=None,
            last_run_status=None,
            last_run_at=None,
            permanent_failure=None,
            topic_attribution_enabled=False,
        )
        data = readiness.model_dump()
        assert "topic_attribution_enabled" in data
        assert data["topic_attribution_enabled"] is False


# ---------------------------------------------------------------------------
# _is_topic_attribution_enabled function tests
# ---------------------------------------------------------------------------


class TestIsTopicAttributionEnabled:
    def test_is_topic_attribution_enabled_returns_false_for_base_plugin_settings(self) -> None:
        """PluginSettingsBase() → _is_topic_attribution_enabled returns False."""
        from core.api.routes.tenants import _is_topic_attribution_enabled  # noqa: PLC0415

        result = _is_topic_attribution_enabled(PluginSettingsBase())
        assert result is False

    def test_is_topic_attribution_enabled_returns_true_when_enabled(self) -> None:
        """CCloudPluginConfig with topic_attribution enabled=True → returns True."""
        from core.api.routes.tenants import _is_topic_attribution_enabled  # noqa: PLC0415

        config = _make_ccloud_plugin_config(enabled=True)
        result = _is_topic_attribution_enabled(config)
        assert result is True

    def test_is_topic_attribution_enabled_returns_false_when_disabled(self) -> None:
        """CCloudPluginConfig with topic_attribution enabled=False → returns False."""
        from core.api.routes.tenants import _is_topic_attribution_enabled  # noqa: PLC0415

        config = _make_ccloud_plugin_config(enabled=False)
        result = _is_topic_attribution_enabled(config)
        assert result is False


# ---------------------------------------------------------------------------
# HTTP integration tests — /api/v1/tenants
# ---------------------------------------------------------------------------


class TestListTenantsTopicAttributionEnabled:
    def test_list_tenants_returns_topic_attribution_enabled_false(self) -> None:
        """GET /api/v1/tenants with default plugin settings → topic_attribution_enabled: false."""
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant()
        app = create_app(settings, mode="api")
        backend = _make_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.tenants.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/tenants")

        assert response.status_code == 200
        body = response.json()
        assert len(body["tenants"]) == 1
        tenant = body["tenants"][0]
        assert "topic_attribution_enabled" in tenant
        assert tenant["topic_attribution_enabled"] is False

    def test_list_tenants_returns_topic_attribution_enabled_true(self) -> None:
        """GET /api/v1/tenants with topic_attribution enabled returns true."""
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant(plugin_settings=_make_ccloud_plugin_config(enabled=True))
        app = create_app(settings, mode="api")
        backend = _make_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.tenants.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/tenants")

        assert response.status_code == 200
        body = response.json()
        assert len(body["tenants"]) == 1
        tenant = body["tenants"][0]
        assert "topic_attribution_enabled" in tenant
        assert tenant["topic_attribution_enabled"] is True


# ---------------------------------------------------------------------------
# HTTP integration test — /api/v1/tenants/{tenant}/status
# ---------------------------------------------------------------------------


class TestTenantStatusTopicAttributionEnabled:
    def test_tenant_status_returns_topic_attribution_enabled_false(self) -> None:
        """GET /api/v1/tenants/acme/status → topic_attribution_enabled: false when not configured."""
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant()
        app = create_app(settings, mode="api")
        backend = _make_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.dependencies.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/tenants/acme/status")

        assert response.status_code == 200
        body = response.json()
        assert body["tenant_name"] == "acme"
        assert body["topic_attribution_enabled"] is False

    def test_tenant_status_returns_topic_attribution_enabled_true(self) -> None:
        """GET /api/v1/tenants/acme/status with CCloudPluginConfig enabled → topic_attribution_enabled: true."""
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant(plugin_settings=_make_ccloud_plugin_config(enabled=True))
        app = create_app(settings, mode="api")
        backend = _make_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.dependencies.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/tenants/acme/status")

        assert response.status_code == 200
        body = response.json()
        assert body["tenant_name"] == "acme"
        assert body["topic_attribution_enabled"] is True


# ---------------------------------------------------------------------------
# HTTP integration test — /api/v1/readiness
# ---------------------------------------------------------------------------


class TestReadinessTopicAttributionEnabled:
    def test_readiness_returns_topic_attribution_enabled(self) -> None:
        """GET /api/v1/readiness → topic_attribution_enabled present in each tenant object."""
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant()
        app = create_app(settings, workflow_runner=None, mode="api")
        backend = _make_readiness_backend()

        with (
            patch("workflow_runner.cleanup_orphaned_runs_for_all_tenants"),
            patch("core.api.routes.readiness.get_or_create_backend", return_value=backend),
            TestClient(app) as client,
        ):
            response = client.get("/api/v1/readiness")

        assert response.status_code == 200
        body = response.json()
        assert len(body["tenants"]) >= 1
        for tenant in body["tenants"]:
            assert tenant["topic_attribution_enabled"] is False
