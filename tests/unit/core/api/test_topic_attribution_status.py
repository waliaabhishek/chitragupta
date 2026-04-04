from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from core.api.schemas import TenantReadiness, TenantStatusDetailResponse, TenantStatusSummary
from core.api.topic_attribution_status import TopicAttributionStatus, resolve_topic_attribution_status
from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
from core.metrics.config import MetricsConnectionConfig
from plugins.confluent_cloud.config import CCloudCredentials, CCloudPluginConfig, TopicAttributionConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ccloud_plugin_config(enabled: bool) -> CCloudPluginConfig:
    return CCloudPluginConfig(
        ccloud_api=CCloudCredentials(key="test-key", secret="test-secret"),  # type: ignore[arg-type]
        topic_attribution=TopicAttributionConfig(enabled=enabled),
        metrics=MetricsConnectionConfig(url="http://prometheus:9090") if enabled else None,
    )


def _make_raw_plugin_settings_with_ta_enabled() -> PluginSettingsBase:
    return PluginSettingsBase.model_validate(
        {
            "ccloud_api": {"key": "test-key", "secret": "test-secret"},  # pragma: allowlist secret
            "topic_attribution": {"enabled": True},
            # No metrics — will trigger config_error
        }
    )


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
# Unit tests — resolve_topic_attribution_status
# ---------------------------------------------------------------------------


class TestResolveTopicAttributionStatus:
    def test_resolve_disabled_no_topic_attribution_field(self) -> None:
        """PluginSettingsBase() with no topic_attribution field → status='disabled', error=None."""
        result = resolve_topic_attribution_status(PluginSettingsBase(), ecosystem="confluent_cloud")
        assert result == TopicAttributionStatus(status="disabled", error=None)

    def test_resolve_disabled_when_enabled_false_dict(self) -> None:
        """plugin_settings with topic_attribution={"enabled": False} → status='disabled'."""
        settings = PluginSettingsBase.model_validate({"topic_attribution": {"enabled": False}})
        result = resolve_topic_attribution_status(settings, ecosystem="confluent_cloud")
        assert result.status == "disabled"

    def test_resolve_config_error_when_metrics_missing(self) -> None:
        """Raw PluginSettingsBase with topic_attribution enabled but no metrics.

        → status='config_error', error contains 'metrics'.
        """
        settings = _make_raw_plugin_settings_with_ta_enabled()
        result = resolve_topic_attribution_status(settings, ecosystem="confluent_cloud")
        assert result.status == "config_error"
        assert result.error is not None
        assert "metrics" in result.error

    def test_resolve_enabled_when_ccloud_config_valid(self) -> None:
        """CCloudPluginConfig-compatible dict with topic_attribution.enabled=True and metrics present.

        → status='enabled'.
        """
        config = _make_ccloud_plugin_config(enabled=True)
        result = resolve_topic_attribution_status(config, ecosystem="confluent_cloud")
        assert result.status == "enabled"

    def test_resolve_enabled_non_ccloud_no_validation(self) -> None:
        """Non-ccloud ecosystem with topic_attribution={"enabled": True} → status='enabled' (no validation)."""
        settings = PluginSettingsBase.model_validate({"topic_attribution": {"enabled": True}})
        result = resolve_topic_attribution_status(settings, ecosystem="other")
        assert result.status == "enabled"


# ---------------------------------------------------------------------------
# Schema serialization tests
# ---------------------------------------------------------------------------


class TestTenantStatusSummaryTopicAttributionStatus:
    def test_tenant_status_summary_disabled_serializes_correctly(self) -> None:
        """TenantStatusSummary with topic_attribution_status='disabled', error=None serializes correctly."""
        summary = TenantStatusSummary(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            dates_pending=0,
            dates_calculated=10,
            last_calculated_date=None,
            topic_attribution_status="disabled",
            topic_attribution_error=None,
        )
        data = summary.model_dump()
        assert data["topic_attribution_status"] == "disabled"
        assert data["topic_attribution_error"] is None
        assert "topic_attribution_enabled" not in data

    def test_tenant_status_summary_config_error_serializes_correctly(self) -> None:
        """TenantStatusSummary with topic_attribution_status='config_error' and error serializes correctly."""
        summary = TenantStatusSummary(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            dates_pending=0,
            dates_calculated=10,
            last_calculated_date=None,
            topic_attribution_status="config_error",
            topic_attribution_error="some error",
        )
        data = summary.model_dump()
        assert data["topic_attribution_status"] == "config_error"
        assert data["topic_attribution_error"] == "some error"


class TestTenantStatusDetailResponseTopicAttributionStatus:
    def test_tenant_status_detail_response_serializes_correctly(self) -> None:
        """TenantStatusDetailResponse with new fields serializes correctly."""
        detail = TenantStatusDetailResponse(
            tenant_name="acme",
            tenant_id="t-001",
            ecosystem="ccloud",
            states=[],
            topic_attribution_status="disabled",
            topic_attribution_error=None,
        )
        data = detail.model_dump()
        assert data["topic_attribution_status"] == "disabled"
        assert data["topic_attribution_error"] is None
        assert "topic_attribution_enabled" not in data


class TestTenantReadinessTopicAttributionStatus:
    def test_tenant_readiness_serializes_correctly(self) -> None:
        """TenantReadiness with new fields serializes correctly."""
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
            topic_attribution_status="disabled",
            topic_attribution_error=None,
        )
        data = readiness.model_dump()
        assert data["topic_attribution_status"] == "disabled"
        assert data["topic_attribution_error"] is None
        assert "topic_attribution_enabled" not in data


# ---------------------------------------------------------------------------
# HTTP integration tests — /api/v1/tenants
# ---------------------------------------------------------------------------


class TestListTenantsTopicAttributionStatus:
    def test_list_tenants_default_returns_disabled_status(self) -> None:
        """GET /api/v1/tenants with default plugin settings → topic_attribution_status: 'disabled', error: null."""
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
        assert tenant["topic_attribution_status"] == "disabled"
        assert tenant["topic_attribution_error"] is None
        assert "topic_attribution_enabled" not in tenant

    def test_list_tenants_enabled_config_returns_enabled_status(self) -> None:
        """GET /api/v1/tenants with CCloudPluginConfig enabled=True + metrics → topic_attribution_status: 'enabled'."""
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
        assert tenant["topic_attribution_status"] == "enabled"
        assert tenant["topic_attribution_error"] is None

    def test_list_tenants_config_error_returns_config_error_status(self) -> None:
        """GET /api/v1/tenants with TA enabled but no metrics, ecosystem=confluent_cloud.

        → topic_attribution_status: 'config_error'.
        """
        from core.api.app import create_app  # noqa: PLC0415

        settings = AppSettings(
            tenants={
                "acme": TenantConfig(
                    tenant_id="t-001",
                    ecosystem="confluent_cloud",
                    storage=StorageConfig(connection_string="sqlite:///:memory:"),
                    plugin_settings=_make_raw_plugin_settings_with_ta_enabled(),
                )
            }
        )
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
        assert tenant["topic_attribution_status"] == "config_error"
        assert tenant["topic_attribution_error"] is not None
        assert "metrics" in tenant["topic_attribution_error"]


# ---------------------------------------------------------------------------
# HTTP integration tests — /api/v1/tenants/{tenant}/status
# ---------------------------------------------------------------------------


class TestTenantStatusTopicAttributionStatus:
    def test_tenant_status_default_returns_disabled(self) -> None:
        """GET /api/v1/tenants/acme/status with default config → topic_attribution_status: 'disabled'."""
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
        assert body["topic_attribution_status"] == "disabled"
        assert "topic_attribution_enabled" not in body

    def test_tenant_status_enabled_config_returns_enabled(self) -> None:
        """GET /api/v1/tenants/acme/status with CCloudPluginConfig enabled → topic_attribution_status: 'enabled'."""
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
        assert body["topic_attribution_status"] == "enabled"


# ---------------------------------------------------------------------------
# HTTP integration test — /api/v1/readiness
# ---------------------------------------------------------------------------


class TestReadinessTopicAttributionStatus:
    def test_readiness_returns_new_status_fields(self) -> None:
        """GET /api/v1/readiness → new fields present, topic_attribution_enabled absent."""
        import core.api.routes.readiness as readiness_module  # noqa: PLC0415
        from core.api.app import create_app  # noqa: PLC0415

        settings = _make_settings_with_tenant()
        app = create_app(settings, workflow_runner=None, mode="api")
        backend = _make_readiness_backend()

        readiness_module._readiness_cache = None  # reset TTL cache to avoid ordering dependency
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
            assert "topic_attribution_status" in tenant
            assert "topic_attribution_error" in tenant
            assert "topic_attribution_enabled" not in tenant
