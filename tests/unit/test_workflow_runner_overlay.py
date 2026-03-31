from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

from core.config.models import AppSettings, FeaturesConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import TenantRuntime, WorkflowRunner


def _make_settings(tenants: dict[str, TenantConfig] | None = None) -> AppSettings:
    return AppSettings(tenants=tenants or {}, features=FeaturesConfig())


def _make_tenant(**overrides: Any) -> TenantConfig:
    unique = uuid.uuid4().hex[:8]
    defaults: dict[str, Any] = {
        "ecosystem": "eco",
        "tenant_id": "tid",
        "lookback_days": 30,
        "cutoff_days": 5,
        "storage": StorageConfig(connection_string=f"sqlite:///test_{unique}.db"),
        "retention_days": 30,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


def _make_mock_backend_with_uow() -> tuple[MagicMock, MagicMock]:
    mock_backend = MagicMock()
    mock_uow = MagicMock()
    mock_backend.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
    mock_backend.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
    mock_uow.billing.delete_before.return_value = 0
    mock_uow.resources.delete_before.return_value = 0
    mock_uow.identities.delete_before.return_value = 0
    mock_uow.chargebacks.delete_before.return_value = 0
    mock_uow.topic_attributions.delete_before.return_value = 0
    return mock_backend, mock_uow


class TestRunTenantOverlayConfigAccess:
    """_run_tenant must access topic attribution config via runtime.plugin, not getattr on config."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_accesses_ta_config_via_plugin_get_overlay_config(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True)
        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        mock_plugin.get_metrics_source.return_value = None

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        runner._run_tenant("t1", tenant)

        # New behavior: TA config accessed via plugin.get_overlay_config, not config.plugin_settings
        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_plugin_without_overlay_plugin_handled_gracefully(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        from core.plugin.protocols import OverlayPlugin

        mock_plugin = MagicMock(
            spec=[
                "ecosystem",
                "initialize",
                "get_service_handlers",
                "get_cost_input",
                "get_metrics_source",
                "get_fallback_allocator",
                "build_shared_context",
                "get_storage_module",
                "close",
            ]
        )
        mock_plugin.get_metrics_source.return_value = None
        assert not isinstance(mock_plugin, OverlayPlugin)

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        # Must not raise even though plugin does not implement OverlayPlugin
        result = runner._run_tenant("t1", tenant)
        assert result is not None


class TestCleanupRetentionOverlayConfigAccess:
    """_cleanup_retention must access topic attribution config via runtime.plugin, not getattr on config."""

    def test_cleanup_retention_accesses_ta_config_via_plugin(self) -> None:
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True, retention_days=45)

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)

        mock_backend, mock_uow = _make_mock_backend_with_uow()

        # Tenant config WITHOUT TA in plugin_settings — old getattr path would skip TA cleanup
        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime

        runner._cleanup_retention()

        # New behavior: TA config accessed via plugin, not config.plugin_settings
        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")

    def test_cleanup_retention_plugin_without_overlay_plugin_no_ta_cleanup(self) -> None:
        from core.plugin.protocols import OverlayPlugin

        mock_plugin = MagicMock(
            spec=[
                "ecosystem",
                "initialize",
                "get_service_handlers",
                "get_cost_input",
                "get_metrics_source",
                "get_fallback_allocator",
                "build_shared_context",
                "get_storage_module",
                "close",
            ]
        )
        assert not isinstance(mock_plugin, OverlayPlugin)

        mock_backend, mock_uow = _make_mock_backend_with_uow()

        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1", retention_days=30)
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, MagicMock())

        runtime = TenantRuntime(
            tenant_name="t1",
            plugin=mock_plugin,
            storage=mock_backend,
            orchestrator=MagicMock(),
            config_hash="abc123",
            created_at=datetime.now(UTC),
        )
        runner._tenant_runtimes["t1"] = runtime

        runner._cleanup_retention()

        mock_uow.topic_attributions.delete_before.assert_not_called()


class TestRunTenantTaEmitter:
    """GIT-175-04: _run_tenant runs TA emitter when ta_config.enabled=True and emitters configured."""

    @patch("workflow_runner.EmitterRunner")
    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_run_tenant_runs_ta_emitter_when_emitters_configured(
        self,
        mock_storage: MagicMock,
        mock_orch_cls: MagicMock,
        mock_emitter_cls: MagicMock,
    ) -> None:
        """When ta_config.enabled=True and emitters list is non-empty, EmitterRunner.run is called."""
        from core.config.models import EmitterSpec
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(
            enabled=True,
            emitters=[EmitterSpec(type="csv", params={"output_dir": "/tmp/ta"})],
        )

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        mock_plugin.get_metrics_source.return_value = None

        mock_backend = MagicMock()
        mock_storage.return_value = mock_backend

        mock_orch = MagicMock()
        mock_orch.run.return_value = PipelineRunResult(
            tenant_name="t1",
            tenant_id="tid1",
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
        )
        mock_orch_cls.return_value = mock_orch

        registry = MagicMock()
        registry.create.return_value = mock_plugin

        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1")
        settings = _make_settings(tenants={"t1": tenant})
        runner = WorkflowRunner(settings, registry)

        runner._run_tenant("t1", tenant)

        mock_emitter_cls.assert_called_once()
        mock_emitter_cls.return_value.run.assert_called_once_with("tid1")
