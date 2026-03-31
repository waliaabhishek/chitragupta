from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock, patch

from core.config.models import AppSettings, FeaturesConfig, StorageConfig, TenantConfig
from core.engine.orchestrator import PipelineRunResult
from workflow_runner import WorkflowRunner


def _make_tenant(**overrides: Any) -> TenantConfig:
    unique = uuid.uuid4().hex[:8]
    defaults: dict[str, Any] = {
        "ecosystem": "eco",
        "tenant_id": "tid1",
        "lookback_days": 30,
        "cutoff_days": 5,
        "storage": StorageConfig(connection_string=f"sqlite:///test_{unique}.db"),
        "retention_days": 30,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class TestOverlayConfigIntegration:
    """Full data flow: WorkflowRunner -> TenantRuntime.plugin -> get_overlay_config -> OverlayConfig."""

    @patch("workflow_runner.ChargebackOrchestrator")
    @patch("core.storage.registry.create_storage_backend")
    def test_workflow_runner_call_chain_uses_get_overlay_config(
        self, mock_storage: MagicMock, mock_orch_cls: MagicMock
    ) -> None:
        """After fix: WorkflowRunner accesses TA config via plugin.get_overlay_config, not getattr.

        Verifies the full call chain from WorkflowRunner._run_tenant through
        TenantRuntime.plugin.get_overlay_config to an OverlayConfig instance.
        Storage and metrics are mocked; only the config-access path is asserted.
        """
        from core.plugin.protocols import OverlayConfig, OverlayPlugin
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True)

        class _FakeOverlayPlugin:
            """Concrete stub satisfying OverlayPlugin — tests the protocol, not MagicMock duck typing."""

            def initialize(self, config: dict) -> None:
                pass

            def get_storage_module(self) -> Any:
                return MagicMock()

            def get_metrics_source(self) -> None:
                return None

            def close(self) -> None:
                pass

        fake_plugin = _FakeOverlayPlugin()
        fake_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        assert isinstance(fake_plugin, OverlayPlugin)

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
        registry.create.return_value = fake_plugin

        tenant = _make_tenant(ecosystem="eco", tenant_id="tid1")
        settings = AppSettings(tenants={"t1": tenant}, features=FeaturesConfig())
        runner = WorkflowRunner(settings, registry)

        runner._run_tenant("t1", tenant)

        # Verify the call chain: WorkflowRunner -> runtime.plugin -> get_overlay_config
        fake_plugin.get_overlay_config.assert_called_with("topic_attribution")

        # Verify the returned config satisfies OverlayConfig protocol
        overlay_cfg = fake_plugin.get_overlay_config.return_value
        assert isinstance(overlay_cfg, OverlayConfig)
        assert overlay_cfg.enabled is True
