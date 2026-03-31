from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock


def _make_tenant_config(**overrides: Any) -> Any:
    from core.config.models import TenantConfig

    defaults: dict[str, Any] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "lookback_days": 30,
        "cutoff_days": 5,
    }
    defaults.update(overrides)
    return TenantConfig(**defaults)


class TestGetTaConfigWithPlugin:
    """_get_ta_config must accept a plugin and delegate to get_overlay_config."""

    def test_get_ta_config_with_overlay_plugin_returns_config(self) -> None:
        from core.engine.orchestrator import _get_ta_config

        class FakeOverlayConfig:
            enabled: bool = True

        class FakePlugin:
            def get_overlay_config(self, name: str) -> object:
                if name == "topic_attribution":
                    return FakeOverlayConfig()
                return None

        result = _get_ta_config(FakePlugin())

        assert result is not None
        assert result.enabled is True

    def test_get_ta_config_without_overlay_plugin_returns_none(self) -> None:
        from core.engine.orchestrator import _get_ta_config
        from core.plugin.protocols import OverlayPlugin

        class NonOverlayPlugin:
            pass

        assert not isinstance(NonOverlayPlugin(), OverlayPlugin)
        result = _get_ta_config(NonOverlayPlugin())

        assert result is None


class TestGatherPhasePluginBasedConfig:
    """GatherPhase must derive topic attribution enabled state from bundle.plugin, not tenant_config."""

    def test_gather_phase_calls_get_overlay_config_on_plugin(self) -> None:
        from core.engine.orchestrator import GatherPhase

        class FakeOverlayConfig:
            enabled: bool = True

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=FakeOverlayConfig())

        bundle = MagicMock()
        bundle.plugin = mock_plugin
        tenant_config = _make_tenant_config()

        phase = GatherPhase(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tenant_config=tenant_config,
            bundle=bundle,
        )

        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")
        assert phase._topic_attribution_enabled is True

    def test_gather_phase_topic_attribution_disabled_when_plugin_not_overlay_plugin(self) -> None:
        from core.engine.orchestrator import GatherPhase
        from core.plugin.protocols import OverlayPlugin

        # spec without get_overlay_config → not an OverlayPlugin
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

        bundle = MagicMock()
        bundle.plugin = mock_plugin
        tenant_config = _make_tenant_config()

        phase = GatherPhase(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tenant_config=tenant_config,
            bundle=bundle,
        )

        assert phase._topic_attribution_enabled is False


class TestChargebackOrchestratorPluginBasedConfig:
    """ChargebackOrchestrator must derive TopicAttributionPhase config from plugin.get_overlay_config."""

    def test_chargeback_orchestrator_constructs_ta_phase_from_plugin_config(self) -> None:
        from core.engine.orchestrator import ChargebackOrchestrator
        from plugins.confluent_cloud.config import TopicAttributionConfig

        ta_config = TopicAttributionConfig(enabled=True)

        mock_plugin = MagicMock()
        mock_plugin.get_overlay_config = MagicMock(return_value=ta_config)
        mock_plugin.ecosystem = "confluent_cloud"
        mock_plugin.get_service_handlers.return_value = {}
        mock_plugin.get_fallback_allocator.return_value = None

        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_storage.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_storage.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)

        tenant_config = _make_tenant_config(ecosystem="confluent_cloud")

        orchestrator = ChargebackOrchestrator(
            tenant_name="test",
            tenant_config=tenant_config,
            plugin=mock_plugin,
            storage_backend=mock_storage,
        )

        mock_plugin.get_overlay_config.assert_called_with("topic_attribution")
        assert orchestrator._topic_overlay_phase is not None

    def test_chargeback_orchestrator_no_ta_phase_when_plugin_not_overlay_plugin(self) -> None:
        from core.engine.orchestrator import ChargebackOrchestrator
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
        mock_plugin.ecosystem = "confluent_cloud"
        mock_plugin.get_service_handlers.return_value = {}
        mock_plugin.get_fallback_allocator.return_value = None
        assert not isinstance(mock_plugin, OverlayPlugin)

        mock_storage = MagicMock()
        mock_uow = MagicMock()
        mock_storage.create_unit_of_work.return_value.__enter__ = MagicMock(return_value=mock_uow)
        mock_storage.create_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)

        tenant_config = _make_tenant_config(ecosystem="confluent_cloud")

        orchestrator = ChargebackOrchestrator(
            tenant_name="test",
            tenant_config=tenant_config,
            plugin=mock_plugin,
            storage_backend=mock_storage,
        )

        assert orchestrator._topic_overlay_phase is None
