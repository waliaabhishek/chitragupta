"""Tests for SelfManagedKafkaPlugin."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from core.models import MetricQuery, MetricRow


@pytest.fixture
def base_settings() -> dict:
    return {
        "cluster_id": "kafka-001",
        "metrics_identifier": "kraft-a-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": {"source": "static"},
        "metrics": {"url": "http://prom:9090"},
    }


class _MetricsSourceFake:
    """Typed MetricsSource double for plugin factory wiring tests."""

    def __init__(self) -> None:
        self.closed = False
        self.query_calls: list[tuple[Sequence[MetricQuery], datetime, datetime, timedelta, str | None]] = []

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        self.query_calls.append((queries, start, end, step, resource_id_filter))
        return {}

    def close(self) -> None:
        self.closed = True


class TestPluginEcosystemProperty:
    def test_ecosystem_is_self_managed_kafka(self):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        assert plugin.ecosystem == "self_managed_kafka"


class TestPluginInitialize:
    def test_initialize_with_valid_config(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert plugin._config is not None
        assert plugin._config.cluster_id == "kafka-001"
        assert plugin._metrics_source is not None
        assert plugin._handler is not None

    def test_initialize_invalid_config_raises(self):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        with pytest.raises(ValidationError):
            plugin.initialize({})  # Missing required fields

    def test_creates_metrics_source(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert isinstance(plugin._metrics_source, CanonicalizingMetricsSource)

    def test_creates_metrics_source_with_basic_auth(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource

        base_settings["metrics"] = {
            "url": "http://prom:9090",
            "auth_type": "basic",
            "username": "user",
            "password": "pass",
        }
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert isinstance(plugin._metrics_source, CanonicalizingMetricsSource)

    def test_no_admin_client_for_prometheus_source(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert plugin._admin_client is None

    def test_admin_api_branch_creates_admin_client(self, base_settings):
        """initialize() creates KafkaAdminClient when resource_source.source='admin_api'."""
        from unittest.mock import MagicMock, patch

        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["resource_source"] = {
            "source": "admin_api",
            "bootstrap_servers": "kafka:9092",
        }

        mock_admin = MagicMock()
        plugin = SelfManagedKafkaPlugin()

        with patch(
            "plugins.self_managed_kafka.gathering.admin_api.create_admin_client",
            return_value=mock_admin,
        ) as mock_factory:
            plugin.initialize(base_settings)

        mock_factory.assert_called_once()
        assert plugin._admin_client is mock_admin
        # Handler should receive the admin client
        handler = plugin.get_service_handlers()["kafka"]
        assert handler._admin_client is mock_admin


class TestPluginGetServiceHandlers:
    def test_returns_single_kafka_handler(self, base_settings):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        handlers = plugin.get_service_handlers()
        assert len(handlers) == 1
        assert "kafka" in handlers
        assert isinstance(handlers["kafka"], SelfManagedKafkaHandler)

    def test_raises_before_initialize(self):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.get_service_handlers()


class TestPluginGetCostInput:
    def test_returns_constructed_cost_input(self, base_settings):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        cost_input = plugin.get_cost_input()
        assert isinstance(cost_input, ConstructedCostInput)

    def test_raises_before_initialize(self):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.get_cost_input()


class TestPluginTopicAttributionProvider:
    def test_enabled_configuration_constructs_provider_and_exposes_overlay_config(
        self, base_settings: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["topic_attribution"] = {
            "enabled": True,
            "compute_policy": "shared_even_v1",
            "exclude_topic_patterns": ["__consumer_offsets"],
        }
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        config = plugin.get_overlay_config("topic_attribution")
        provider = plugin.get_topic_attribution_provider()

        assert config is not None
        assert config.enabled is True
        assert config.compute_policy == "shared_even_v1"
        assert config.exclude_topic_patterns == ["__consumer_offsets"]
        assert provider is not None
        assert provider.supported_product_types == frozenset(
            {
                "SELF_KAFKA_COMPUTE",
                "SELF_KAFKA_STORAGE",
                "SELF_KAFKA_NETWORK_INGRESS",
                "SELF_KAFKA_NETWORK_EGRESS",
            }
        )

    def test_disabled_configuration_does_not_create_provider(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert plugin.get_overlay_config("topic_attribution").enabled is False
        assert plugin.get_topic_attribution_provider() is None

    def test_disabled_overlay_preserves_legacy_discovery_query(self, base_settings: dict[str, object]) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)
        source = _MetricsSourceFake()
        plugin._metrics_source = source

        plugin.build_shared_context("tenant-1")

        queries = source.query_calls[0][0]
        assert [query.key for query in queries] == ["broker_topic_discovery"]


class TestInvalidCostRateDiagnostics:
    @pytest.mark.parametrize(
        ("selector_label", "field_path", "value", "category", "reason"),
        [
            (None, "compute_hourly_rate", "-0.125", "compute", "negative"),
            ("deployment", "network_egress_per_gib", "Infinity", "network_egress", "non_finite"),
            (
                None,
                "region_overrides.us-west-2.storage_per_gib_hourly",
                "-0.125",
                "storage",
                "negative",
            ),
            (
                "deployment",
                "region_overrides.us-west-2.network_ingress_per_gib",
                "NaN",
                "network_ingress",
                "non_finite",
            ),
        ],
    )
    @pytest.mark.parametrize("entrypoint", ("validate_plugin_settings", "initialize"))
    def test_plugin_startup_entrypoints_report_sanitized_invalid_rate_details(
        self,
        base_settings: dict[str, object],
        selector_label: str | None,
        field_path: str,
        value: str,
        category: str,
        reason: str,
        entrypoint: str,
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        if selector_label is not None:
            base_settings["metrics_identifier_label"] = selector_label
        if field_path.startswith("region_overrides"):
            base_settings["cost_model"]["region_overrides"] = {
                "us-west-2": {field_path.rsplit(".", maxsplit=1)[1]: value}
            }
        else:
            base_settings["cost_model"][field_path] = value

        with pytest.raises(ValueError) as error:
            getattr(SelfManagedKafkaPlugin(), entrypoint)(base_settings)

        detail = str(error.value)
        import traceback

        traceback_text = "".join(traceback.format_exception(error.value))
        expected_selector = f"{selector_label or 'kafka_cluster_id'}=kraft-a-001"
        expected_field = f"cost_model.{field_path}"
        assert "invalid_self_managed_cost_rate" in detail
        assert "cluster=kafka-001" in detail
        assert f"selector={expected_selector}" in detail
        assert f"field={expected_field}" in detail
        assert f"category={category}" in detail
        assert f"reason={reason}" in detail
        assert "date=" not in detail
        assert value not in detail
        assert "http://prom:9090" not in detail
        assert error.value.__cause__ is None
        assert error.value.__suppress_context__ is True
        assert "ValidationError" not in traceback_text
        assert "input_value" not in traceback_text
        assert "errors.pydantic.dev" not in traceback_text


class TestPluginTopicAttributionProviderState:
    def test_new_pipeline_cycle_discards_stale_admin_partitionless_proof(
        self, base_settings: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["resource_source"] = {
            "source": "admin_api",
            "bootstrap_servers": "kafka:9092",
        }
        with patch("plugins.self_managed_kafka.gathering.admin_api.create_admin_client"):
            plugin = SelfManagedKafkaPlugin()
            plugin.initialize(base_settings)

        assert plugin._handler is not None
        plugin._handler._admin_inventory_complete = True
        plugin._handler._admin_inventory_is_partitionless = True

        plugin.reset_topic_attribution_inventory_proof()

        assert plugin._handler.admin_inventory_complete is False
        assert plugin._handler.admin_inventory_is_partitionless is False


class TestPluginGetMetricsSource:
    def test_returns_metrics_source_after_initialize(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        source = plugin.get_metrics_source()
        assert isinstance(source, CanonicalizingMetricsSource)

    def test_returns_none_before_initialize(self):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        assert plugin.get_metrics_source() is None


class TestPluginConformsToProtocol:
    def test_conforms_to_ecosystem_plugin_protocol(self, base_settings):
        from core.plugin.protocols import EcosystemPlugin
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        assert isinstance(plugin, EcosystemPlugin)


class TestPluginClose:
    def test_close_with_no_admin_client(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)
        # Should not raise even without admin client
        plugin.close()

    def test_close_clears_admin_client(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["resource_source"] = {
            "source": "admin_api",
            "bootstrap_servers": "kafka:9092",
        }

        mock_admin = MagicMock()
        plugin = SelfManagedKafkaPlugin()
        # Manually inject mock admin to avoid real kafka connection
        plugin._admin_client = mock_admin

        plugin.close()

        mock_admin.close.assert_called_once()
        assert plugin._admin_client is None

    def test_close_logs_single_warning_and_continues_cleanup_when_admin_close_fails(
        self,
        base_settings,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)
        failing_admin = MagicMock()
        failing_admin.close.side_effect = OSError("secret")
        metrics_source = MagicMock()
        plugin._admin_client = failing_admin
        plugin._metrics_source = metrics_source

        with caplog.at_level(logging.WARNING, logger="plugins.self_managed_kafka.plugin"):
            plugin.close()

        assert plugin._admin_client is None
        assert plugin._metrics_source is None
        metrics_source.close.assert_called_once()
        warning_records = [record for record in caplog.records if record.levelname == "WARNING"]
        assert len(warning_records) == 1
        assert "plugin_close" in warning_records[0].getMessage()
        assert "error_type=OSError" in warning_records[0].getMessage()
        assert "secret" not in caplog.text


class TestPluginInjectsDependencies:
    def test_metrics_source_injected_into_handler(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        handler = plugin.get_service_handlers()["kafka"]
        assert handler._metrics_source is plugin._metrics_source

    def test_metrics_source_injected_into_cost_input(self, base_settings):
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        cost_input = plugin.get_cost_input()
        assert cost_input._metrics_source is plugin._metrics_source

    def test_initialize_builds_one_catalog_and_injects_the_same_canonicalizing_source_into_every_consumer(
        self, base_settings: dict[str, object]
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource

        base_settings["topic_attribution"] = {"enabled": True, "compute_policy": "shared_even_v1"}
        base_settings["metric_name_overrides"] = {"kafka_log_log_size": "company_log_size"}
        raw_source = _MetricsSourceFake()
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=raw_source):
            plugin.initialize(base_settings)

        handler = plugin.get_service_handlers()["kafka"]
        cost_input = plugin.get_cost_input()
        topic_provider = plugin.get_topic_attribution_provider()
        shared_source = plugin.get_metrics_source()
        shared_catalog = plugin._telemetry_catalog

        assert isinstance(shared_source, CanonicalizingMetricsSource)
        assert handler._metrics_source is shared_source
        assert cost_input._metrics_source is shared_source
        assert topic_provider._metrics_source is shared_source
        assert handler._telemetry_catalog is shared_catalog
        assert cost_input._telemetry_catalog is shared_catalog
        assert topic_provider._telemetry_catalog is shared_catalog


class TestPluginGetFallbackAllocator:
    """Tests for get_fallback_allocator() — GAP-074."""

    def test_get_fallback_allocator_returns_none(self) -> None:
        """SelfManagedKafkaPlugin.get_fallback_allocator() returns None."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        assert plugin.get_fallback_allocator() is None


class TestPluginTelemetryWiring:
    def test_initialize_wires_configured_selector_without_startup_principal_inference(
        self, base_settings: dict
    ) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = _MetricsSourceFake()
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(base_settings)

        assert plugin.get_service_handlers()["kafka"]._config.metrics_identifier == "kraft-a-001"
        assert plugin.get_cost_input()._config.metrics_identifier_label == "kafka_cluster_id"
        assert source.query_calls == []

    def test_initialize_rejects_missing_metrics_identifier(self, base_settings: dict) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        del base_settings["metrics_identifier"]

        with pytest.raises(ValidationError) as error:
            SelfManagedKafkaPlugin().initialize(base_settings)

        assert error.value.errors()[0]["loc"] == ("metrics_identifier",)

    def test_plugin_implements_scope_gate_protocol(self, base_settings: dict) -> None:
        from core.plugin.protocols import ScopeGatePlugin
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        with patch(
            "plugins.self_managed_kafka.plugin.create_metrics_source",
            return_value=_MetricsSourceFake(),
        ):
            plugin.initialize(base_settings)

        assert isinstance(plugin, ScopeGatePlugin)
