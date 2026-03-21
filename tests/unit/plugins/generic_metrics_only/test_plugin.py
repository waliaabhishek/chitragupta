"""Tests for GenericMetricsOnlyPlugin and register()."""

from __future__ import annotations

import pytest

KAFKA_YAML_SETTINGS = {
    "ecosystem_name": "self_managed_kafka",
    "cluster_id": "kafka-prod",
    "display_name": "Production Kafka Cluster",
    "metrics": {"url": "http://prometheus:9090"},
    "identity_source": {
        "source": "prometheus",
        "label": "principal",
        "discovery_query": "group by (principal) (kafka_server_brokertopicmetrics_bytesin_total)",
        "default_team": "UNASSIGNED",
    },
    "cost_types": [
        {
            "name": "SELF_KAFKA_COMPUTE",
            "product_category": "kafka",
            "rate": "1.20",
            "cost_quantity": {"type": "fixed", "count": 3},
            "allocation_strategy": "even_split",
        },
        {
            "name": "SELF_KAFKA_STORAGE",
            "product_category": "kafka",
            "rate": "0.00002",
            "cost_quantity": {
                "type": "storage_gib",
                "query": "sum(kafka_log_log_size)",
            },
            "allocation_strategy": "even_split",
        },
        {
            "name": "SELF_KAFKA_NETWORK_INGRESS",
            "product_category": "kafka",
            "rate": "0.05",
            "cost_quantity": {
                "type": "network_gib",
                "query": "sum(increase(kafka_server_brokertopicmetrics_bytesin_total[1h]))",
            },
            "allocation_strategy": "usage_ratio",
            "allocation_query": "sum by (principal) (increase(kafka_server_brokertopicmetrics_bytesin_total[1h]))",
            "allocation_label": "principal",
        },
        {
            "name": "SELF_KAFKA_NETWORK_EGRESS",
            "product_category": "kafka",
            "rate": "0.05",
            "cost_quantity": {
                "type": "network_gib",
                "query": "sum(increase(kafka_server_brokertopicmetrics_bytesout_total[1h]))",
            },
            "allocation_strategy": "usage_ratio",
            "allocation_query": "sum by (principal) (increase(kafka_server_brokertopicmetrics_bytesout_total[1h]))",
            "allocation_label": "principal",
        },
    ],
}

PG_SETTINGS = {
    "ecosystem_name": "self_managed_postgres",
    "cluster_id": "pg-prod-1",
    "metrics": {"url": "http://prom:9090"},
    "identity_source": {
        "source": "prometheus",
        "label": "datname",
        "discovery_query": "group by (datname) (pg_stat_database_blks_hit)",
    },
    "cost_types": [
        {
            "name": "PG_COMPUTE",
            "product_category": "postgres",
            "rate": "2.50",
            "cost_quantity": {"type": "fixed", "count": 2},
            "allocation_strategy": "even_split",
        }
    ],
}


class TestPluginInitialize:
    def test_initialize_sets_ecosystem_to_generic_metrics_only(self) -> None:
        """Verification 4: plugin.ecosystem == "generic_metrics_only" after initialize (hardcoded)."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        assert plugin.ecosystem == "generic_metrics_only"

    def test_ecosystem_before_initialize_is_sentinel(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        assert plugin.ecosystem == "generic_metrics_only"


class TestBuildSharedContext:
    def test_cluster_resource_id_matches_config(self) -> None:
        """Test case 5: cluster_resource.resource_id == config["cluster_id"]."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        ctx = plugin.build_shared_context("tenant-1")
        assert ctx.cluster_resource.resource_id == "pg-prod-1"

    def test_cluster_resource_ecosystem_is_generic_metrics_only(self) -> None:
        """Verification 5: cluster_resource.ecosystem == "generic_metrics_only" (hardcoded)."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        ctx = plugin.build_shared_context("tenant-1")
        assert ctx.cluster_resource.ecosystem == "generic_metrics_only"

    def test_cluster_resource_has_correct_tenant_id(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        ctx = plugin.build_shared_context("tenant-xyz")
        assert ctx.cluster_resource.tenant_id == "tenant-xyz"

    def test_build_shared_context_before_initialize_raises(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.build_shared_context("tenant-1")


class TestRegister:
    def test_register_returns_tuple_with_correct_key(self) -> None:
        """Test case 21: register() returns ("generic_metrics_only", GenericMetricsOnlyPlugin)."""
        from plugins.generic_metrics_only import register
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        key, factory = register()
        assert key == "generic_metrics_only"
        assert factory is GenericMetricsOnlyPlugin

    def test_register_factory_is_callable(self) -> None:
        from plugins.generic_metrics_only import register

        _, factory = register()
        assert callable(factory)


class TestSelfManagedKafkaAsGenericYaml:
    def test_kafka_yaml_config_parses_without_error(self) -> None:
        """Test case 22: SMK YAML config parses without error (ecosystem_name silently ignored)."""
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        config = GenericMetricsOnlyConfig.model_validate(KAFKA_YAML_SETTINGS)
        assert config.cluster_id == "kafka-prod"

    def test_kafka_yaml_config_produces_handler_with_correct_product_types(self) -> None:
        """Test case 22: handler.handles_product_types == [...four Kafka types...]."""
        from unittest.mock import MagicMock

        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        config = GenericMetricsOnlyConfig.model_validate(KAFKA_YAML_SETTINGS)
        mock_metrics = MagicMock()
        handler = GenericMetricsOnlyHandler(config=config, metrics_source=mock_metrics)

        assert list(handler.handles_product_types) == [
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        ]


class TestPluginGetFallbackAllocator:
    """Tests for get_fallback_allocator() — GAP-074."""

    def test_get_fallback_allocator_returns_none(self) -> None:
        """GenericMetricsOnlyPlugin.get_fallback_allocator() returns None."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        assert plugin.get_fallback_allocator() is None


class TestPluginMethods:
    """Tests for get_service_handlers, get_cost_input, get_metrics_source, close, validate_plugin_settings."""

    def test_get_service_handlers_returns_generic_handler(self) -> None:
        from unittest.mock import patch

        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source"):
            plugin.initialize(PG_SETTINGS)

        handlers = plugin.get_service_handlers()
        assert "generic" in handlers

    def test_get_service_handlers_before_initialize_raises(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.get_service_handlers()

    def test_get_cost_input_returns_cost_input_instance(self) -> None:
        from unittest.mock import patch

        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source"):
            plugin.initialize(PG_SETTINGS)

        cost_input = plugin.get_cost_input()
        assert isinstance(cost_input, GenericConstructedCostInput)

    def test_get_cost_input_before_initialize_raises(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.get_cost_input()

    def test_get_metrics_source_returns_none_before_initialize(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        assert plugin.get_metrics_source() is None

    def test_get_metrics_source_returns_source_after_initialize(self) -> None:
        from unittest.mock import MagicMock, patch

        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        mock_source = MagicMock()
        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source", return_value=mock_source):
            plugin.initialize(PG_SETTINGS)

        assert plugin.get_metrics_source() is mock_source

    def test_close_clears_metrics_source(self) -> None:
        from unittest.mock import MagicMock, patch

        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        mock_source = MagicMock()
        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source", return_value=mock_source):
            plugin.initialize(PG_SETTINGS)

        plugin.close()
        mock_source.close.assert_called_once()
        assert plugin._metrics_source is None

    def test_close_before_initialize_is_noop(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.close()  # should not raise

    def test_validate_plugin_settings_accepts_valid_config(self) -> None:
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.validate_plugin_settings(PG_SETTINGS)  # should not raise

    def test_get_storage_module_returns_module(self) -> None:
        from unittest.mock import patch

        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin
        from plugins.generic_metrics_only.storage.module import GenericMetricsOnlyStorageModule

        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source"):
            plugin.initialize(PG_SETTINGS)

        assert isinstance(plugin.get_storage_module(), GenericMetricsOnlyStorageModule)


class TestPluginIntegrationEcosystemLabel:
    """Integration: full data flow from initialize() to billing line ecosystem value."""

    def test_gather_billing_lines_have_ecosystem_generic_metrics_only(self) -> None:
        """Verification GIT-002: ecosystem in billing lines is always "generic_metrics_only"."""
        from datetime import UTC, datetime
        from unittest.mock import MagicMock, patch

        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        mock_source = MagicMock()
        mock_source.query.return_value = {}  # no Prometheus data — fixed cost type skips query

        plugin = GenericMetricsOnlyPlugin()
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source", return_value=mock_source):
            plugin.initialize(PG_SETTINGS)

        cost_input = plugin.get_cost_input()
        mock_uow = MagicMock()
        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 2, tzinfo=UTC)

        items = list(cost_input.gather("tenant-1", start, end, mock_uow))

        assert len(items) >= 1
        assert all(item.ecosystem == "generic_metrics_only" for item in items)
