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
    def test_initialize_sets_ecosystem_from_config(self) -> None:
        """Test case 4: plugin.ecosystem == config["ecosystem_name"] after initialize."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        assert plugin.ecosystem == "self_managed_postgres"

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

    def test_cluster_resource_ecosystem_matches_ecosystem_name(self) -> None:
        """Test case 6: cluster_resource.ecosystem == config["ecosystem_name"]."""
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        plugin.initialize(PG_SETTINGS)
        ctx = plugin.build_shared_context("tenant-1")
        assert ctx.cluster_resource.ecosystem == "self_managed_postgres"

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
        """Test case 22: SMK YAML config parses without error."""
        from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

        config = GenericMetricsOnlyConfig.model_validate(KAFKA_YAML_SETTINGS)
        assert config.ecosystem_name == "self_managed_kafka"
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
