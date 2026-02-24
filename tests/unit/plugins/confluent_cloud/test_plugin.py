from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_plugin_ecosystem_property():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    assert plugin.ecosystem == "confluent_cloud"


def test_plugin_initialize_validates_config():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
        }
    )

    assert plugin._config is not None
    assert plugin._config.ccloud_api.key == "k"


def test_plugin_initialize_invalid_config_raises():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()

    with pytest.raises(ValidationError):
        plugin.initialize({})  # Missing required ccloud_api


def test_plugin_get_service_handlers_returns_kafka_and_sr():
    """get_service_handlers returns KafkaHandler and SchemaRegistryHandler."""
    from plugins.confluent_cloud import ConfluentCloudPlugin
    from plugins.confluent_cloud.handlers.kafka import KafkaHandler
    from plugins.confluent_cloud.handlers.schema_registry import SchemaRegistryHandler

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    handlers = plugin.get_service_handlers()

    assert "kafka" in handlers
    assert "schema_registry" in handlers
    assert isinstance(handlers["kafka"], KafkaHandler)
    assert isinstance(handlers["schema_registry"], SchemaRegistryHandler)


def test_plugin_get_service_handlers_correct_order():
    """Handlers are returned in correct order (Kafka first)."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    handlers = plugin.get_service_handlers()
    handler_keys = list(handlers.keys())

    # Kafka must come before schema_registry for environment gathering
    assert handler_keys.index("kafka") < handler_keys.index("schema_registry")


def test_plugin_get_service_handlers_raises_before_initialize():
    """get_service_handlers raises if called before initialize."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()

    with pytest.raises(RuntimeError, match="not initialized"):
        plugin.get_service_handlers()


def test_plugin_get_cost_input_returns_billing_cost_input():
    """get_cost_input() returns CCloudBillingCostInput after initialization."""
    from plugins.confluent_cloud import ConfluentCloudPlugin
    from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    cost_input = plugin.get_cost_input()
    assert isinstance(cost_input, CCloudBillingCostInput)


def test_plugin_get_cost_input_raises_before_initialize():
    """get_cost_input() raises RuntimeError if called before initialize()."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()

    with pytest.raises(RuntimeError, match="not initialized"):
        plugin.get_cost_input()


def test_plugin_conforms_to_protocol():
    from core.plugin.protocols import EcosystemPlugin
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    assert isinstance(plugin, EcosystemPlugin)


def test_plugin_creates_connection():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "mykey", "secret": "mysecret"}})

    assert plugin._connection is not None
    assert plugin._connection.api_key == "mykey"


def test_plugin_get_metrics_source_returns_none_without_config():
    """Without metrics config, get_metrics_source returns None."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    source = plugin.get_metrics_source()
    assert source is None


def test_plugin_get_metrics_source_with_config():
    """With metrics config, get_metrics_source returns PrometheusMetricsSource."""
    from core.metrics.prometheus import PrometheusMetricsSource
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "metrics": {"type": "prometheus", "url": "http://prom:9090"},
        }
    )

    source = plugin.get_metrics_source()
    assert isinstance(source, PrometheusMetricsSource)


def test_plugin_get_metrics_source_with_basic_auth():
    """Metrics source works with basic auth."""
    from core.metrics.prometheus import PrometheusMetricsSource
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "metrics": {
                "type": "prometheus",
                "url": "http://prom:9090",
                "auth_type": "basic",
                "username": "user",
                "password": "pass",
            },
        }
    )

    source = plugin.get_metrics_source()
    assert isinstance(source, PrometheusMetricsSource)
