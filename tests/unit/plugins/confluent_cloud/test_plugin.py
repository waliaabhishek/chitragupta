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


def test_plugin_get_service_handlers_empty():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    handlers = plugin.get_service_handlers()
    assert handlers == {}  # Stub returns empty dict


def test_plugin_get_cost_input_not_implemented():
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

    with pytest.raises(NotImplementedError):
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


def test_plugin_get_metrics_source_returns_none():
    """GAP-015+017: plugin owns metrics source; stub returns None."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    assert plugin.get_metrics_source() is None
