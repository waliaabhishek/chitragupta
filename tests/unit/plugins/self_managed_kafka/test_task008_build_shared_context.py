"""TASK-008: SelfManagedKafkaPlugin.build_shared_context() tests.

Verifies that build_shared_context() returns an SMKSharedContext with the
cluster resource constructed from plugin config.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def base_settings() -> dict:
    return {
        "cluster_id": "kafka-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "metrics": {"url": "http://prom:9090"},
    }


class TestSMKSharedContext:
    """Unit tests for the SMKSharedContext dataclass."""

    def test_has_cluster_resource_field(self) -> None:
        """SMKSharedContext has a cluster_resource field."""
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        mock_resource = MagicMock()
        ctx = SMKSharedContext(cluster_resource=mock_resource)
        assert ctx.cluster_resource is mock_resource

    def test_is_frozen(self) -> None:
        """SMKSharedContext is frozen — field reassignment raises AttributeError."""
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        mock_resource = MagicMock()
        ctx = SMKSharedContext(cluster_resource=mock_resource)
        with pytest.raises(AttributeError):
            ctx.cluster_resource = MagicMock()  # type: ignore[misc]


class TestSelfManagedKafkaPluginBuildSharedContext:
    """build_shared_context() returns SMKSharedContext with cluster_resource from config."""

    def test_returns_smk_shared_context_type(self, base_settings: dict) -> None:
        """build_shared_context returns an SMKSharedContext instance."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        result = plugin.build_shared_context("tenant-1")
        assert isinstance(result, SMKSharedContext)

    def test_cluster_resource_has_correct_cluster_id(self, base_settings: dict) -> None:
        """cluster_resource.resource_id matches config.cluster_id."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        result = plugin.build_shared_context("tenant-1")

        assert isinstance(result, SMKSharedContext)
        assert result.cluster_resource.resource_id == "kafka-001"

    def test_cluster_resource_type_is_cluster(self, base_settings: dict) -> None:
        """cluster_resource.resource_type is 'cluster'."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        result = plugin.build_shared_context("tenant-1")

        assert isinstance(result, SMKSharedContext)
        assert result.cluster_resource.resource_type == "cluster"

    def test_raises_when_not_initialized(self) -> None:
        """build_shared_context raises RuntimeError when called before initialize()."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()

        with pytest.raises(RuntimeError, match="not initialized"):
            plugin.build_shared_context("tenant-1")

    def test_conforms_to_ecosystem_plugin_protocol(self, base_settings: dict) -> None:
        """SelfManagedKafkaPlugin still conforms to EcosystemPlugin after adding build_shared_context."""
        from core.plugin.protocols import EcosystemPlugin
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        assert isinstance(plugin, EcosystemPlugin)
