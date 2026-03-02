"""Tests for SelfManagedKafkaPlugin."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError


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
        from core.metrics.prometheus import PrometheusMetricsSource
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert isinstance(plugin._metrics_source, PrometheusMetricsSource)

    def test_creates_metrics_source_with_basic_auth(self, base_settings):
        from core.metrics.prometheus import PrometheusMetricsSource
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["metrics"] = {
            "url": "http://prom:9090",
            "auth_type": "basic",
            "username": "user",
            "password": "pass",
        }
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        assert isinstance(plugin._metrics_source, PrometheusMetricsSource)

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


class TestPluginGetMetricsSource:
    def test_returns_metrics_source_after_initialize(self, base_settings):
        from core.metrics.prometheus import PrometheusMetricsSource
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(base_settings)

        source = plugin.get_metrics_source()
        assert isinstance(source, PrometheusMetricsSource)

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


class TestPluginPrincipalLabelValidation:
    """Issue 3: startup validation of 'principal' label availability in Prometheus."""

    def test_initialize_validates_principal_label_available(self, base_settings):
        """Prometheus returns rows with 'principal' label → _prometheus_principals_available = True."""
        from unittest.mock import MagicMock, patch

        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["identity_source"] = {"source": "prometheus"}

        mock_row = MagicMock()
        mock_row.labels = {"principal": "User:alice"}

        plugin = SelfManagedKafkaPlugin()
        with patch.object(plugin, "_create_metrics_source") as mock_create:
            mock_metrics = MagicMock()
            mock_metrics.query.return_value = {"distinct_principals": [mock_row]}
            mock_create.return_value = mock_metrics

            plugin.initialize(base_settings)

        assert plugin._prometheus_principals_available is True

    def test_initialize_warns_when_principal_label_missing(self, base_settings):
        """Prometheus returns rows without 'principal' label → WARNING logged, flag = False."""
        from unittest.mock import MagicMock, patch

        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["identity_source"] = {"source": "prometheus"}

        mock_row = MagicMock()
        mock_row.labels = {}  # No 'principal' label

        plugin = SelfManagedKafkaPlugin()
        with patch.object(plugin, "_create_metrics_source") as mock_create:
            mock_metrics = MagicMock()
            mock_metrics.query.return_value = {"distinct_principals": [mock_row]}
            mock_create.return_value = mock_metrics

            with patch("plugins.self_managed_kafka.plugin.LOGGER") as mock_logger:
                plugin.initialize(base_settings)
                mock_logger.warning.assert_called_once()
                warning_msg = mock_logger.warning.call_args[0][0]
                assert "principal" in warning_msg.lower()

        assert plugin._prometheus_principals_available is False

    def test_initialize_handles_prometheus_unreachable(self, base_settings):
        """MetricsQueryError during validation → WARNING logged, plugin continues."""
        from unittest.mock import MagicMock, patch

        from core.metrics.protocol import MetricsQueryError
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["identity_source"] = {"source": "prometheus"}

        plugin = SelfManagedKafkaPlugin()
        with patch.object(plugin, "_create_metrics_source") as mock_create:
            mock_metrics = MagicMock()
            mock_metrics.query.side_effect = MetricsQueryError("connection refused")
            mock_create.return_value = mock_metrics

            with patch("plugins.self_managed_kafka.plugin.LOGGER") as mock_logger:
                # Must not raise — plugin continues gracefully
                plugin.initialize(base_settings)
                mock_logger.warning.assert_called_once()

        assert plugin._prometheus_principals_available is False
        assert plugin._handler is not None

    def test_gather_identities_static_fallback_when_prometheus_unavailable(self, base_settings):
        """When principal label unavailable and static_identities configured → static identities returned."""
        from unittest.mock import MagicMock, patch

        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        base_settings["identity_source"] = {
            "source": "prometheus",
            "static_identities": [
                {"identity_id": "User:alice", "identity_type": "principal", "display_name": "Alice"},
            ],
        }

        plugin = SelfManagedKafkaPlugin()
        with patch.object(plugin, "_create_metrics_source") as mock_create:
            mock_metrics = MagicMock()
            # No principal labels → _prometheus_principals_available = False
            mock_metrics.query.return_value = {"distinct_principals": []}
            mock_create.return_value = mock_metrics

            plugin.initialize(base_settings)

        assert plugin._prometheus_principals_available is False

        handler = plugin.get_service_handlers()["kafka"]
        mock_uow = MagicMock()
        identities = list(handler.gather_identities("tenant-1", mock_uow))

        identity_ids = [i.identity_id for i in identities]
        assert "User:alice" in identity_ids
