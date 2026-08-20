"""Tests for broker and topic discovery lifecycle."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from core.models import MetricRow


def _settings() -> dict[str, object]:
    return {
        "cluster_id": "billing-cluster-a",
        "metrics_identifier": "kraft-a-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": {"source": "static"},
        "resource_source": {"source": "prometheus"},
        "metrics": {"url": "http://prometheus:9090"},
    }


def test_resource_discovery_is_not_an_initialization_time_principal_readiness_check() -> None:
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source = MagicMock()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(_settings())

    source.query.assert_not_called()
    assert not hasattr(plugin, "_prometheus_principals_available")
    assert not hasattr(plugin, "_cached_discovery")


def test_resource_discovery_is_scoped_and_collects_only_brokers_and_topics() -> None:
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source = MagicMock()

    def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
        return {
            queries[0].key: [
                MetricRow(
                    timestamp=datetime(2026, 8, 1, tzinfo=UTC),
                    metric_key=queries[0].key,
                    value=1.0,
                    labels={"broker": "1", "topic": "orders", "kafka_cluster_id": "kraft-a-001"},
                )
            ]
        }

    source.query.side_effect = query
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(_settings())
    context = plugin.build_shared_context("tenant-1")

    assert context.discovered_brokers == frozenset({"1"})
    assert context.discovered_topics == frozenset({"orders"})
    _, kwargs = source.query.call_args
    assert kwargs["resource_id_filter"] == "kraft-a-001"
    assert kwargs["queries"][0].label_keys == ("broker", "topic")
    assert kwargs["queries"][0].resource_label == "kafka_cluster_id"
