"""Tests for SelfManagedKafkaHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from core.models import CoreResource, MetricRow, ResourceStatus

if TYPE_CHECKING:
    from plugins.self_managed_kafka.shared_context import SMKSharedContext


def _make_smk_ctx(cluster_id: str = "kafka-001") -> SMKSharedContext:
    """Create an SMKSharedContext with a cluster resource matching cluster_id."""
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

    cluster = CoreResource(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        resource_id=cluster_id,
        resource_type="cluster",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )
    return SMKSharedContext(cluster_resource=cluster)


@pytest.fixture
def base_config():
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.from_plugin_settings(
        {
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
    )


@pytest.fixture
def static_config():
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "kafka-001",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "identity_source": {
                "source": "static",
                "static_identities": [
                    {"identity_id": "team-data", "identity_type": "team"},
                ],
            },
            "metrics": {"url": "http://prom:9090"},
        }
    )


@pytest.fixture
def mock_metrics_source():
    return MagicMock()


def make_metric_row(key: str, principal: str, value: float) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels={"principal": principal},
    )


class TestHandlerProperties:
    def test_service_type(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.service_type == "kafka"

    def test_handles_product_types(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        pts = handler.handles_product_types
        assert "SELF_KAFKA_COMPUTE" in pts
        assert "SELF_KAFKA_STORAGE" in pts
        assert "SELF_KAFKA_NETWORK_INGRESS" in pts
        assert "SELF_KAFKA_NETWORK_EGRESS" in pts
        assert len(pts) == 4


class TestGatherResources:
    def test_always_yields_cluster_first(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        mock_metrics_source.query.return_value = {"distinct_brokers": [], "distinct_topics": []}
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, _make_smk_ctx("kafka-001")))
        cluster_resource = resources[0]
        assert cluster_resource.resource_id == "kafka-001"
        assert cluster_resource.resource_type == "cluster"

    def test_prometheus_source_queries_metrics(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        ctx = SMKSharedContext(
            cluster_resource=_make_smk_ctx("kafka-001").cluster_resource,
            discovered_brokers=frozenset({"0"}),
            discovered_topics=frozenset(),
        )
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))
        resource_types = [r.resource_type for r in resources]
        assert "cluster" in resource_types
        assert "broker" in resource_types
        mock_metrics_source.query.assert_not_called()

    def test_admin_api_source_uses_admin_client(self, mock_metrics_source):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "resource_source": {
                    "source": "admin_api",
                    "bootstrap_servers": "kafka:9092",
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )
        mock_admin = MagicMock()
        mock_admin.describe_cluster.return_value = {"brokers": [{"node_id": 0, "host": "kafka-1", "port": 9092}]}
        mock_admin.list_topics.return_value = ["orders"]

        handler = SelfManagedKafkaHandler(config, mock_metrics_source, admin_client=mock_admin)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, _make_smk_ctx("kafka-001")))
        resource_types = {r.resource_type for r in resources}
        assert "cluster" in resource_types
        assert "broker" in resource_types
        assert "topic" in resource_types
        # Should NOT query Prometheus for resources when admin_api configured
        mock_metrics_source.query.assert_not_called()

    def test_admin_api_source_with_none_client_yields_only_cluster(self, mock_metrics_source):
        """_gather_resources_from_admin returns early when admin_client is None."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "resource_source": {
                    "source": "admin_api",
                    "bootstrap_servers": "kafka:9092",
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        # admin_client=None: guard should prevent any Admin API calls
        handler = SelfManagedKafkaHandler(config, mock_metrics_source, admin_client=None)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, _make_smk_ctx("kafka-001")))
        resource_types = {r.resource_type for r in resources}
        # Only cluster is yielded; brokers/topics skipped due to early return
        assert resource_types == {"cluster"}


class TestGatherIdentities:
    def test_prometheus_source_queries_metrics(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        ctx = SMKSharedContext(
            cluster_resource=_make_smk_ctx("kafka-001").cluster_resource,
            discovered_brokers=frozenset(),
            discovered_topics=frozenset(),
            discovered_principals=frozenset({"User:alice"}),
        )
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        # Populate _current_gather_ctx via gather_resources first
        list(handler.gather_resources("tenant-1", uow, ctx))

        identities = list(handler.gather_identities("tenant-1", uow))
        assert len(identities) == 1
        assert identities[0].identity_id == "User:alice"
        mock_metrics_source.query.assert_not_called()

    def test_static_source_loads_from_config(self, static_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(static_config, mock_metrics_source)
        uow = MagicMock()

        identities = list(handler.gather_identities("tenant-1", uow))
        assert len(identities) == 1
        assert identities[0].identity_id == "team-data"
        # Should NOT query Prometheus for identities
        mock_metrics_source.query.assert_not_called()

    def test_both_source_combines_prometheus_and_static(self, mock_metrics_source):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {
                    "source": "both",
                    "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        ctx = SMKSharedContext(
            cluster_resource=_make_smk_ctx("kafka-001").cluster_resource,
            discovered_brokers=frozenset(),
            discovered_topics=frozenset(),
            discovered_principals=frozenset({"User:alice"}),
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        uow = MagicMock()

        # Populate _current_gather_ctx via gather_resources first
        list(handler.gather_resources("tenant-1", uow, ctx))

        identities = list(handler.gather_identities("tenant-1", uow))
        ids = {i.identity_id for i in identities}
        assert "User:alice" in ids
        assert "team-data" in ids


class TestResolveIdentities:
    def test_prometheus_source_extracts_principals_from_metrics_data(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        metrics_data = {
            "bytes_in_per_principal": [make_metric_row("bytes_in_per_principal", "User:alice", 1000.0)],
        }
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), metrics_data, uow
        )

        assert "User:alice" in resolution.metrics_derived

    def test_no_metrics_data_returns_empty_resolution(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), None, uow
        )

        assert len(resolution.metrics_derived) == 0
        assert len(resolution.resource_active) == 0

    def test_static_source_populates_resource_active(self, static_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(static_config, mock_metrics_source)
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), None, uow
        )

        assert "team-data" in resolution.resource_active

    def test_both_source_populates_both_sets(self, mock_metrics_source):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {
                    "source": "both",
                    "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        metrics_data = {"bytes_in_per_principal": [make_metric_row("bytes_in_per_principal", "User:alice", 1000.0)]}
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), metrics_data, uow
        )

        assert "team-data" in resolution.resource_active
        assert "User:alice" in resolution.metrics_derived

    def test_prometheus_unavailable_falls_back_to_static_in_resolve(self, mock_metrics_source):
        """When prometheus_principals_available=False and static_identities configured,
        resolve_identities() uses static identities instead of metrics_data."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {
                    "source": "prometheus",
                    "static_identities": [{"identity_id": "User:alice", "identity_type": "principal"}],
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source, prometheus_principals_available=False)
        metrics_data = {"bytes_in_per_principal": [make_metric_row("bytes_in_per_principal", "User:bob", 1000.0)]}
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), metrics_data, uow
        )

        # Static fallback: alice from static_identities
        assert "User:alice" in resolution.resource_active
        # Prometheus path skipped: bob (from metrics_data) not in metrics_derived
        assert "User:bob" not in resolution.metrics_derived


class TestGetMetricsForProductType:
    def test_prometheus_source_returns_principal_metrics(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)

        for pt in (
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        ):
            metrics = handler.get_metrics_for_product_type(pt)
            assert len(metrics) == 2
            metric_keys = {m.key for m in metrics}
            assert "bytes_in_per_principal" in metric_keys
            assert "bytes_out_per_principal" in metric_keys

    def test_static_source_returns_empty_metrics(self, static_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(static_config, mock_metrics_source)

        for pt in (
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        ):
            metrics = handler.get_metrics_for_product_type(pt)
            assert metrics == []

    def test_unknown_product_type_returns_empty(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.get_metrics_for_product_type("UNKNOWN") == []


class TestGetAllocator:
    def test_compute_allocator(self, base_config, mock_metrics_source):
        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.get_allocator("SELF_KAFKA_COMPUTE") is allocate_evenly_with_fallback

    def test_storage_allocator(self, base_config, mock_metrics_source):
        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.get_allocator("SELF_KAFKA_STORAGE") is allocate_evenly_with_fallback

    def test_network_ingress_allocator(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.get_allocator("SELF_KAFKA_NETWORK_INGRESS") is SMK_INGRESS_MODEL

    def test_network_egress_allocator(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        assert handler.get_allocator("SELF_KAFKA_NETWORK_EGRESS") is SMK_EGRESS_MODEL

    def test_unknown_product_type_raises(self, base_config, mock_metrics_source):
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")
