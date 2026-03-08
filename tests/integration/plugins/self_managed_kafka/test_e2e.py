"""Integration tests for the self-managed Kafka plugin."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    CoreBillingLineItem,
    CoreIdentity,
    CoreResource,
    IdentityResolution,
    IdentitySet,
    MetricRow,
    ResourceStatus,
)


def _make_smk_ctx(cluster_id: str, tenant_id: str = "tenant-1") -> object:
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

    cluster = CoreResource(
        ecosystem="self_managed_kafka",
        tenant_id=tenant_id,
        resource_id=cluster_id,
        resource_type="cluster",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )
    return SMKSharedContext(cluster_resource=cluster)


@pytest.fixture
def prometheus_settings() -> dict:
    return {
        "cluster_id": "kafka-cluster-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": {
            "source": "prometheus",
            "principal_to_team": {"User:alice": "team-data", "User:bob": "team-analytics"},
        },
        "metrics": {"url": "http://prom:9090"},
    }


@pytest.fixture
def mock_prometheus():
    return MagicMock()


def make_row(key: str, value: float, labels: dict | None = None) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels=labels or {},
    )


class TestFullPrometheusPipeline:
    def test_compute_storage_even_split_network_usage_ratio(self, prometheus_settings, mock_prometheus):
        """Full gather→resolve→allocate flow with mixed allocation strategies."""
        from core.engine.helpers import allocate_evenly_with_fallback as self_kafka_compute_allocator
        from plugins.self_managed_kafka.allocators.kafka_allocators import (
            self_kafka_network_ingress_allocator,
        )
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        config = SelfManagedKafkaConfig.from_plugin_settings(prometheus_settings)

        gb = 1073741824
        mock_prometheus.query.return_value = {
            "cluster_bytes_in": [make_row("cluster_bytes_in", gb * 10)],
            "cluster_bytes_out": [make_row("cluster_bytes_out", gb * 20)],
            "cluster_storage_bytes": [make_row("cluster_storage_bytes", gb * 50)] * 24,
        }

        cost_input = ConstructedCostInput(config, mock_prometheus)
        uow = MagicMock()
        day_start = datetime(2026, 2, 1, tzinfo=UTC)
        day_end = datetime(2026, 2, 2, tzinfo=UTC)

        billing_items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert len(billing_items) == 4

        # Verify all lines reference cluster as resource
        for item in billing_items:
            assert item.resource_id == "kafka-cluster-001"

        # Test COMPUTE allocation (even split)
        compute_line = next(i for i in billing_items if i.product_type == "SELF_KAFKA_COMPUTE")
        two_identities = IdentitySet()
        two_identities.add(CoreIdentity("self_managed_kafka", "tenant-1", "User:alice", "principal"))
        two_identities.add(CoreIdentity("self_managed_kafka", "tenant-1", "User:bob", "principal"))

        resolution = IdentityResolution(
            resource_active=two_identities,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=compute_line.timestamp,
            billing_line=compute_line,
            identities=resolution,
            split_amount=compute_line.total_cost,
            metrics_data=None,
        )
        compute_result = self_kafka_compute_allocator(ctx)
        compute_total = sum(r.amount for r in compute_result.rows)
        assert compute_total == compute_line.total_cost
        # Even split: alice and bob get equal share
        alice_compute = sum(r.amount for r in compute_result.rows if r.identity_id == "User:alice")
        bob_compute = sum(r.amount for r in compute_result.rows if r.identity_id == "User:bob")
        assert alice_compute == bob_compute

        # Test NETWORK allocation (usage ratio)
        ingress_line = next(i for i in billing_items if i.product_type == "SELF_KAFKA_NETWORK_INGRESS")
        metrics_data = {
            "bytes_in_per_principal": [
                make_row("bytes_in_per_principal", 700.0, {"principal": "User:alice"}),
                make_row("bytes_in_per_principal", 300.0, {"principal": "User:bob"}),
            ],
            "bytes_out_per_principal": [],
        }
        network_ctx = AllocationContext(
            timeslice=ingress_line.timestamp,
            billing_line=ingress_line,
            identities=resolution,
            split_amount=ingress_line.total_cost,
            metrics_data=metrics_data,
        )
        network_result = self_kafka_network_ingress_allocator(network_ctx)
        network_total = sum(r.amount for r in network_result.rows)
        assert network_total == ingress_line.total_cost
        alice_network = sum(r.amount for r in network_result.rows if r.identity_id == "User:alice")
        bob_network = sum(r.amount for r in network_result.rows if r.identity_id == "User:bob")
        assert alice_network > bob_network  # alice has 70% of bytes

    def test_cluster_resource_created_and_linked(self, prometheus_settings, mock_prometheus):
        """Cluster resource is created first and billing lines reference it."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        config = SelfManagedKafkaConfig.from_plugin_settings(prometheus_settings)
        ctx = SMKSharedContext(
            cluster_resource=_make_smk_ctx("kafka-cluster-001").cluster_resource,
            discovered_brokers=frozenset({"0"}),
            discovered_topics=frozenset(),
        )

        handler = SelfManagedKafkaHandler(config, mock_prometheus)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))
        cluster = resources[0]
        assert cluster.resource_type == "cluster"
        assert cluster.resource_id == "kafka-cluster-001"
        assert cluster.parent_id is None

        broker = next(r for r in resources if r.resource_type == "broker")
        assert broker.parent_id == "kafka-cluster-001"
        mock_prometheus.query.assert_not_called()

    def test_multi_principal_allocation_with_realistic_metrics(self, prometheus_settings, mock_prometheus):
        """Multi-principal allocation distributes costs proportionally."""
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_kafka",
            tenant_id="tenant-1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="kafka-cluster-001",
            product_category="kafka",
            product_type="SELF_KAFKA_NETWORK_INGRESS",
            quantity=Decimal("100"),
            unit_price=Decimal("0.01"),
            total_cost=Decimal("1.00"),
        )

        principals = IdentitySet()
        for p in ("User:alice", "User:bob", "User:charlie"):
            principals.add(CoreIdentity("self_managed_kafka", "tenant-1", p, "principal"))

        metrics_data = {
            "bytes_in_per_principal": [
                make_row("bytes_in_per_principal", 500.0, {"principal": "User:alice"}),
                make_row("bytes_in_per_principal", 300.0, {"principal": "User:bob"}),
                make_row("bytes_in_per_principal", 200.0, {"principal": "User:charlie"}),
            ],
            "bytes_out_per_principal": [],
        }

        resolution = IdentityResolution(
            resource_active=principals,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=resolution,
            split_amount=billing_line.total_cost,
            metrics_data=metrics_data,
        )

        result = self_kafka_network_ingress_allocator(ctx)
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("1.00")

        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        charlie_amount = sum(r.amount for r in result.rows if r.identity_id == "User:charlie")

        assert alice_amount > bob_amount > charlie_amount


class TestStaticIdentitiesFlow:
    def test_static_identities_resolve_without_prometheus(self, mock_prometheus):
        """With static identities, no Prometheus queries needed for allocation."""
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
                    "source": "static",
                    "static_identities": [
                        {"identity_id": "team-data", "identity_type": "team"},
                        {"identity_id": "team-platform", "identity_type": "team"},
                    ],
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        handler = SelfManagedKafkaHandler(config, mock_prometheus)
        uow = MagicMock()

        resolution = handler.resolve_identities(
            "tenant-1", "kafka-001", datetime(2026, 2, 1, tzinfo=UTC), timedelta(days=1), None, uow
        )

        assert "team-data" in resolution.resource_active
        assert "team-platform" in resolution.resource_active
        # No metrics needed for static-only
        assert len(resolution.metrics_derived) == 0


class TestAdminApiFlow:
    def test_admin_api_discovers_resources_without_prometheus(self):
        """Admin API discovery works without Prometheus resource queries."""
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
                "identity_source": {"source": "static"},
                "metrics": {"url": "http://prom:9090"},
            }
        )

        mock_admin = MagicMock()
        mock_admin.describe_cluster.return_value = {
            "brokers": [
                {"node_id": 0, "host": "kafka-1", "port": 9092},
                {"node_id": 1, "host": "kafka-2", "port": 9092},
            ]
        }
        mock_admin.list_topics.return_value = ["orders", "payments"]

        mock_metrics = MagicMock()
        handler = SelfManagedKafkaHandler(config, mock_metrics, admin_client=mock_admin)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, _make_smk_ctx("kafka-001")))
        types = {r.resource_type for r in resources}
        assert "cluster" in types
        assert "broker" in types
        assert "topic" in types

        # Prometheus NOT called for resources
        mock_metrics.query.assert_not_called()


class TestPluginLifecycle:
    def test_plugin_close_cleans_up_admin_client(self):
        """Plugin.close() calls AdminClient.close() and clears reference."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_admin = MagicMock()
        plugin._admin_client = mock_admin

        plugin.close()

        mock_admin.close.assert_called_once()
        assert plugin._admin_client is None

    def test_plugin_injects_same_metrics_source_into_handler_and_cost_input(self):
        """Plugin shares MetricsSource between CostInput and Handler."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        settings = {
            "cluster_id": "kafka-001",
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
        plugin = SelfManagedKafkaPlugin()
        plugin.initialize(settings)

        handler = plugin.get_service_handlers()["kafka"]
        cost_input = plugin.get_cost_input()

        assert handler._metrics_source is plugin._metrics_source
        assert cost_input._metrics_source is plugin._metrics_source


class TestConfigValidationIntegration:
    def test_full_yaml_style_config_parsed_correctly(self):
        """Full YAML-style config (as loaded from config file) is parsed correctly."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-cluster-001",
                "broker_count": 6,
                "region": "us-west-2",
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                    "region_overrides": {"us-west-2": {"compute_hourly_rate": "0.08"}},
                },
                "resource_source": {"source": "prometheus"},
                "identity_source": {
                    "source": "prometheus",
                    "principal_to_team": {
                        "User:alice": "team-data-eng",
                        "User:bob": "team-analytics",
                    },
                    "default_team": "UNASSIGNED",
                },
                "metrics": {
                    "type": "prometheus",
                    "url": "http://prometheus:9090",
                    "auth_type": "none",
                },
            }
        )

        assert config.cluster_id == "kafka-cluster-001"
        assert config.broker_count == 6
        assert config.region == "us-west-2"
        effective = config.get_effective_cost_model()
        from decimal import Decimal

        assert effective.compute_hourly_rate == Decimal("0.08")
        assert config.identity_source.principal_to_team["User:alice"] == "team-data-eng"

    def test_invalid_config_raises_validation_error(self):
        """Missing required fields raises ValidationError."""
        from pydantic import ValidationError

        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

        with pytest.raises(ValidationError):
            SelfManagedKafkaConfig.from_plugin_settings({"cluster_id": "kafka"})
