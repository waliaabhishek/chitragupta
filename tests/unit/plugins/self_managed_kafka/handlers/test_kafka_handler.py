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
            "metrics_identifier": "kraft-a-001",
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
            "metrics_identifier": "kraft-a-001",
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
            discovered_topics=frozenset({"orders"}),
        )
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))
        resource_types = [r.resource_type for r in resources]
        assert "cluster" in resource_types
        assert "broker" in resource_types
        assert "topic" in resource_types
        assert {(resource.resource_type, resource.parent_id) for resource in resources[1:]} == {
            ("broker", "kafka-001"),
            ("topic", "kafka-001"),
        }
        mock_metrics_source.query.assert_not_called()

    def test_admin_api_source_uses_admin_client(self, mock_metrics_source):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "metrics_identifier": "kraft-a-001",
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
        assert handler.admin_inventory_complete is True
        assert handler.admin_inventory_is_partitionless is False

    def test_admin_api_source_with_none_client_yields_only_cluster(self, mock_metrics_source):
        """_gather_resources_from_admin returns early when admin_client is None."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "metrics_identifier": "kraft-a-001",
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
        assert handler.admin_inventory_complete is False
        assert handler.admin_inventory_is_partitionless is False

    def test_admin_api_failure_clears_previous_partitionless_inventory_proof(
        self, base_config, mock_metrics_source
    ) -> None:
        from plugins.self_managed_kafka.config import ResourceSourceConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = base_config.model_copy(
            update={
                "resource_source": ResourceSourceConfig.model_validate(
                    {"source": "admin_api", "bootstrap_servers": "kafka:9092"}
                )
            }
        )
        mock_admin = MagicMock()
        mock_admin.describe_cluster.return_value = {"brokers": []}
        mock_admin.list_topics.return_value = []
        handler = SelfManagedKafkaHandler(config, mock_metrics_source, admin_client=mock_admin)

        list(handler.gather_resources("tenant-1", MagicMock(), _make_smk_ctx("kafka-001")))
        assert handler.admin_inventory_is_partitionless is True

        mock_admin.describe_cluster.side_effect = RuntimeError("unavailable")
        with pytest.raises(RuntimeError, match="Failed to gather brokers"):
            list(handler.gather_resources("tenant-1", MagicMock(), _make_smk_ctx("kafka-001")))

        assert handler.admin_inventory_complete is False
        assert handler.admin_inventory_is_partitionless is False


class TestHandlerIdentityResolution:
    def test_broker_topic_rows_never_create_principal_identities(
        self, base_config, mock_metrics_source: MagicMock
    ) -> None:
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        mock_metrics_source.query.return_value = {"quota_byte_rate": [], "quota_throttle_time_ms": []}
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        broker_topic_rows = {
            "topic_bytes": [
                MetricRow(
                    timestamp=datetime(2026, 8, 1, tzinfo=UTC),
                    metric_key="topic_bytes",
                    value=4096.0,
                    labels={
                        "broker": "1",
                        "topic": "orders",
                        "kafka_cluster_id": "kraft-a-001",
                        "principal": "accidental-label",
                    },
                )
            ]
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "kafka-001",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            broker_topic_rows,
            MagicMock(),
        )

        assert resolution.metrics_derived.ids() == frozenset()
        assert resolution.context["principal_attribution_status"] == "not_observed"

    def test_static_identities_remain_resource_active_policy_inputs(
        self, static_config, mock_metrics_source: MagicMock
    ) -> None:
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(static_config, mock_metrics_source)
        resolution = handler.resolve_identities(
            "tenant-1",
            "kafka-001",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.resource_active.ids() == frozenset({"team-data"})
        assert resolution.context["principal_attribution_status"] == "policy_only_configured"
        mock_metrics_source.query.assert_not_called()

    @pytest.mark.parametrize(
        "product_type",
        [
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        ],
    )
    def test_product_types_do_not_request_principal_broker_topic_metrics(
        self, product_type: str, base_config, mock_metrics_source: MagicMock
    ) -> None:
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)

        assert handler.get_metrics_for_product_type(product_type) == []


class TestHandlerTelemetryAliases:
    def test_legacy_readiness_and_measured_quota_queries_use_resolved_names_without_value_mapping(self) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "billing-cluster-a",
                "metrics_identifier": "kafka-prod",
                "metrics_identifier_label": "deployment",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "metrics": {"url": "http://prometheus:9090"},
                "identity_source": {"source": "both"},
                "metric_name_overrides": {
                    "kafka_server_quota_byte_rate": "company_quota_rate",
                    "kafka_server_quota_throttle_time_ms": "company_quota_throttle",
                },
                "label_name_overrides": {
                    "kafka_server_quota_byte_rate": {
                        "broker": "node",
                        "quota_type": "quota_kind",
                        "quota_scope": "scope_name",
                        "user": "principal_name",
                        "client_id": "client_name",
                    },
                    "kafka_server_quota_throttle_time_ms": {
                        "broker": "node",
                        "quota_type": "quota_kind",
                        "quota_scope": "scope_name",
                        "user": "principal_name",
                        "client_id": "client_name",
                    },
                },
            }
        )
        source = MagicMock()
        source.query.return_value = {"quota_byte_rate": [], "quota_throttle_time_ms": []}
        handler = SelfManagedKafkaHandler(
            config,
            source,
            telemetry_catalog=ResolvedTelemetryCatalog(config),
        )

        measured = handler._quota_query("ingress", "Produce", timedelta(hours=2))
        evidence = handler._principal_telemetry_evidence(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
        )

        assert (
            measured.query_expression,
            measured.label_keys,
            measured.resource_label,
            measured.query_mode,
        ) == (
            'company_quota_rate{deployment="kafka-prod",quota_kind="Produce"}[7200s]',
            ("node", "deployment", "quota_kind", "scope_name", "principal_name", "client_name"),
            "deployment",
            "instant",
        )
        readiness_queries = source.query.call_args.kwargs["queries"]
        observed = [
            (query.key, query.query_expression, query.label_keys, query.resource_label) for query in readiness_queries
        ]
        assert observed == [
            (
                "quota_byte_rate",
                "sum by (quota_kind, scope_name, principal_name, client_name) (company_quota_rate{})",
                ("quota_kind", "scope_name", "principal_name", "client_name", "deployment"),
                "deployment",
            ),
            (
                "quota_throttle_time_ms",
                "avg by (quota_kind, scope_name, principal_name, client_name) (company_quota_throttle{})",
                ("quota_kind", "scope_name", "principal_name", "client_name", "deployment"),
                "deployment",
            ),
        ]
        assert evidence.status.value == "not_observed"
