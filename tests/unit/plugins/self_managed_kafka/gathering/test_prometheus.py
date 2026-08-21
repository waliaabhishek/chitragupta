"""Tests for Prometheus-based resource and identity discovery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricRow


def make_row(key: str, labels: dict) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=1.0,
        labels=labels,
    )


@pytest.fixture
def mock_metrics_source():
    return MagicMock()


@pytest.fixture
def base_identity_config():
    from plugins.self_managed_kafka.config import IdentitySourceConfig

    return IdentitySourceConfig.model_validate(
        {
            "source": "prometheus",
            "principal_to_team": {"User:alice": "team-data", "User:bob": "team-analytics"},
            "default_team": "UNASSIGNED",
        }
    )


class TestGatherClusterResource:
    def test_creates_cluster_resource(self):
        from plugins.self_managed_kafka.gathering.prometheus import gather_cluster_resource

        resource = gather_cluster_resource(
            ecosystem="self_managed_kafka",
            tenant_id="tenant-1",
            cluster_id="kafka-001",
            broker_count=3,
            region="us-west-2",
        )

        assert resource.ecosystem == "self_managed_kafka"
        assert resource.tenant_id == "tenant-1"
        assert resource.resource_id == "kafka-001"
        assert resource.resource_type == "cluster"
        assert resource.parent_id is None
        assert resource.created_at is None
        assert resource.deleted_at is None
        assert resource.last_seen_at is not None
        assert resource.metadata["broker_count"] == 3
        assert resource.metadata["region"] == "us-west-2"

    def test_display_name_defaults_to_cluster_id(self):
        from plugins.self_managed_kafka.gathering.prometheus import gather_cluster_resource

        resource = gather_cluster_resource("self_managed_kafka", "t1", "my-cluster", 3)
        assert resource.display_name == "my-cluster"

    def test_display_name_override(self):
        from plugins.self_managed_kafka.gathering.prometheus import gather_cluster_resource

        resource = gather_cluster_resource(
            "self_managed_kafka", "t1", "my-cluster", 3, display_name="Production Cluster"
        )
        assert resource.display_name == "Production Cluster"


class TestBrokerTopicDiscovery:
    def test_enabled_discovery_requires_all_three_result_families(self) -> None:
        from plugins.self_managed_kafka.gathering.prometheus import run_broker_topic_discovery

        metrics_source = MagicMock()
        metrics_source.query.return_value = {
            "broker_topic_discovery_bytes_in": [],
            "broker_topic_discovery_bytes_out": [],
        }

        with pytest.raises(MetricsQueryError, match="broker_topic_discovery_log_size"):
            run_broker_topic_discovery(
                metrics_source,
                metrics_identifier_label="kafka_cluster_id",
                metrics_identifier="kraft-a-001",
                step=timedelta(hours=1),
            )

    def test_disabled_overlay_uses_the_legacy_single_discovery_query(self) -> None:
        from plugins.self_managed_kafka.gathering.prometheus import run_broker_topic_discovery

        metrics_source = MagicMock()
        metrics_source.query.return_value = {"broker_topic_discovery": []}

        run_broker_topic_discovery(
            metrics_source,
            metrics_identifier_label="kafka_cluster_id",
            metrics_identifier="kraft-a-001",
            step=timedelta(hours=1),
            include_topic_evidence=False,
        )

        queries = metrics_source.query.call_args.kwargs["queries"]
        assert [query.key for query in queries] == ["broker_topic_discovery"]

    def test_discovery_unions_independently_lazy_bytes_and_storage_families(self) -> None:
        from plugins.self_managed_kafka.gathering.prometheus import run_broker_topic_discovery

        metrics_source = MagicMock()
        metrics_source.query.return_value = {
            "broker_topic_discovery_bytes_in": [
                make_row(
                    "broker_topic_discovery_bytes_in",
                    {"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
                )
            ],
            "broker_topic_discovery_bytes_out": [
                make_row(
                    "broker_topic_discovery_bytes_out",
                    {"kafka_cluster_id": "kraft-a-001", "broker": "2", "topic": "payments"},
                )
            ],
            "broker_topic_discovery_log_size": [
                make_row(
                    "broker_topic_discovery_log_size",
                    {"kafka_cluster_id": "kraft-a-001", "broker": "3", "topic": "idle-topic"},
                )
            ],
        }

        brokers, topics = run_broker_topic_discovery(
            metrics_source,
            metrics_identifier_label="kafka_cluster_id",
            metrics_identifier="kraft-a-001",
            step=timedelta(hours=1),
        )

        assert brokers == frozenset({"1", "2", "3"})
        assert topics == frozenset({"orders", "payments", "idle-topic"})
        queries = metrics_source.query.call_args.kwargs["queries"]
        assert {query.resource_label for query in queries} == {"kafka_cluster_id"}
        assert {query.key for query in queries} == {
            "broker_topic_discovery_bytes_in",
            "broker_topic_discovery_bytes_out",
            "broker_topic_discovery_log_size",
        }

    def test_discovery_is_cluster_scoped_and_does_not_interpret_principal_labels(self) -> None:
        from plugins.self_managed_kafka.gathering.prometheus import run_broker_topic_discovery

        metrics_source = MagicMock()
        metrics_source.query.return_value = {
            "broker_topic_discovery_bytes_in": [
                make_row(
                    "broker_topic_discovery_bytes_in",
                    {
                        "kafka_cluster_id": "kraft-a-001",
                        "broker": "1",
                        "topic": "orders",
                        "principal": "accidental-label",
                    },
                )
            ],
            "broker_topic_discovery_bytes_out": [],
            "broker_topic_discovery_log_size": [],
        }

        brokers, topics = run_broker_topic_discovery(
            metrics_source,
            metrics_identifier_label="kafka_cluster_id",
            metrics_identifier="kraft-a-001",
            step=timedelta(hours=1),
        )

        assert brokers == frozenset({"1"})
        assert topics == frozenset({"orders"})
        _, kwargs = metrics_source.query.call_args
        assert kwargs["resource_id_filter"] == "kraft-a-001"
        query = kwargs["queries"][0]
        assert query.label_keys == ("broker", "topic")
        assert query.resource_label == "kafka_cluster_id"


class TestLoadStaticIdentities:
    def test_loads_static_identities(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        config = IdentitySourceConfig.model_validate(
            {
                "source": "static",
                "static_identities": [
                    {"identity_id": "team-data", "identity_type": "team", "display_name": "Data Engineering"},
                    {"identity_id": "team-analytics", "identity_type": "team"},
                ],
            }
        )

        identities = list(load_static_identities(config, "self_managed_kafka", "t1"))
        assert len(identities) == 2
        ids = {i.identity_id for i in identities}
        assert "team-data" in ids
        assert "team-analytics" in ids

    def test_display_name_defaults_to_identity_id(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        config = IdentitySourceConfig.model_validate(
            {
                "source": "static",
                "static_identities": [{"identity_id": "team-no-name", "identity_type": "team"}],
            }
        )

        identities = list(load_static_identities(config, "self_managed_kafka", "t1"))
        assert identities[0].display_name == "team-no-name"

    def test_static_identity_temporal_fields(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        config = IdentitySourceConfig.model_validate(
            {
                "source": "static",
                "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
            }
        )

        identities = list(load_static_identities(config, "self_managed_kafka", "t1"))
        i = identities[0]
        assert i.created_at is None
        assert i.deleted_at is None
        assert i.last_seen_at is not None

    def test_empty_static_identities(self):
        from plugins.self_managed_kafka.config import IdentitySourceConfig
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        config = IdentitySourceConfig.model_validate({"source": "static"})
        identities = list(load_static_identities(config, "self_managed_kafka", "t1"))
        assert identities == []
