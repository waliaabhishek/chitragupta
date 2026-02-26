"""Tests for Prometheus-based resource and identity discovery."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

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


class TestGatherBrokersFromMetrics:
    def test_discovers_brokers_from_metrics(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_brokers_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [
                make_row("distinct_brokers", {"broker": "0"}),
                make_row("distinct_brokers", {"broker": "1"}),
                make_row("distinct_brokers", {"broker": "2"}),
            ]
        }

        brokers = list(gather_brokers_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))

        assert len(brokers) == 3
        ids = {b.resource_id for b in brokers}
        assert "cluster-001:broker:0" in ids
        assert "cluster-001:broker:1" in ids
        assert "cluster-001:broker:2" in ids

    def test_broker_parent_id_is_cluster(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_brokers_from_metrics

        mock_metrics_source.query.return_value = {"distinct_brokers": [make_row("distinct_brokers", {"broker": "0"})]}

        brokers = list(gather_brokers_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        assert brokers[0].parent_id == "cluster-001"
        assert brokers[0].resource_type == "broker"

    def test_broker_temporal_fields(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_brokers_from_metrics

        mock_metrics_source.query.return_value = {"distinct_brokers": [make_row("distinct_brokers", {"broker": "0"})]}

        brokers = list(gather_brokers_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        b = brokers[0]
        assert b.created_at is None
        assert b.deleted_at is None
        assert b.last_seen_at is not None

    def test_no_brokers_in_metrics(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_brokers_from_metrics

        mock_metrics_source.query.return_value = {"distinct_brokers": []}
        brokers = list(gather_brokers_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        assert brokers == []

    def test_deduplicates_brokers(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_brokers_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [
                make_row("distinct_brokers", {"broker": "0"}),
                make_row("distinct_brokers", {"broker": "0"}),  # duplicate
            ]
        }

        brokers = list(gather_brokers_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        assert len(brokers) == 1


class TestGatherTopicsFromMetrics:
    def test_discovers_topics_from_metrics(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_topics_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_topics": [
                make_row("distinct_topics", {"topic": "orders"}),
                make_row("distinct_topics", {"topic": "payments"}),
            ]
        }

        topics = list(gather_topics_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        assert len(topics) == 2
        ids = {t.resource_id for t in topics}
        assert "cluster-001:topic:orders" in ids

    def test_topic_resource_type(self, mock_metrics_source):
        from plugins.self_managed_kafka.gathering.prometheus import gather_topics_from_metrics

        mock_metrics_source.query.return_value = {"distinct_topics": [make_row("distinct_topics", {"topic": "orders"})]}

        topics = list(gather_topics_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", "cluster-001"))
        assert topics[0].resource_type == "topic"
        assert topics[0].parent_id == "cluster-001"


class TestGatherPrincipalsFromMetrics:
    def test_discovers_principals(self, mock_metrics_source, base_identity_config):
        from plugins.self_managed_kafka.gathering.prometheus import gather_principals_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_principals": [
                make_row("distinct_principals", {"principal": "User:alice"}),
                make_row("distinct_principals", {"principal": "User:bob"}),
            ]
        }

        identities = list(
            gather_principals_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", base_identity_config)
        )
        assert len(identities) == 2
        ids = {i.identity_id for i in identities}
        assert "User:alice" in ids

    def test_applies_team_mapping(self, mock_metrics_source, base_identity_config):
        from plugins.self_managed_kafka.gathering.prometheus import gather_principals_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_principals": [make_row("distinct_principals", {"principal": "User:alice"})]
        }

        identities = list(
            gather_principals_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", base_identity_config)
        )
        alice = identities[0]
        assert alice.metadata["team"] == "team-data"

    def test_uses_default_team_when_not_mapped(self, mock_metrics_source, base_identity_config):
        from plugins.self_managed_kafka.gathering.prometheus import gather_principals_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_principals": [make_row("distinct_principals", {"principal": "User:unknown"})]
        }

        identities = list(
            gather_principals_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", base_identity_config)
        )
        assert identities[0].metadata["team"] == "UNASSIGNED"

    def test_temporal_fields(self, mock_metrics_source, base_identity_config):
        from plugins.self_managed_kafka.gathering.prometheus import gather_principals_from_metrics

        mock_metrics_source.query.return_value = {
            "distinct_principals": [make_row("distinct_principals", {"principal": "User:alice"})]
        }

        identities = list(
            gather_principals_from_metrics(mock_metrics_source, "self_managed_kafka", "t1", base_identity_config)
        )
        i = identities[0]
        assert i.created_at is None
        assert i.deleted_at is None
        assert i.last_seen_at is not None


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
