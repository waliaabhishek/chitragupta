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
