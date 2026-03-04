"""TASK-008: SelfManagedKafkaHandler.gather_resources() with SMKSharedContext.

Verifies that the handler yields shared_ctx.cluster_resource first,
then proceeds with broker/topic gathering — without calling gather_cluster_resource().
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.models import Resource, ResourceStatus


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
def mock_metrics_source():
    return MagicMock()


def _make_cluster_resource(cluster_id: str = "kafka-001") -> Resource:
    return Resource(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        resource_id=cluster_id,
        resource_type="cluster",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )


@pytest.fixture
def smk_shared_ctx():
    """An SMKSharedContext with a mock cluster resource."""
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

    cluster = _make_cluster_resource("kafka-001")
    return SMKSharedContext(cluster_resource=cluster)


class TestSMKHandlerGatherResourcesSharedContext:
    """TASK-008: SelfManagedKafkaHandler yields cluster from shared_ctx first."""

    def test_yields_cluster_resource_from_shared_ctx_first(
        self, base_config: Any, mock_metrics_source: MagicMock, smk_shared_ctx: Any
    ) -> None:
        """The first yielded resource is shared_ctx.cluster_resource."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [],
            "distinct_topics": [],
        }
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        result = list(handler.gather_resources("tenant-1", uow, smk_shared_ctx))

        assert len(result) == 1
        assert result[0].resource_id == "kafka-001"
        assert result[0].resource_type == "cluster"

    def test_does_not_call_gather_cluster_resource(
        self, base_config: Any, mock_metrics_source: MagicMock, smk_shared_ctx: Any
    ) -> None:
        """gather_cluster_resource() must NOT be called — cluster comes from shared_ctx."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [],
            "distinct_topics": [],
        }
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        prom_module = "plugins.self_managed_kafka.gathering.prometheus"
        with patch(f"{prom_module}.gather_cluster_resource") as mock_gather_cluster:
            list(handler.gather_resources("tenant-1", uow, smk_shared_ctx))

        mock_gather_cluster.assert_not_called()

    def test_proceeds_with_broker_gathering_after_cluster(
        self, base_config: Any, mock_metrics_source: MagicMock, smk_shared_ctx: Any
    ) -> None:
        """Handler continues with broker/topic gathering after yielding cluster from shared_ctx."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [
                MagicMock(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="distinct_brokers",
                    value=1.0,
                    labels={"broker": "0"},
                )
            ],
            "distinct_topics": [],
        }
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        result = list(handler.gather_resources("tenant-1", uow, smk_shared_ctx))
        resource_types = {r.resource_type for r in result}

        assert "cluster" in resource_types
        assert "broker" in resource_types

    def test_returns_empty_when_shared_ctx_wrong_type(self, base_config: Any, mock_metrics_source: MagicMock) -> None:
        """gather_resources yields nothing when shared_ctx is not SMKSharedContext."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        result = list(handler.gather_resources("tenant-1", uow, shared_ctx=None))
        assert result == []

    def test_cluster_resource_from_shared_ctx_is_yielded_not_constructed(
        self, base_config: Any, mock_metrics_source: MagicMock
    ) -> None:
        """The exact cluster_resource from shared_ctx is yielded (identity check)."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        specific_cluster = _make_cluster_resource("my-specific-cluster")
        ctx = SMKSharedContext(cluster_resource=specific_cluster)

        mock_metrics_source.query.return_value = {
            "distinct_brokers": [],
            "distinct_topics": [],
        }
        handler = SelfManagedKafkaHandler(base_config, mock_metrics_source)
        uow = MagicMock()

        result = list(handler.gather_resources("tenant-1", uow, ctx))

        # The first resource must be the exact cluster object from shared_ctx
        assert result[0] is specific_cluster


# Type alias for fixture type hints (avoids import issues at collection time)
from typing import Any
