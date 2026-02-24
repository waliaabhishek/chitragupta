"""Tests for KafkaHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from core.models import Identity, MetricRow


class TestKafkaHandlerProperties:
    """Tests for KafkaHandler properties."""

    def test_service_type(self) -> None:
        """service_type returns 'kafka'."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.service_type == "kafka"

    def test_handles_product_types(self) -> None:
        """handles_product_types returns all Kafka product types."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        expected = (
            "KAFKA_NUM_CKU",
            "KAFKA_NUM_CKUS",
            "KAFKA_BASE",
            "KAFKA_PARTITION",
            "KAFKA_STORAGE",
            "KAFKA_NETWORK_READ",
            "KAFKA_NETWORK_WRITE",
        )
        assert handler.handles_product_types == expected


class TestKafkaHandlerGetAllocator:
    """Tests for get_allocator method."""

    def test_cku_allocator(self) -> None:
        """KAFKA_NUM_CKU returns kafka_num_cku_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_NUM_CKU") is kafka_num_cku_allocator

    def test_ckus_allocator(self) -> None:
        """KAFKA_NUM_CKUS returns kafka_num_cku_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_NUM_CKUS") is kafka_num_cku_allocator

    def test_network_read_allocator(self) -> None:
        """KAFKA_NETWORK_READ returns kafka_network_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_NETWORK_READ") is kafka_network_allocator

    def test_network_write_allocator(self) -> None:
        """KAFKA_NETWORK_WRITE returns kafka_network_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_NETWORK_WRITE") is kafka_network_allocator

    def test_base_allocator(self) -> None:
        """KAFKA_BASE returns kafka_base_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_BASE") is kafka_base_allocator

    def test_partition_allocator(self) -> None:
        """KAFKA_PARTITION returns kafka_base_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_PARTITION") is kafka_base_allocator

    def test_storage_allocator(self) -> None:
        """KAFKA_STORAGE returns kafka_base_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KAFKA_STORAGE") is kafka_base_allocator

    def test_unknown_product_type_raises(self) -> None:
        """Unknown product type raises ValueError."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")


class TestKafkaHandlerGetMetrics:
    """Tests for get_metrics_for_product_type method."""

    def test_cku_returns_metrics(self) -> None:
        """KAFKA_NUM_CKU returns bytes_in and bytes_out metrics."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KAFKA_NUM_CKU")

        assert len(metrics) == 2
        keys = {m.key for m in metrics}
        assert keys == {"bytes_in", "bytes_out"}

    def test_network_returns_metrics(self) -> None:
        """KAFKA_NETWORK_READ returns bytes_in and bytes_out metrics."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KAFKA_NETWORK_READ")

        assert len(metrics) == 2

    def test_base_returns_empty(self) -> None:
        """KAFKA_BASE returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KAFKA_BASE")

        assert metrics == []

    def test_storage_returns_empty(self) -> None:
        """KAFKA_STORAGE returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KAFKA_STORAGE")

        assert metrics == []

    def test_metrics_have_correct_structure(self) -> None:
        """Metrics have correct query structure with placeholders."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KAFKA_NUM_CKU")

        for metric in metrics:
            assert "{resource_id}" in metric.query_expression
            assert "{step}" in metric.query_expression
            assert "principal_id" in metric.label_keys
            assert metric.resource_label == "kafka_id"


class TestKafkaHandlerGatherResources:
    """Tests for gather_resources method."""

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when connection is None."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow))
        assert result == []

    def test_calls_gather_environments_and_clusters(self, mock_uow: MagicMock) -> None:
        """gather_resources calls gather_environments then gather_kafka_clusters."""
        from unittest.mock import patch

        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_conn = MagicMock()
        env_resource = MagicMock()
        env_resource.resource_id = "env-abc"
        cluster_resource = MagicMock()

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_environments",
                return_value=[env_resource],
            ) as mock_envs,
            patch(
                "plugins.confluent_cloud.gathering.gather_kafka_clusters",
                return_value=[cluster_resource],
            ) as mock_clusters,
        ):
            handler = KafkaHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow))

        mock_envs.assert_called_once_with(mock_conn, "confluent_cloud", "org-123")
        mock_clusters.assert_called_once_with(mock_conn, "confluent_cloud", "org-123", ["env-abc"])
        assert result == [env_resource, cluster_resource]


class TestKafkaHandlerResolveIdentities:
    """Tests for resolve_identities method."""

    def test_resolves_api_key_owners(self, mock_uow: MagicMock) -> None:
        """API key owners are resolved to resource_active."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner",
            identity_type="service_account",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.identities.find_by_period.return_value = [api_key, sa_owner]

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=24),
            metrics_data=None,
            uow=mock_uow,
        )

        assert len(result.resource_active) == 1
        assert "sa-owner" in result.resource_active.ids()

    def test_extracts_metrics_principals(self, mock_uow: MagicMock) -> None:
        """Principal IDs from metrics are added to metrics_derived."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_uow.identities.find_by_period.return_value = []
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="bytes_in",
                    value=1000.0,
                    labels={"kafka_id": "lkc-abc", "principal_id": "sa-metrics-user"},
                ),
            ],
        }

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=24),
            metrics_data=metrics_data,
            uow=mock_uow,
        )

        assert len(result.metrics_derived) == 1
        assert "sa-metrics-user" in result.metrics_derived.ids()

    def test_tenant_period_is_empty(self, mock_uow: MagicMock) -> None:
        """tenant_period is returned empty (orchestrator fills it)."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        mock_uow.identities.find_by_period.return_value = []

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(hours=24),
            metrics_data=None,
            uow=mock_uow,
        )

        assert len(result.tenant_period) == 0
