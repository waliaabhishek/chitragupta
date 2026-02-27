"""Tests for ConnectorHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.models import Resource, ResourceStatus


class TestConnectorHandlerProperties:
    """Tests for ConnectorHandler properties."""

    def test_service_type(self) -> None:
        """service_type returns 'connector'."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.service_type == "connector"

    def test_handles_product_types(self) -> None:
        """handles_product_types returns all Connect product types."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        expected = (
            "CONNECT_CAPACITY",
            "CONNECT_NUM_TASKS",
            "CONNECT_THROUGHPUT",
            "CUSTOM_CONNECT_PLUGIN",
        )
        assert handler.handles_product_types == expected


class TestConnectorHandlerGetAllocator:
    """Tests for get_allocator method."""

    def test_capacity_allocator(self) -> None:
        """CONNECT_CAPACITY returns connect_capacity_allocator."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("CONNECT_CAPACITY") is connect_capacity_allocator

    def test_tasks_allocator(self) -> None:
        """CONNECT_NUM_TASKS returns connect_tasks_allocator."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("CONNECT_NUM_TASKS") is connect_tasks_allocator

    def test_throughput_allocator(self) -> None:
        """CONNECT_THROUGHPUT returns connect_throughput_allocator."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_throughput_allocator,
        )
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("CONNECT_THROUGHPUT") is connect_throughput_allocator

    def test_custom_plugin_allocator(self) -> None:
        """CUSTOM_CONNECT_PLUGIN returns connect_capacity_allocator (infrastructure cost)."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("CUSTOM_CONNECT_PLUGIN") is connect_capacity_allocator

    def test_unknown_product_type_raises(self) -> None:
        """Unknown product type raises ValueError."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")


class TestConnectorHandlerGetMetrics:
    """Tests for get_metrics_for_product_type method."""

    def test_capacity_returns_empty(self) -> None:
        """CONNECT_CAPACITY returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("CONNECT_CAPACITY")
        assert metrics == []

    def test_tasks_returns_empty(self) -> None:
        """CONNECT_NUM_TASKS returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("CONNECT_NUM_TASKS")
        assert metrics == []

    def test_throughput_returns_empty(self) -> None:
        """CONNECT_THROUGHPUT returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("CONNECT_THROUGHPUT")
        assert metrics == []

    def test_custom_plugin_returns_empty(self) -> None:
        """CUSTOM_CONNECT_PLUGIN returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("CUSTOM_CONNECT_PLUGIN")
        assert metrics == []


class TestConnectorHandlerGatherResources:
    """Tests for gather_resources method."""

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when connection is None."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow))
        assert result == []

    def test_calls_gather_connectors_with_kafka_clusters(self, mock_uow: MagicMock) -> None:
        """gather_resources calls gather_connectors with Kafka cluster IDs."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_conn = MagicMock()

        # Setup mock resources with Kafka clusters
        kafka_cluster = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lkc-abc",
            resource_type="kafka_cluster",
            status=ResourceStatus.ACTIVE,
            parent_id="env-001",
            metadata={},
        )
        other_resource = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-001",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        mock_uow.resources.find_by_period.return_value = ([kafka_cluster, other_resource], 2)

        connector_resource = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_connectors",
            return_value=[connector_resource],
        ) as mock_gather:
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow))

        # Should be called with (env_id, cluster_id) tuples
        mock_gather.assert_called_once()
        call_args = mock_gather.call_args
        assert call_args[0][0] is mock_conn  # connection
        assert call_args[0][1] == "confluent_cloud"  # ecosystem
        assert call_args[0][2] == "org-123"  # tenant_id
        clusters_arg = list(call_args[0][3])  # clusters iterable
        assert clusters_arg == [("env-001", "lkc-abc")]

        assert result == [connector_resource]

    def test_handles_multiple_kafka_clusters(self, mock_uow: MagicMock) -> None:
        """gather_resources handles multiple Kafka clusters."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_conn = MagicMock()

        # Multiple Kafka clusters in different environments
        cluster1 = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lkc-001",
            resource_type="kafka_cluster",
            status=ResourceStatus.ACTIVE,
            parent_id="env-001",
            metadata={},
        )
        cluster2 = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lkc-002",
            resource_type="kafka_cluster",
            status=ResourceStatus.ACTIVE,
            parent_id="env-002",
            metadata={},
        )
        mock_uow.resources.find_by_period.return_value = ([cluster1, cluster2], 2)

        with patch(
            "plugins.confluent_cloud.gathering.gather_connectors",
            return_value=[],
        ) as mock_gather:
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow))

        # Should include both clusters as (env_id, cluster_id) tuples
        call_args = mock_gather.call_args
        clusters_arg = list(call_args[0][3])
        assert ("env-001", "lkc-001") in clusters_arg
        assert ("env-002", "lkc-002") in clusters_arg

    def test_handles_no_kafka_clusters(self, mock_uow: MagicMock) -> None:
        """gather_resources handles case where no Kafka clusters exist."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_conn = MagicMock()
        mock_uow.resources.find_by_period.return_value = ([], 0)

        with patch(
            "plugins.confluent_cloud.gathering.gather_connectors",
            return_value=[],
        ) as mock_gather:
            handler = ConnectorHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow))

        # Should be called with empty clusters list
        call_args = mock_gather.call_args
        clusters_arg = list(call_args[0][3])
        assert clusters_arg == []
        assert result == []


class TestConnectorHandlerGatherIdentities:
    """Tests for gather_identities method."""

    def test_returns_empty(self, mock_uow: MagicMock) -> None:
        """gather_identities returns empty (Kafka handler gathers org-level identities)."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("org-123", mock_uow))
        assert result == []


class TestConnectorHandlerResolveIdentities:
    """Tests for resolve_identities method."""

    def test_delegates_to_resolve_connector_identity(self, mock_uow: MagicMock) -> None:
        """resolve_identities delegates to resolve_connector_identity."""
        from core.models import IdentityResolution, IdentitySet
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        with patch(
            "plugins.confluent_cloud.handlers.connectors.resolve_connector_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
            result = handler.resolve_identities(
                tenant_id="org-123",
                resource_id="conn-abc",
                billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                billing_duration=timedelta(hours=24),
                metrics_data=None,
                uow=mock_uow,
            )

        mock_resolve.assert_called_once_with(
            tenant_id="org-123",
            resource_id="conn-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )
        assert result is mock_resolution

    def test_passes_correct_billing_window(self, mock_uow: MagicMock) -> None:
        """resolve_identities computes correct billing end from timestamp + duration."""
        from core.models import IdentityResolution, IdentitySet
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        with patch(
            "plugins.confluent_cloud.handlers.connectors.resolve_connector_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
            handler.resolve_identities(
                tenant_id="org-123",
                resource_id="conn-abc",
                billing_timestamp=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
                billing_duration=timedelta(hours=6),
                metrics_data={"unused": []},  # Should be ignored for connectors
                uow=mock_uow,
            )

        call_kwargs = mock_resolve.call_args.kwargs
        assert call_kwargs["billing_start"] == datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
        assert call_kwargs["billing_end"] == datetime(2026, 1, 15, 18, 0, tzinfo=UTC)
