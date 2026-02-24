"""Tests for KsqldbHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.models import Resource, ResourceStatus


class TestKsqldbHandlerProperties:
    """Tests for KsqldbHandler properties."""

    def test_service_type(self) -> None:
        """service_type returns 'ksqldb'."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.service_type == "ksqldb"

    def test_handles_product_types(self) -> None:
        """handles_product_types returns both ksqlDB product types."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        expected = (
            "KSQL_NUM_CSU",
            "KSQL_NUM_CSUS",
        )
        assert handler.handles_product_types == expected


class TestKsqldbHandlerGetAllocator:
    """Tests for get_allocator method."""

    def test_csu_allocator(self) -> None:
        """KSQL_NUM_CSU returns ksqldb_csu_allocator."""
        from plugins.confluent_cloud.allocators.ksqldb_allocators import (
            ksqldb_csu_allocator,
        )
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KSQL_NUM_CSU") is ksqldb_csu_allocator

    def test_csus_allocator(self) -> None:
        """KSQL_NUM_CSUS returns ksqldb_csu_allocator (alternate spelling)."""
        from plugins.confluent_cloud.allocators.ksqldb_allocators import (
            ksqldb_csu_allocator,
        )
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("KSQL_NUM_CSUS") is ksqldb_csu_allocator

    def test_unknown_product_type_raises(self) -> None:
        """Unknown product type raises ValueError."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")


class TestKsqldbHandlerGetMetrics:
    """Tests for get_metrics_for_product_type method."""

    def test_csu_returns_empty(self) -> None:
        """KSQL_NUM_CSU returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KSQL_NUM_CSU")
        assert metrics == []

    def test_csus_returns_empty(self) -> None:
        """KSQL_NUM_CSUS returns empty list (no metrics needed)."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("KSQL_NUM_CSUS")
        assert metrics == []


class TestKsqldbHandlerGatherResources:
    """Tests for gather_resources method."""

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when connection is None."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow))
        assert result == []

    def test_calls_gather_ksqldb_clusters_with_env_ids(self, mock_uow: MagicMock) -> None:
        """gather_resources calls gather_ksqldb_clusters with environment IDs."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_conn = MagicMock()

        # Setup mock resources with environments
        environment = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-001",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        other_resource = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lkc-abc",
            resource_type="kafka_cluster",
            status=ResourceStatus.ACTIVE,
            parent_id="env-001",
            metadata={},
        )
        mock_uow.resources.find_by_period.return_value = [environment, other_resource]

        ksqldb_resource = MagicMock()

        with patch(
            "plugins.confluent_cloud.gathering.gather_ksqldb_clusters",
            return_value=[ksqldb_resource],
        ) as mock_gather:
            handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow))

        # Should be called with environment IDs
        mock_gather.assert_called_once()
        call_args = mock_gather.call_args
        assert call_args[0][0] is mock_conn  # connection
        assert call_args[0][1] == "confluent_cloud"  # ecosystem
        assert call_args[0][2] == "org-123"  # tenant_id
        env_ids_arg = list(call_args[0][3])  # environment_ids iterable
        assert env_ids_arg == ["env-001"]

        assert result == [ksqldb_resource]

    def test_handles_multiple_environments(self, mock_uow: MagicMock) -> None:
        """gather_resources handles multiple environments."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_conn = MagicMock()

        # Multiple environments
        env1 = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-001",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        env2 = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-002",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        mock_uow.resources.find_by_period.return_value = [env1, env2]

        with patch(
            "plugins.confluent_cloud.gathering.gather_ksqldb_clusters",
            return_value=[],
        ) as mock_gather:
            handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            list(handler.gather_resources("org-123", mock_uow))

        # Should include both environment IDs
        call_args = mock_gather.call_args
        env_ids_arg = list(call_args[0][3])
        assert "env-001" in env_ids_arg
        assert "env-002" in env_ids_arg

    def test_handles_no_environments(self, mock_uow: MagicMock) -> None:
        """gather_resources handles case where no environments exist."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_conn = MagicMock()
        mock_uow.resources.find_by_period.return_value = []

        with patch(
            "plugins.confluent_cloud.gathering.gather_ksqldb_clusters",
            return_value=[],
        ) as mock_gather:
            handler = KsqldbHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow))

        # Should be called with empty environment IDs list
        call_args = mock_gather.call_args
        env_ids_arg = list(call_args[0][3])
        assert env_ids_arg == []
        assert result == []


class TestKsqldbHandlerGatherIdentities:
    """Tests for gather_identities method."""

    def test_returns_empty(self, mock_uow: MagicMock) -> None:
        """gather_identities returns empty (Kafka handler gathers org-level identities)."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("org-123", mock_uow))
        assert result == []


class TestKsqldbHandlerResolveIdentities:
    """Tests for resolve_identities method."""

    def test_delegates_to_resolve_ksqldb_identity(self, mock_uow: MagicMock) -> None:
        """resolve_identities delegates to resolve_ksqldb_identity."""
        from core.models import IdentityResolution, IdentitySet
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        with patch(
            "plugins.confluent_cloud.handlers.ksqldb.resolve_ksqldb_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
            result = handler.resolve_identities(
                tenant_id="org-123",
                resource_id="lksqlc-abc",
                billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                billing_duration=timedelta(hours=24),
                metrics_data=None,
                uow=mock_uow,
            )

        mock_resolve.assert_called_once_with(
            tenant_id="org-123",
            resource_id="lksqlc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )
        assert result is mock_resolution

    def test_passes_correct_billing_window(self, mock_uow: MagicMock) -> None:
        """resolve_identities computes correct billing end from timestamp + duration."""
        from core.models import IdentityResolution, IdentitySet
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        with patch(
            "plugins.confluent_cloud.handlers.ksqldb.resolve_ksqldb_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
            handler.resolve_identities(
                tenant_id="org-123",
                resource_id="lksqlc-abc",
                billing_timestamp=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
                billing_duration=timedelta(hours=6),
                metrics_data={"unused": []},  # Should be ignored for ksqlDB
                uow=mock_uow,
            )

        call_kwargs = mock_resolve.call_args.kwargs
        assert call_kwargs["billing_start"] == datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
        assert call_kwargs["billing_end"] == datetime(2026, 1, 15, 18, 0, tzinfo=UTC)
