"""Tests for FlinkHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.models import IdentityResolution, IdentitySet, Resource, ResourceStatus


class TestFlinkHandlerProperties:
    """Tests for FlinkHandler properties."""

    def test_service_type(self) -> None:
        """service_type returns 'flink'."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.service_type == "flink"

    def test_handles_product_types(self) -> None:
        """handles_product_types returns both Flink product types."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        expected = ("FLINK_NUM_CFU", "FLINK_NUM_CFUS")
        assert handler.handles_product_types == expected


class TestFlinkHandlerGetAllocator:
    """Tests for get_allocator method."""

    def test_cfu_allocator(self) -> None:
        """FLINK_NUM_CFU returns flink_cfu_allocator."""
        from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("FLINK_NUM_CFU") is flink_cfu_allocator

    def test_cfus_allocator(self) -> None:
        """FLINK_NUM_CFUS returns flink_cfu_allocator (alternate spelling)."""
        from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        assert handler.get_allocator("FLINK_NUM_CFUS") is flink_cfu_allocator

    def test_unknown_product_type_raises(self) -> None:
        """Unknown product type raises ValueError."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")


class TestFlinkHandlerGetMetrics:
    """Tests for get_metrics_for_product_type method."""

    def test_cfu_returns_metric_query(self) -> None:
        """FLINK_NUM_CFU returns CFU metric query."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("FLINK_NUM_CFU")

        assert len(metrics) == 2
        keys = {mq.key for mq in metrics}
        assert "flink_cfu_primary" in keys
        assert "flink_cfu_fallback" in keys
        primary = next(mq for mq in metrics if mq.key == "flink_cfu_primary")
        assert "compute_pool_id" in primary.label_keys
        assert "flink_statement_name" in primary.label_keys
        assert primary.resource_label == "compute_pool_id"

    def test_cfus_returns_metric_query(self) -> None:
        """FLINK_NUM_CFUS returns primary and fallback CFU metric queries."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("FLINK_NUM_CFUS")
        assert len(metrics) == 2
        keys = {mq.key for mq in metrics}
        assert "flink_cfu_primary" in keys
        assert "flink_cfu_fallback" in keys


class TestFlinkMetricQueryResourceFilter:
    """Tests for GAP-19: Flink queries must use {} placeholder for resource filter injection."""

    def test_primary_query_contains_placeholder(self) -> None:
        """_FLINK_METRICS_PRIMARY query_expression contains {} placeholder."""
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_PRIMARY

        assert "{}" in _FLINK_METRICS_PRIMARY.query_expression

    def test_fallback_query_contains_placeholder(self) -> None:
        """_FLINK_METRICS_FALLBACK query_expression contains {} placeholder."""
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_FALLBACK

        assert "{}" in _FLINK_METRICS_FALLBACK.query_expression

    def test_primary_with_resource_id_filter_injects_compute_pool_id(self) -> None:
        """resource_id_filter injects {compute_pool_id="lfcp-abc123"} into primary query."""
        from core.metrics.prometheus import _inject_resource_filter
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_PRIMARY

        result = _inject_resource_filter(
            expression=_FLINK_METRICS_PRIMARY.query_expression,
            resource_label=_FLINK_METRICS_PRIMARY.resource_label,
            resource_id_filter="lfcp-abc123",
        )

        assert '{compute_pool_id="lfcp-abc123"}' in result

    def test_fallback_with_resource_id_filter_injects_compute_pool_id(self) -> None:
        """resource_id_filter injects {compute_pool_id="lfcp-abc123"} into fallback query."""
        from core.metrics.prometheus import _inject_resource_filter
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_FALLBACK

        result = _inject_resource_filter(
            expression=_FLINK_METRICS_FALLBACK.query_expression,
            resource_label=_FLINK_METRICS_FALLBACK.resource_label,
            resource_id_filter="lfcp-abc123",
        )

        assert '{compute_pool_id="lfcp-abc123"}' in result

    def test_primary_with_none_filter_strips_placeholder(self) -> None:
        """resource_id_filter=None strips {} from primary query, leaving no selector braces."""
        from core.metrics.prometheus import _inject_resource_filter
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_PRIMARY

        result = _inject_resource_filter(
            expression=_FLINK_METRICS_PRIMARY.query_expression,
            resource_label=_FLINK_METRICS_PRIMARY.resource_label,
            resource_id_filter=None,
        )

        assert "{}" not in result
        assert "confluent_flink_num_cfu" in result
        # No selector braces remain around the metric name
        assert "confluent_flink_num_cfu{" not in result

    def test_fallback_with_none_filter_strips_placeholder(self) -> None:
        """resource_id_filter=None strips {} from fallback query, leaving no selector braces."""
        from core.metrics.prometheus import _inject_resource_filter
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_FALLBACK

        result = _inject_resource_filter(
            expression=_FLINK_METRICS_FALLBACK.query_expression,
            resource_label=_FLINK_METRICS_FALLBACK.resource_label,
            resource_id_filter=None,
        )

        assert "{}" not in result
        assert "confluent_flink_statement_utilization_cfu_minutes_consumed" in result
        assert "confluent_flink_statement_utilization_cfu_minutes_consumed{" not in result

    def test_different_pools_produce_distinct_expressions(self) -> None:
        """Two different pool IDs produce distinct, pool-specific expressions."""
        from core.metrics.prometheus import _inject_resource_filter
        from plugins.confluent_cloud.handlers.flink import _FLINK_METRICS_PRIMARY

        query = _FLINK_METRICS_PRIMARY

        result_pool1 = _inject_resource_filter(query.query_expression, query.resource_label, "lfcp-abc123")
        result_pool2 = _inject_resource_filter(query.query_expression, query.resource_label, "lfcp-xyz789")

        assert result_pool1 != result_pool2
        assert 'compute_pool_id="lfcp-abc123"' in result_pool1
        assert 'compute_pool_id="lfcp-xyz789"' in result_pool2
        assert "lfcp-abc123" not in result_pool2
        assert "lfcp-xyz789" not in result_pool1


class TestFlinkHandlerGatherResources:
    """Tests for gather_resources method."""

    def test_returns_empty_when_connection_is_none(self, mock_uow: MagicMock) -> None:
        """gather_resources yields nothing when connection is None."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_resources("org-123", mock_uow))
        assert result == []

    def test_calls_gather_functions_with_env_ids(self, mock_uow: MagicMock) -> None:
        """gather_resources calls gather_flink_compute_pools with env_ids from shared_ctx."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        mock_conn = MagicMock()

        environment = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="env-001",
            resource_type="environment",
            status=ResourceStatus.ACTIVE,
            metadata={},
        )
        ctx = CCloudSharedContext(
            environment_resources=(environment,),
            kafka_cluster_resources=(),
        )

        pool_resource = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            resource_type="flink_compute_pool",
            status=ResourceStatus.ACTIVE,
            metadata={"region": "us-east-1", "cloud": "aws", "is_allocatable": False},
        )

        with (
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_compute_pools",
                return_value=[pool_resource],
            ) as mock_gather_pools,
            patch(
                "plugins.confluent_cloud.gathering.gather_flink_statements",
                return_value=[],
            ) as mock_gather_stmts,
        ):
            handler = FlinkHandler(connection=mock_conn, config=None, ecosystem="confluent_cloud")
            result = list(handler.gather_resources("org-123", mock_uow, ctx))

        mock_gather_pools.assert_called_once()
        call_args = mock_gather_pools.call_args
        assert call_args[0][0] is mock_conn
        assert call_args[0][1] == "confluent_cloud"
        assert call_args[0][2] == "org-123"
        env_ids_arg = list(call_args[0][3])
        assert env_ids_arg == ["env-001"]

        assert pool_resource in result
        mock_gather_stmts.assert_called_once()


class TestFlinkHandlerGatherIdentities:
    """Tests for gather_identities method."""

    def test_returns_empty(self, mock_uow: MagicMock) -> None:
        """gather_identities returns empty (Kafka handler gathers org-level identities)."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        result = list(handler.gather_identities("org-123", mock_uow))
        assert result == []


class TestFlinkHandlerResolveIdentities:
    """Tests for resolve_identities method."""

    def test_delegates_to_resolve_flink_identity(self, mock_uow: MagicMock) -> None:
        """resolve_identities delegates to resolve_flink_identity."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={"stmt_owner_cfu": {"sa-1": 10.0}},
        )

        with patch(
            "plugins.confluent_cloud.handlers.flink.resolve_flink_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
            result = handler.resolve_identities(
                tenant_id="org-123",
                resource_id="lfcp-pool-1",
                billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                billing_duration=timedelta(hours=24),
                metrics_data={"some": []},
                uow=mock_uow,
            )

        mock_resolve.assert_called_once_with(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data={"some": []},
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )
        assert result is mock_resolution

    def test_passes_correct_billing_window(self, mock_uow: MagicMock) -> None:
        """resolve_identities computes correct billing end from timestamp + duration."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        with patch(
            "plugins.confluent_cloud.handlers.flink.resolve_flink_identity",
            return_value=mock_resolution,
        ) as mock_resolve:
            handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
            handler.resolve_identities(
                tenant_id="org-123",
                resource_id="lfcp-pool-1",
                billing_timestamp=datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
                billing_duration=timedelta(hours=6),
                metrics_data=None,
                uow=mock_uow,
            )

        call_kwargs = mock_resolve.call_args.kwargs
        assert call_kwargs["billing_start"] == datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
        assert call_kwargs["billing_end"] == datetime(2026, 1, 15, 18, 0, tzinfo=UTC)

    def test_context_carries_stmt_owner_cfu(self, mock_uow: MagicMock) -> None:
        """IdentityResolution.context carries stmt_owner_cfu for allocator."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        mock_resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={"stmt_owner_cfu": {"sa-1": 25.0, "sa-2": 75.0}},
        )

        with patch(
            "plugins.confluent_cloud.handlers.flink.resolve_flink_identity",
            return_value=mock_resolution,
        ):
            handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
            result = handler.resolve_identities(
                tenant_id="org-123",
                resource_id="lfcp-pool-1",
                billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                billing_duration=timedelta(hours=24),
                metrics_data=None,
                uow=mock_uow,
            )

        assert result.context["stmt_owner_cfu"] == {"sa-1": 25.0, "sa-2": 75.0}
