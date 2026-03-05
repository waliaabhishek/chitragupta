"""Tests for GenericMetricsOnlyHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from core.models import MetricRow


def make_metric_row(key: str, value: float, labels: dict | None = None) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels=labels or {},
    )


@pytest.fixture
def pg_config():
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

    return GenericMetricsOnlyConfig.model_validate(
        {
            "ecosystem_name": "self_managed_postgres",
            "cluster_id": "pg-prod-1",
            "metrics": {"url": "http://prom:9090"},
            "identity_source": {
                "source": "prometheus",
                "label": "datname",
                "discovery_query": "group by (datname) (pg_stat_database_blks_hit)",
                "default_team": "UNASSIGNED",
            },
            "cost_types": [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                },
                {
                    "name": "PG_NETWORK",
                    "product_category": "postgres",
                    "rate": "0.05",
                    "cost_quantity": {
                        "type": "network_gib",
                        "query": "sum(pg_stat_database_blks_read)",
                    },
                    "allocation_strategy": "usage_ratio",
                    "allocation_query": "sum by (datname) (pg_stat_database_blks_read)",
                    "allocation_label": "datname",
                },
            ],
        }
    )


@pytest.fixture
def static_config():
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

    return GenericMetricsOnlyConfig.model_validate(
        {
            "ecosystem_name": "self_managed_postgres",
            "cluster_id": "pg-prod-1",
            "metrics": {"url": "http://prom:9090"},
            "identity_source": {
                "source": "static",
                "static_identities": [
                    {
                        "identity_id": "team-data",
                        "identity_type": "team",
                        "display_name": "Data Team",
                        "team": "data",
                    }
                ],
            },
            "cost_types": [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ],
        }
    )


@pytest.fixture
def mock_metrics():
    return MagicMock()


class TestHandlerHandlesProductTypes:
    def test_handles_product_types_returns_names_in_config_order(self, pg_config, mock_metrics) -> None:
        """Test case 7: handles_product_types returns names in config order."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        assert list(handler.handles_product_types) == ["PG_COMPUTE", "PG_NETWORK"]


class TestHandlerGetAllocator:
    def test_get_allocator_even_split_calls_allocate_evenly(self, pg_config, mock_metrics) -> None:
        """Test case 8: even_split allocator calls allocate_evenly."""
        from unittest.mock import MagicMock, patch

        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_COMPUTE")

        mock_ctx = MagicMock()
        mock_identities = MagicMock()
        mock_identities.merged_active.ids.return_value = ["alice", "bob"]
        mock_ctx.identities = mock_identities
        mock_ctx.metrics_data = {}

        with patch("plugins.generic_metrics_only.handler.allocate_evenly") as mock_evenly:
            mock_evenly.return_value = MagicMock()
            allocator(mock_ctx)
            mock_evenly.assert_called_once()

    def test_get_allocator_usage_ratio_reads_alloc_key_and_calls_allocate_by_usage_ratio(
        self, pg_config, mock_metrics
    ) -> None:
        """Test case 9: usage_ratio allocator reads metrics_data["alloc_PG_NETWORK"] and calls allocate_by_usage_ratio."""
        from unittest.mock import MagicMock, patch

        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")

        mock_ctx = MagicMock()
        mock_identities = MagicMock()
        mock_identities.merged_active.ids.return_value = ["alice"]
        mock_ctx.identities = mock_identities

        # Provide metrics_data with the expected alloc key
        alice_row = make_metric_row("alloc_PG_NETWORK", 100.0, {"datname": "alice"})
        mock_ctx.metrics_data = {"alloc_PG_NETWORK": [alice_row]}

        with patch("plugins.generic_metrics_only.handler.allocate_by_usage_ratio") as mock_ratio:
            mock_ratio.return_value = MagicMock()
            allocator(mock_ctx)
            mock_ratio.assert_called_once()
            # Verify it was called with the identity values extracted from correct key
            # allocate_by_usage_ratio(ctx, identity_values)
            assert "alice" in mock_ratio.call_args[0][1]

    def test_get_allocator_unknown_type_raises(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("NONEXISTENT")


class TestHandlerGetMetricsForProductType:
    def test_even_split_with_prometheus_source_returns_discovery_query(self, pg_config, mock_metrics) -> None:
        """Test case 10: even_split + prometheus returns MetricQuery(key="discovery", ...)."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_COMPUTE")

        assert len(queries) == 1
        assert queries[0].key == "discovery"
        assert queries[0].query_expression == "group by (datname) (pg_stat_database_blks_hit)"

    def test_usage_ratio_returns_alloc_metric_query(self, pg_config, mock_metrics) -> None:
        """Test case 11: usage_ratio returns MetricQuery(key="alloc_PG_NETWORK", ...)."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_NETWORK")

        assert len(queries) == 1
        assert queries[0].key == "alloc_PG_NETWORK"
        assert queries[0].query_expression == "sum by (datname) (pg_stat_database_blks_read)"

    def test_even_split_with_static_source_returns_empty_list(self, static_config, mock_metrics) -> None:
        """Test case 12: static-only source returns []."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=static_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_COMPUTE")

        assert queries == []

    def test_unknown_product_type_returns_empty_list(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        assert handler.get_metrics_for_product_type("NONEXISTENT") == []


class TestHandlerResolveIdentities:
    def test_resolve_identities_prometheus_source_extracts_from_metrics_data(self, pg_config, mock_metrics) -> None:
        """Test case 13: metrics_data={"discovery": [row(datname="alice")]} produces metrics_derived containing "alice"."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        alice_row = make_metric_row("discovery", 1.0, {"datname": "alice"})
        metrics_data = {"discovery": [alice_row]}

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=metrics_data,
            uow=mock_uow,
        )

        identity_ids = list(result.metrics_derived.ids())
        assert "alice" in identity_ids

    def test_resolve_identities_static_source_returns_in_resource_active(self, static_config, mock_metrics) -> None:
        """Test case 14: static identity config returns identity in resource_active."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=static_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=None,
            uow=mock_uow,
        )

        identity_ids = list(result.resource_active.ids())
        assert "team-data" in identity_ids

    def test_resolve_identities_no_metrics_data_produces_empty_metrics_derived(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=None,
            uow=mock_uow,
        )

        assert list(result.metrics_derived.ids()) == []

    def test_resolve_identities_deduplicates_repeated_identity(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        # Same identity appears multiple times in metrics
        alice_row_1 = make_metric_row("discovery", 10.0, {"datname": "alice"})
        alice_row_2 = make_metric_row("discovery", 20.0, {"datname": "alice"})
        metrics_data = {"discovery": [alice_row_1, alice_row_2]}

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=metrics_data,
            uow=mock_uow,
        )

        identity_ids = list(result.metrics_derived.ids())
        assert identity_ids.count("alice") == 1
