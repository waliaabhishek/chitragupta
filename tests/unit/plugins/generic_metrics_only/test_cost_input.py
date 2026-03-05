"""Tests for GenericConstructedCostInput."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricRow


def make_metric_row(key: str, value: float, labels: dict | None = None) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels=labels or {},
    )


_GIB = 1073741824  # 1 GiB in bytes


@pytest.fixture
def day_start() -> datetime:
    return datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def day_end() -> datetime:
    return datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def mock_metrics_source() -> MagicMock:
    return MagicMock()


def make_config(cost_types: list, identity_source: dict | None = None):
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

    return GenericMetricsOnlyConfig.model_validate(
        {
            "ecosystem_name": "self_managed_postgres",
            "cluster_id": "pg-prod-1",
            "metrics": {"url": "http://prom:9090"},
            "identity_source": identity_source
            or {
                "source": "static",
            },
            "cost_types": cost_types,
        }
    )


class TestFixedCostQuantity:
    def test_fixed_cost_yields_count_times_hours_without_prometheus(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 15: fixed count=3 → quantity = Decimal("3") * hours, no Prometheus query."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 3},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        assert len(items) == 1
        assert items[0].product_type == "PG_COMPUTE"
        # 3 instances * 24 hours
        assert items[0].quantity == Decimal("3") * Decimal("24")
        # Prometheus must NOT be queried for fixed-only config
        mock_metrics_source.query.assert_not_called()

    def test_fixed_cost_total_cost_is_quantity_times_rate(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 3},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items[0].total_cost == items[0].quantity * Decimal("2.50")


class TestStorageGibCostQuantity:
    def test_storage_gib_yields_avg_gib_times_hours(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 16: storage_gib → quantity = avg_gib * hours."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_STORAGE",
                    "product_category": "postgres",
                    "rate": "0.0001",
                    "cost_quantity": {"type": "storage_gib", "query": "sum(pg_database_size_bytes)"},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        # 24 hourly samples, each reporting 100 GiB
        storage_rows = [make_metric_row("cost_PG_STORAGE", float(_GIB * 100))] * 24
        mock_metrics_source.query.return_value = {"cost_PG_STORAGE": storage_rows}

        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        assert len(items) == 1
        # avg_gib = 100, hours = 24, quantity = 100 * 24 = 2400
        assert items[0].quantity == Decimal("100") * Decimal("24")

    def test_storage_gib_zero_when_no_rows(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_STORAGE",
                    "product_category": "postgres",
                    "rate": "0.0001",
                    "cost_quantity": {"type": "storage_gib", "query": "sum(pg_database_size_bytes)"},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        mock_metrics_source.query.return_value = {"cost_PG_STORAGE": []}

        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        # No data → empty result (skipped)
        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items == []


class TestNetworkGibCostQuantity:
    def test_network_gib_yields_total_gib_no_hours_multiplier(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 17: network_gib → quantity = total_gib (no multiplication by hours)."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
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
                }
            ]
        )
        # Total bytes transferred = 10 GiB
        network_row = make_metric_row("cost_PG_NETWORK", float(_GIB * 10))
        mock_metrics_source.query.return_value = {"cost_PG_NETWORK": [network_row]}

        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        assert len(items) == 1
        # 10 GiB total, no hours multiplier
        assert items[0].quantity == Decimal("10")


class TestGatherDayErrorHandling:
    def test_prometheus_error_skips_billing_period(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 18: MetricsQueryError → billing period skipped (empty result)."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_STORAGE",
                    "product_category": "postgres",
                    "rate": "0.0001",
                    "cost_quantity": {"type": "storage_gib", "query": "sum(pg_database_size_bytes)"},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        mock_metrics_source.query.side_effect = MetricsQueryError("Prometheus unavailable")

        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items == []

    def test_fixed_only_config_emits_lines_without_querying_prometheus(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 19: empty _cost_queries (fixed-only) → billing lines without Prometheus query."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                },
                {
                    "name": "PG_REPLICA",
                    "product_category": "postgres",
                    "rate": "1.00",
                    "cost_quantity": {"type": "fixed", "count": 1},
                    "allocation_strategy": "even_split",
                },
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        assert cost_input._cost_queries == []  # no queries for fixed-only

        uow = MagicMock()
        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        assert len(items) == 2
        mock_metrics_source.query.assert_not_called()


class TestBillingLineItemEcosystem:
    def test_billing_line_ecosystem_equals_ecosystem_name_not_registry_key(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        """Test case 20: BillingLineItem.ecosystem == ecosystem_name (not "generic_metrics_only")."""
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        assert len(items) == 1
        assert items[0].ecosystem == "self_managed_postgres"
        assert items[0].ecosystem != "generic_metrics_only"

    def test_billing_line_resource_id_equals_cluster_id(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items[0].resource_id == "pg-prod-1"

    def test_billing_line_timestamp_is_midnight_utc(
        self, mock_metrics_source: MagicMock, day_start: datetime, day_end: datetime
    ) -> None:
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items[0].timestamp == datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)

    def test_multi_day_range_generates_one_line_per_cost_type_per_day(self, mock_metrics_source: MagicMock) -> None:
        from plugins.generic_metrics_only.cost_input import GenericConstructedCostInput

        config = make_config(
            [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ]
        )
        cost_input = GenericConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        items = list(cost_input.gather("tenant-1", start, end, uow))
        assert len(items) == 3  # 1 cost type * 3 days
        mock_metrics_source.query.assert_not_called()
