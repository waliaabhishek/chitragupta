"""Tests for ConstructedCostInput."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricRow


@pytest.fixture
def sample_config():
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "kafka-cluster-001",
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


@pytest.fixture
def day_start():
    return datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def day_end():
    return datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)


def make_metric_row(key: str, value: float, labels: dict | None = None) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels=labels or {},
    )


def sample_metrics_data() -> dict:
    """Sample Prometheus response with realistic data."""
    gb = 1073741824  # 1 GB in bytes
    return {
        "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb * 10)],  # 10 GB
        "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb * 20)],  # 20 GB
        "cluster_storage_bytes": [
            make_metric_row("cluster_storage_bytes", gb * 100)  # 100 GB
        ]
        * 24,  # 24 hourly samples
    }


class TestConstructedCostInputBillingLines:
    def test_generates_four_product_types_per_day(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))

        product_types = {item.product_type for item in items}
        assert product_types == {
            "SELF_KAFKA_COMPUTE",
            "SELF_KAFKA_STORAGE",
            "SELF_KAFKA_NETWORK_INGRESS",
            "SELF_KAFKA_NETWORK_EGRESS",
        }
        assert len(items) == 4

    def test_all_lines_use_cluster_id_as_resource_id(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        for item in items:
            assert item.resource_id == "kafka-cluster-001"

    def test_all_lines_have_correct_ecosystem(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        for item in items:
            assert item.ecosystem == "self_managed_kafka"
            assert item.granularity == "daily"
            assert item.currency == "USD"

    def test_timestamp_is_midnight_utc(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        for item in items:
            assert item.timestamp == datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)


class TestComputeCostCalculation:
    def test_compute_cost_broker_count_times_hours_times_rate(
        self, sample_config, mock_metrics_source, day_start, day_end
    ):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        compute = next(i for i in items if i.product_type == "SELF_KAFKA_COMPUTE")

        # broker_count=3, hours=24, rate=0.10
        expected_qty = Decimal("3") * Decimal("24")  # 72 broker-hours
        expected_cost = expected_qty * Decimal("0.10")  # 7.20
        assert compute.quantity == expected_qty
        assert compute.unit_price == Decimal("0.10")
        assert compute.total_cost == expected_cost


class TestStorageCostCalculation:
    def test_storage_cost_avg_gb_times_hours_times_rate(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb * 100)] * 24,
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        storage = next(i for i in items if i.product_type == "SELF_KAFKA_STORAGE")

        # avg_storage_bytes = 100 GB (all 24 samples are 100 GB)
        # quantity = 100 GB * 24 hours = 2400 GB-hours
        assert storage.quantity == Decimal("100") * Decimal("24")
        assert storage.unit_price == Decimal("0.0001")

    def test_storage_zero_when_no_storage_data(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        storage = next(i for i in items if i.product_type == "SELF_KAFKA_STORAGE")

        assert storage.quantity == Decimal("0")
        assert storage.total_cost == Decimal("0")


class TestNetworkCostCalculation:
    def test_ingress_cost_bytes_to_gb_times_rate(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb * 10)],  # 10 GB
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        ingress = next(i for i in items if i.product_type == "SELF_KAFKA_NETWORK_INGRESS")

        assert ingress.quantity == Decimal("10")
        assert ingress.unit_price == Decimal("0.01")
        assert ingress.total_cost == Decimal("0.10")

    def test_egress_cost_bytes_to_gb_times_rate(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb * 20)],  # 20 GB
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        egress = next(i for i in items if i.product_type == "SELF_KAFKA_NETWORK_EGRESS")

        assert egress.quantity == Decimal("20")
        assert egress.unit_price == Decimal("0.02")
        assert egress.total_cost == Decimal("0.40")


class TestEdgeCases:
    def test_prometheus_query_failure_skips_billing_period(
        self, sample_config, mock_metrics_source, day_start, day_end
    ):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.side_effect = MetricsQueryError("Prometheus unavailable")
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items == []

    def test_empty_metrics_skips_billing_period(self, sample_config, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [],
            "cluster_bytes_out": [],
            "cluster_storage_bytes": [],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        assert items == []

    def test_multi_day_range_generates_lines_per_day(self, sample_config, mock_metrics_source):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        items = list(cost_input.gather("tenant-1", start, end, uow))
        # 4 product types × 3 days = 12 items
        assert len(items) == 12
        # Prometheus queried once per day
        assert mock_metrics_source.query.call_count == 3

    def test_region_override_applied_to_costs(self, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "region": "us-west-2",
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                    "region_overrides": {"us-west-2": {"compute_hourly_rate": "0.08"}},
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb)],
        }
        cost_input = ConstructedCostInput(config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        compute = next(i for i in items if i.product_type == "SELF_KAFKA_COMPUTE")
        assert compute.unit_price == Decimal("0.08")
