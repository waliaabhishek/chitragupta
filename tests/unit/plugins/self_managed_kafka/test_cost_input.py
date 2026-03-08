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


class TestConstructedCostInputStepParam:
    """task-013: ConstructedCostInput must use metrics_step_seconds from config."""

    def test_gather_day_uses_step_from_config(
        self, mock_metrics_source: MagicMock, day_start: object, day_end: object
    ) -> None:
        from datetime import timedelta

        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "metrics_step_seconds": 1800,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
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
        list(cost_input.gather("tenant-1", day_start, day_end, uow))

        _, call_kwargs = mock_metrics_source.query.call_args
        assert call_kwargs["step"] == timedelta(seconds=1800)

    def test_gather_day_default_step_is_one_hour(
        self, sample_config: object, mock_metrics_source: MagicMock, day_start: object, day_end: object
    ) -> None:
        from datetime import timedelta

        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", gb)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()
        list(cost_input.gather("tenant-1", day_start, day_end, uow))

        _, call_kwargs = mock_metrics_source.query.call_args
        assert call_kwargs["step"] == timedelta(hours=1)


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
        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        # Batch query returns rows with timestamps spread across all 3 days
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 2, 12, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 3, 12, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
            ],
            "cluster_bytes_out": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 2, 12, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 3, 12, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
                ),
            ],
            "cluster_storage_bytes": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC),
                    metric_key="cluster_storage_bytes",
                    value=gb,
                    labels={},
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 2, 12, tzinfo=UTC),
                    metric_key="cluster_storage_bytes",
                    value=gb,
                    labels={},
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 3, 12, tzinfo=UTC),
                    metric_key="cluster_storage_bytes",
                    value=gb,
                    labels={},
                ),
            ],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))
        # 4 product types × 3 days = 12 items
        assert len(items) == 12
        # NEW: Prometheus queried ONCE for the full range (batch query), not once per day
        assert mock_metrics_source.query.call_count == 1

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


# ---------------------------------------------------------------------------
# Helpers for batch Prometheus tests (task-039)
# ---------------------------------------------------------------------------


def _make_batch_metrics_3days() -> dict:
    """3-day batch response: rows with timestamps in Feb 1, Feb 2, Feb 3."""
    gb = 1073741824
    days = [
        datetime(2026, 2, 1, 12, tzinfo=UTC),
        datetime(2026, 2, 2, 12, tzinfo=UTC),
        datetime(2026, 2, 3, 12, tzinfo=UTC),
    ]
    return {
        "cluster_bytes_in": [
            MetricRow(timestamp=d, metric_key="cluster_bytes_in", value=gb * 10, labels={}) for d in days
        ],
        "cluster_bytes_out": [
            MetricRow(timestamp=d, metric_key="cluster_bytes_out", value=gb * 20, labels={}) for d in days
        ],
        "cluster_storage_bytes": [
            MetricRow(timestamp=d, metric_key="cluster_storage_bytes", value=gb * 100, labels={}) for d in days
        ],
    }


def _make_single_day_metrics(ts: datetime) -> dict:
    """Per-day query response with all rows timestamped at ts (existing behaviour)."""
    gb = 1073741824
    return {
        "cluster_bytes_in": [MetricRow(timestamp=ts, metric_key="cluster_bytes_in", value=gb, labels={})],
        "cluster_bytes_out": [MetricRow(timestamp=ts, metric_key="cluster_bytes_out", value=gb, labels={})],
        "cluster_storage_bytes": [MetricRow(timestamp=ts, metric_key="cluster_storage_bytes", value=gb, labels={})],
    }


class TestBatchPrometheusQuery:
    """task-039: gather() issues ONE batch query; falls back to per-day on MetricsQueryError."""

    def test_gather_calls_query_exactly_once_for_multi_day_range(
        self, sample_config: object, mock_metrics_source: MagicMock
    ) -> None:
        """Happy path: single batch query covers full range, billing lines split by day."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        mock_metrics_source.query.return_value = _make_batch_metrics_3days()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        # Exactly ONE query for the full 3-day range
        assert mock_metrics_source.query.call_count == 1
        # 4 product types × 3 days = 12 billing lines
        assert len(items) == 12
        # Timestamps must cover each of the 3 days
        timestamps = {item.timestamp for item in items}
        assert datetime(2026, 2, 1, tzinfo=UTC) in timestamps
        assert datetime(2026, 2, 2, tzinfo=UTC) in timestamps
        assert datetime(2026, 2, 3, tzinfo=UTC) in timestamps

    def test_day_with_no_data_in_batch_logs_warning_and_skips(
        self, sample_config: object, mock_metrics_source: MagicMock, caplog: object
    ) -> None:
        """Day 2 slice is empty → warning logged; days 1 and 3 produce billing lines."""
        import logging

        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)

        gb = 1073741824
        # Only rows for Feb 1 and Feb 3 — no rows in the Feb 2 window
        days_with_data = [
            datetime(2026, 2, 1, 12, tzinfo=UTC),
            datetime(2026, 2, 3, 12, tzinfo=UTC),
        ]
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [
                MetricRow(timestamp=d, metric_key="cluster_bytes_in", value=gb * 10, labels={}) for d in days_with_data
            ],
            "cluster_bytes_out": [
                MetricRow(timestamp=d, metric_key="cluster_bytes_out", value=gb * 20, labels={}) for d in days_with_data
            ],
            "cluster_storage_bytes": [
                MetricRow(timestamp=d, metric_key="cluster_storage_bytes", value=gb * 100, labels={})
                for d in days_with_data
            ],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        with caplog.at_level(logging.WARNING):
            items = list(cost_input.gather("tenant-1", start, end, uow))

        # 8 items: days 1 and 3 produce 4 each; day 2 skipped
        assert len(items) == 8
        # Warning logged for the empty day (Feb 2)
        assert "2026-02-02" in caplog.text

    def test_batch_query_error_triggers_per_day_fallback(
        self, sample_config: object, mock_metrics_source: MagicMock
    ) -> None:
        """MetricsQueryError on batch → fallback to per-day queries; all days succeed."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        day_data = _make_single_day_metrics(datetime(2026, 2, 1, 12, tzinfo=UTC))
        # First call (batch) raises; next 3 (per-day) succeed
        mock_metrics_source.query.side_effect = [
            MetricsQueryError("batch unavailable"),
            day_data,
            day_data,
            day_data,
        ]
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        # 1 failed batch attempt + 3 per-day fallback calls = 4 total
        assert mock_metrics_source.query.call_count == 1 + 3
        # All 3 days produced billing lines
        assert len(items) == 12

    def test_fallback_partial_day_failure_skips_only_errored_day(
        self, sample_config: object, mock_metrics_source: MagicMock
    ) -> None:
        """During per-day fallback, one day's query raises → only that day skipped."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        day1_data = _make_single_day_metrics(datetime(2026, 2, 1, 12, tzinfo=UTC))
        day3_data = _make_single_day_metrics(datetime(2026, 2, 3, 12, tzinfo=UTC))

        # Batch fails, day 1 succeeds, day 2 fails, day 3 succeeds
        mock_metrics_source.query.side_effect = [
            MetricsQueryError("batch unavailable"),
            day1_data,
            MetricsQueryError("day 2 prometheus down"),
            day3_data,
        ]
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        # 8 items: days 1 and 3 succeed (4 each), day 2 skipped
        assert len(items) == 8
        assert mock_metrics_source.query.call_count == 4


class TestSliceMetricsForDay:
    """task-039: _slice_metrics_for_day uses half-open interval [day_start, day_end)."""

    def test_row_at_day_end_boundary_excluded_from_current_day(self) -> None:
        """A row timestamped exactly at day_end belongs to the next day, not this one."""
        from datetime import timedelta

        from plugins.self_managed_kafka.cost_input import _slice_metrics_for_day  # noqa: PLC0415

        day_start = datetime(2026, 2, 1, tzinfo=UTC)
        day_end = datetime(2026, 2, 2, tzinfo=UTC)

        row_at_boundary = MetricRow(timestamp=day_end, metric_key="cluster_bytes_in", value=1.0, labels={})
        row_before_boundary = MetricRow(
            timestamp=day_end - timedelta(seconds=1),
            metric_key="cluster_bytes_in",
            value=2.0,
            labels={},
        )

        metrics = {"cluster_bytes_in": [row_at_boundary, row_before_boundary]}
        sliced = _slice_metrics_for_day(metrics, day_start, day_end)

        assert row_before_boundary in sliced["cluster_bytes_in"]
        assert row_at_boundary not in sliced["cluster_bytes_in"]


class TestDayStarts:
    """task-039: _day_starts yields (day_start, day_end) tuples covering the full range."""

    def test_partial_final_day_yields_shorter_second_tuple(self) -> None:
        """1.5-day range yields exactly 2 tuples; second is shorter than 24 h."""
        from datetime import timedelta

        from plugins.self_managed_kafka.cost_input import _day_starts  # noqa: PLC0415

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 2, 12, tzinfo=UTC)  # 36 hours total

        result = list(_day_starts(start, end))

        assert len(result) == 2
        assert result[0] == (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 2, tzinfo=UTC))
        assert result[1] == (datetime(2026, 2, 2, tzinfo=UTC), datetime(2026, 2, 2, 12, tzinfo=UTC))
        # Second window is less than a full day
        assert result[1][1] - result[1][0] < timedelta(hours=24)
