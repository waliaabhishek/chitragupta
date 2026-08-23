"""Tests for ConstructedCostInput."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
            "metrics_identifier": "kraft-a-001",
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
    def test_queries_use_the_configured_prometheus_target_selector(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        list(cost_input.gather("tenant-1", day_start, day_end, MagicMock()))

        _, kwargs = mock_metrics_source.query.call_args
        assert kwargs["resource_id_filter"] == "kraft-a-001"
        queries = kwargs["queries"]
        assert {query.resource_label for query in queries} == {"kafka_cluster_id"}
        assert {query.query_expression for query in queries} == {
            "sum(increase(kafka_server_brokertopicmetrics_alltopics_bytesin_total{}[86400s]))",
            "sum(increase(kafka_server_brokertopicmetrics_alltopics_bytesout_total{}[86400s]))",
            "sum(kafka_log_log_size{})",
        }
        by_key = {query.key: query for query in queries}
        assert by_key["cluster_bytes_in"].query_mode == "instant"
        assert by_key["cluster_bytes_out"].query_mode == "instant"
        assert by_key["cluster_storage_bytes"].query_mode == "range"

    def test_non_midnight_refresh_bounds_produce_identical_utc_calendar_days(
        self,
        sample_config,
        mock_metrics_source,
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = sample_metrics_data()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)

        lines = list(
            cost_input.gather(
                "tenant-1",
                datetime(2026, 2, 1, 13, 42, tzinfo=UTC),
                datetime(2026, 2, 2, 13, 42, tzinfo=UTC),
                MagicMock(),
            )
        )

        assert {line.timestamp for line in lines} == {datetime(2026, 2, 1, tzinfo=UTC)}
        query_kwargs = mock_metrics_source.query.call_args.kwargs
        assert query_kwargs["start"] == datetime(2026, 2, 1, tzinfo=UTC)
        assert query_kwargs["end"] == datetime(2026, 2, 2, tzinfo=UTC)

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

    def test_present_zero_storage_family_produces_measured_zero_line(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", 0)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", day_start, day_end, uow))
        storage = next(i for i in items if i.product_type == "SELF_KAFKA_STORAGE")

        assert storage.quantity == Decimal("0")
        assert storage.total_cost == Decimal("0")

    def test_absent_storage_family_omits_the_day_without_fabricating_a_zero_cost_line(
        self, sample_config, mock_metrics_source, day_start, day_end, caplog: pytest.LogCaptureFixture
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)

        assert list(cost_input.gather("tenant-1", day_start, day_end, MagicMock())) == []
        assert "tenant=tenant-1" in caplog.text
        assert "cluster=kafka-cluster-001" in caplog.text
        assert "selector=kraft-a-001" in caplog.text
        assert "date=2026-02-01" in caplog.text
        assert "metric=kafka_log_log_size" in caplog.text

    def test_storage_sample_at_exact_day_end_is_not_used_by_the_prior_day(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [
                MetricRow(
                    timestamp=day_end,
                    metric_key="cluster_storage_bytes",
                    value=gb * 100,
                    labels={"kafka_cluster_id": "kraft-a-001", "broker": "1"},
                )
            ],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)

        assert list(cost_input.gather("tenant-1", day_start, day_end, MagicMock())) == []

    def test_exact_end_storage_sample_belongs_only_to_the_adjacent_utc_day(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824

        def query(
            *,
            queries: list[object],
            start: datetime,
            end: datetime,
            **_: object,
        ) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if key == "cluster_storage_bytes":
                rows = [
                    make_metric_row("cluster_storage_bytes", gb),
                    MetricRow(day_end, "cluster_storage_bytes", gb * 100, {}),
                ]
            else:
                rows = [
                    MetricRow(start, key, gb, {}),
                    MetricRow(end, key, gb, {}),
                ]
            return {key: rows}

        mock_metrics_source.query.side_effect = query
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)

        items = list(cost_input.gather("tenant-1", day_start, day_end + (day_end - day_start), MagicMock()))
        storage_by_day = {item.timestamp: item.quantity for item in items if item.product_type == "SELF_KAFKA_STORAGE"}

        assert storage_by_day == {
            day_start: Decimal("24"),
            day_end: Decimal("2400"),
        }

    def test_successful_empty_admin_inventory_proves_absent_storage_is_zero(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", gb)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", gb)],
            "cluster_storage_bytes": [],
        }
        cost_input = ConstructedCostInput(
            sample_config,
            mock_metrics_source,
            inventory_is_partitionless=lambda: True,
        )

        items = list(cost_input.gather("tenant-1", day_start, day_end, MagicMock()))

        assert len(items) == 4
        storage = next(item for item in items if item.product_type == "SELF_KAFKA_STORAGE")
        assert storage.quantity == Decimal("0")


class TestNetworkCostCalculation:
    def test_present_zero_broker_wide_counters_produce_zero_network_lines(
        self, sample_config, mock_metrics_source, day_start, day_end
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [make_metric_row("cluster_bytes_in", 0)],
            "cluster_bytes_out": [make_metric_row("cluster_bytes_out", 0)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", 0)],
        }

        items = list(
            ConstructedCostInput(sample_config, mock_metrics_source).gather("tenant-1", day_start, day_end, MagicMock())
        )

        network_lines = [item for item in items if item.product_type.startswith("SELF_KAFKA_NETWORK_")]
        assert [(item.quantity, item.total_cost) for item in network_lines] == [
            (Decimal("0"), Decimal("0.00")),
            (Decimal("0"), Decimal("0.00")),
        ]

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
                "metrics_identifier": "kraft-a-001",
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

    @pytest.mark.parametrize(
        ("missing_key", "metric_name"),
        [
            ("cluster_bytes_in", "alltopics_bytesin_total"),
            ("cluster_bytes_out", "alltopics_bytesout_total"),
        ],
    )
    def test_absent_broker_wide_metrics_fail_closed(
        self,
        sample_config,
        mock_metrics_source,
        day_start,
        day_end,
        missing_key: str,
        metric_name: str,
    ) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [] if missing_key == "cluster_bytes_in" else [make_metric_row("cluster_bytes_in", 0)],
            "cluster_bytes_out": []
            if missing_key == "cluster_bytes_out"
            else [make_metric_row("cluster_bytes_out", 0)],
            "cluster_storage_bytes": [make_metric_row("cluster_storage_bytes", 0)],
        }
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        with pytest.raises(MetricsQueryError, match=metric_name):
            list(cost_input.gather("tenant-1", day_start, day_end, uow))

    def test_multi_day_range_generates_lines_per_day(self, sample_config, mock_metrics_source):
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        gb = 1073741824
        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        # Counter range samples are exact UTC day-end evaluations; gauge rows
        # remain on the legacy per-day grid.
        mock_metrics_source.query.return_value = {
            "cluster_bytes_in": [
                MetricRow(
                    timestamp=datetime(2026, 2, 2, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 3, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 4, tzinfo=UTC), metric_key="cluster_bytes_in", value=gb, labels={}
                ),
            ],
            "cluster_bytes_out": [
                MetricRow(
                    timestamp=datetime(2026, 2, 2, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 3, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 4, tzinfo=UTC), metric_key="cluster_bytes_out", value=gb, labels={}
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
        assert mock_metrics_source.query.call_count == 3

    def test_region_override_applied_to_costs(self, mock_metrics_source, day_start, day_end):
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "metrics_identifier": "kraft-a-001",
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
    """3-day batch response with counters evaluated at each UTC day end."""
    gb = 1073741824
    counter_days = [datetime(2026, 2, day, tzinfo=UTC) for day in (2, 3, 4)]
    gauge_days = [datetime(2026, 2, day, 12, tzinfo=UTC) for day in (1, 2, 3)]
    return {
        "cluster_bytes_in": [
            MetricRow(timestamp=d, metric_key="cluster_bytes_in", value=gb * 10, labels={}) for d in counter_days
        ],
        "cluster_bytes_out": [
            MetricRow(timestamp=d, metric_key="cluster_bytes_out", value=gb * 20, labels={}) for d in counter_days
        ],
        "cluster_storage_bytes": [
            MetricRow(timestamp=d, metric_key="cluster_storage_bytes", value=gb * 100, labels={}) for d in gauge_days
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


class TestDailyPrometheusQuery:
    """Daily cost pools use one query per fixed UTC calendar window."""

    def test_gather_calls_query_for_each_utc_day(self, sample_config: object, mock_metrics_source: MagicMock) -> None:
        """Happy path: each calendar day has its own broker-wide instant query."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        mock_metrics_source.query.return_value = _make_batch_metrics_3days()
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        assert mock_metrics_source.query.call_count == 3
        # 4 product types × 3 days = 12 billing lines
        assert len(items) == 12
        # Timestamps must cover each of the 3 days
        timestamps = {item.timestamp for item in items}
        assert datetime(2026, 2, 1, tzinfo=UTC) in timestamps
        assert datetime(2026, 2, 2, tzinfo=UTC) in timestamps
        assert datetime(2026, 2, 3, tzinfo=UTC) in timestamps

    def test_day_with_missing_storage_omits_only_that_day(
        self, sample_config: object, mock_metrics_source: MagicMock, caplog: object
    ) -> None:
        """A missing storage family leaves only that day pending; complete days persist."""
        import logging

        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)

        gb = 1073741824

        def query(*, queries: list[object], **_: object) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if key == "cluster_storage_bytes":
                return {
                    key: [
                        MetricRow(
                            timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC),
                            metric_key=key,
                            value=gb * 100,
                            labels={},
                        ),
                        MetricRow(
                            timestamp=datetime(2026, 2, 3, 12, tzinfo=UTC),
                            metric_key=key,
                            value=gb * 100,
                            labels={},
                        ),
                    ]
                }
            return {
                key: [
                    MetricRow(
                        timestamp=datetime(2026, 2, day, tzinfo=UTC),
                        metric_key=key,
                        value=gb * (10 if key == "cluster_bytes_in" else 20),
                        labels={},
                    )
                    for day in (2, 3, 4)
                ]
            }

        mock_metrics_source.query.side_effect = query
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        with caplog.at_level(logging.WARNING):
            items = list(cost_input.gather("tenant-1", start, end, uow))

        # 8 items: days 1 and 3 produce 4 each; day 2 skipped
        assert len(items) == 8
        assert "date=2026-02-02" in caplog.text

    def test_daily_queries_cover_every_window(self, sample_config: object, mock_metrics_source: MagicMock) -> None:
        """Three complete UTC days each yield a complete cost-line set."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        def query(*, queries: list[object], **_: object) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            timestamps = (
                [datetime(2026, 2, day, 12, tzinfo=UTC) for day in (1, 2, 3)]
                if key == "cluster_storage_bytes"
                else [datetime(2026, 2, day, tzinfo=UTC) for day in (2, 3, 4)]
            )
            return {
                key: [
                    MetricRow(
                        timestamp=timestamp,
                        metric_key=key,
                        value=1073741824,
                        labels={},
                    )
                    for timestamp in timestamps
                ]
            }

        mock_metrics_source.query.side_effect = query
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        assert mock_metrics_source.query.call_count == 3
        # All 3 days produced billing lines
        assert len(items) == 12

    def test_partial_daily_query_failure_skips_only_errored_day(
        self, sample_config: object, mock_metrics_source: MagicMock
    ) -> None:
        """One transient daily query failure leaves other UTC days complete."""
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        start = datetime(2026, 2, 1, tzinfo=UTC)
        end = datetime(2026, 2, 4, tzinfo=UTC)  # 3 days

        def query(
            *,
            queries: list[object],
            start: datetime,
            end: datetime,
            **_: object,
        ) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if key == "cluster_bytes_in" and end - start > timedelta(days=1):
                raise MetricsQueryError("bounded counter response failed")
            if key == "cluster_bytes_in" and start.date() == datetime(2026, 2, 2, tzinfo=UTC).date():
                raise MetricsQueryError("day 2 prometheus down")
            if key == "cluster_storage_bytes":
                return {
                    key: [
                        MetricRow(datetime(2026, 2, day, 12, tzinfo=UTC), key, 1073741824, {})
                        for day in (1, 3)
                        for _ in range(24)
                    ]
                }
            if end - start > timedelta(days=1):
                return {
                    key: [
                        MetricRow(
                            timestamp=datetime(2026, 2, day, tzinfo=UTC),
                            metric_key=key,
                            value=1073741824,
                            labels={},
                        )
                        for day in (2, 3, 4)
                        if start <= datetime(2026, 2, day, tzinfo=UTC) <= end
                    ]
                }
            return {key: [MetricRow(end, key, 1073741824, {})]}

        mock_metrics_source.query.side_effect = query
        cost_input = ConstructedCostInput(sample_config, mock_metrics_source)
        uow = MagicMock()

        items = list(cost_input.gather("tenant-1", start, end, uow))

        # 8 items: days 1 and 3 succeed (4 each), day 2 skipped
        assert len(items) == 8
        assert mock_metrics_source.query.call_count == 6


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


class TestBoundedHistoricalCostAcquisition:
    def test_divisor_step_backfill_uses_three_logical_family_queries_per_bounded_chunk(self) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        config = _historical_config(chunk_days=3)
        source = MagicMock()
        source.query.side_effect = _historical_cluster_response
        first_day = datetime(2026, 2, 1, tzinfo=UTC)

        items = list(
            ConstructedCostInput(config, source).gather(
                "tenant-1",
                first_day,
                first_day + timedelta(days=4),
                MagicMock(),
            )
        )

        assert len(items) == 16
        assert _logical_family_queries(source) == 6
        assert all(
            call.kwargs["end"] - call.kwargs["start"] <= timedelta(days=3) for call in source.query.call_args_list
        )

    def test_failed_counter_chunk_retries_only_its_owned_days_without_discarding_later_days(self) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        source = MagicMock()

        def response(
            *,
            queries: list[object],
            start: datetime,
            end: datetime,
            step: timedelta,
            **kwargs: object,
        ) -> dict[str, list[MetricRow]]:
            if queries[0].key == "cluster_bytes_in" and end - start > timedelta(days=1):
                raise MetricsQueryError("bounded counter response failed")
            return _historical_cluster_response(queries=queries, start=start, end=end, step=step, **kwargs)

        source.query.side_effect = response
        first_day = datetime(2026, 2, 1, tzinfo=UTC)

        items = list(
            ConstructedCostInput(_historical_config(chunk_days=3), source).gather(
                "tenant-1",
                first_day,
                first_day + timedelta(days=4),
                MagicMock(),
            )
        )

        assert {item.timestamp for item in items} == {first_day + timedelta(days=index) for index in range(4)}
        assert _logical_family_queries(source) == 9

    def test_non_divisor_step_preserves_bounded_gauge_request_groups(self) -> None:
        from plugins.self_managed_kafka.cost_input import ConstructedCostInput

        source = MagicMock()
        source.query.side_effect = _historical_cluster_response
        first_day = datetime(2026, 2, 1, tzinfo=UTC)

        items = list(
            ConstructedCostInput(_historical_config(chunk_days=3, metrics_step_seconds=3601), source).gather(
                "tenant-1",
                first_day,
                first_day + timedelta(days=4),
                MagicMock(),
            )
        )

        assert len(items) == 16
        assert _logical_family_queries(source) == 8
        storage_calls = [
            call for call in source.query.call_args_list if call.kwargs["queries"][0].key == "cluster_storage_bytes"
        ]
        assert len(storage_calls) == 4


def _historical_config(*, chunk_days: int, metrics_step_seconds: int = 3600):
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "kafka-cluster-001",
            "metrics_identifier": "kraft-a-001",
            "broker_count": 3,
            "historical_acquisition_chunk_days": chunk_days,
            "metrics_step_seconds": metrics_step_seconds,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "metrics": {"url": "http://prom:9090"},
        }
    )


def _historical_cluster_response(
    *,
    queries: list[object],
    start: datetime,
    end: datetime,
    step: timedelta,
    **_: object,
) -> dict[str, list[MetricRow]]:
    values: dict[str, list[MetricRow]] = {}
    for query in queries:
        timestamps = _grid(start, end, step) if query.key == "cluster_storage_bytes" else _counter_grid(start, end)
        values[query.key] = [
            MetricRow(
                timestamp=timestamp,
                metric_key=query.key,
                value=1073741824,
                labels={},
            )
            for timestamp in timestamps
        ]
    return values


def _counter_grid(start: datetime, end: datetime) -> tuple[datetime, ...]:
    timestamps: list[datetime] = []
    timestamp = start
    while timestamp <= end:
        timestamps.append(timestamp)
        timestamp += timedelta(days=1)
    return tuple(timestamps)


def _grid(start: datetime, end: datetime, step: timedelta) -> tuple[datetime, ...]:
    timestamps: list[datetime] = []
    timestamp = start
    while timestamp <= end:
        timestamps.append(timestamp)
        timestamp += step
    return tuple(timestamps)


def _logical_family_queries(source: MagicMock) -> int:
    return sum(len(call.kwargs["queries"]) for call in source.query.call_args_list)


def test_cost_queries_use_resolved_physical_metric_names_without_changing_daily_semantics() -> None:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.cost_input import _cost_queries
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

    config = SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "billing-cluster-a",
            "metrics_identifier": "kafka-prod",
            "metrics_identifier_label": "deployment",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "metrics": {"url": "http://prometheus:9090"},
            "metric_name_overrides": {
                "kafka_server_brokertopicmetrics_alltopics_bytesin_total": "company_alltopics_in",
                "kafka_server_brokertopicmetrics_alltopics_bytesout_total": "company_alltopics_out",
                "kafka_log_log_size": "company_log_size",
            },
        }
    )

    queries = _cost_queries(ResolvedTelemetryCatalog(config), config.metrics_identifier_label)

    observed = [
        (query.key, query.query_expression, query.query_mode, query.label_keys, query.resource_label)
        for query in queries
    ]
    assert observed == [
        (
            "cluster_bytes_in",
            "sum(increase(company_alltopics_in{}[86400s]))",
            "instant",
            ("deployment",),
            "deployment",
        ),
        (
            "cluster_bytes_out",
            "sum(increase(company_alltopics_out{}[86400s]))",
            "instant",
            ("deployment",),
            "deployment",
        ),
        (
            "cluster_storage_bytes",
            "sum(company_log_size{})",
            "range",
            ("deployment",),
            "deployment",
        ),
    ]
