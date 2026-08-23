"""Tests for self-managed Kafka topic-attribution metric evidence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.models import CoreBillingLineItem, MetricRow


def _billing_line(product_type: str, quantity: Decimal, total_cost: Decimal) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="billing-cluster-a",
        product_category="kafka",
        product_type=product_type,
        quantity=quantity,
        unit_price=Decimal("1"),
        total_cost=total_cost,
        granularity="daily",
        currency="USD",
        metadata={},
    )


def _provider(
    metrics_source: MagicMock,
    *,
    inventory_is_partitionless: bool = False,
    compute_policy: str = "shared_even_v1",
) -> Any:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.overlays.topic_attribution import SelfManagedKafkaTopicAttributionProvider

    config = SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "billing-cluster-a",
            "metrics_identifier": "kraft-a-001",
            "broker_count": 3,
            "metrics": {"url": "http://prometheus:9090"},
            "cost_model": {
                "compute_hourly_rate": "0.10",
                "storage_per_gib_hourly": "0.0001",
                "network_ingress_per_gib": "0.01",
                "network_egress_per_gib": "0.02",
            },
            "topic_attribution": {"enabled": True, "compute_policy": compute_policy},
        }
    )
    return SelfManagedKafkaTopicAttributionProvider(
        config=config,
        metrics_source=metrics_source,
        inventory_is_partitionless=lambda: inventory_is_partitionless,
    )


@pytest.mark.parametrize(
    ("product_type", "metric_key", "metric_name"),
    [
        ("SELF_KAFKA_NETWORK_INGRESS", "topic_bytes_in", "kafka_server_brokertopicmetrics_bytesin_total"),
        ("SELF_KAFKA_NETWORK_EGRESS", "topic_bytes_out", "kafka_server_brokertopicmetrics_bytesout_total"),
    ],
)
def test_network_direction_uses_its_matching_topic_metric_and_fixed_utc_window(
    product_type: str,
    metric_key: str,
    metric_name: str,
) -> None:
    source = MagicMock()
    source.query.return_value = {
        metric_key: [
            MetricRow(
                timestamp=datetime(2026, 2, 2, tzinfo=UTC),
                metric_key=metric_key,
                value=1073741824,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
            )
        ]
    }
    provider = _provider(source)

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line(product_type, Decimal("1"), Decimal("10"))],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("orders", "bytes_ratio", Decimal("10.0000")),
    ]
    query_kwargs = source.query.call_args.kwargs
    assert query_kwargs["start"] == datetime(2026, 2, 1, tzinfo=UTC)
    assert query_kwargs["end"] == datetime(2026, 2, 2, tzinfo=UTC)
    query = query_kwargs["queries"][0]
    assert query.query_expression == f"sum by (topic) (increase({metric_name}{{}}[86400s]))"
    assert query.resource_label == "kafka_cluster_id"
    assert query_kwargs["resource_id_filter"] == "kraft-a-001"


def test_compute_shared_policy_allocates_across_complete_active_topic_universe() -> None:
    provider = _provider(MagicMock())

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_COMPUTE", Decimal("24"), Decimal("9"))],
        resource_topics=frozenset({"orders", "__consumer_offsets", "payments"}),
        metrics_step=timedelta(hours=1),
    )

    assert {(row.topic_name, row.attribution_method, row.amount) for row in result.rows} == {
        ("orders", "shared_even_v1", Decimal("3.0000")),
        ("__consumer_offsets", "shared_even_v1", Decimal("3.0000")),
        ("payments", "shared_even_v1", Decimal("3.0000")),
    }


def test_compute_disabled_preserves_the_full_cost_as_an_explicit_outcome() -> None:
    provider = _provider(MagicMock(), compute_policy="disabled")

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_COMPUTE", Decimal("24"), Decimal("9"))],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("__UNATTRIBUTED__", "compute_policy_disabled", Decimal("9.0000")),
    ]


def test_compute_shared_policy_without_an_active_topic_keeps_the_cost_unattributed() -> None:
    provider = _provider(MagicMock())

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_COMPUTE", Decimal("24"), Decimal("9"))],
        resource_topics=frozenset(),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("__UNATTRIBUTED__", "no_topic_inventory", Decimal("9.0000")),
    ]


def test_measured_query_failure_is_retryable_without_terminal_rows() -> None:
    from core.metrics.protocol import MetricsQueryError

    source = MagicMock()
    source.query.side_effect = MetricsQueryError("unavailable")
    line = _billing_line("SELF_KAFKA_NETWORK_INGRESS", Decimal("1"), Decimal("10"))

    result = _provider(source).attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[line],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert result.rows == ()
    assert result.retry_lines == (line,)


@pytest.mark.parametrize("value", [-1, float("nan")])
def test_invalid_topic_evidence_becomes_an_explicit_terminal_outcome(value: float) -> None:
    source = MagicMock()
    source.query.return_value = {
        "topic_bytes_in": [
            MetricRow(
                timestamp=datetime(2026, 2, 2, tzinfo=UTC),
                metric_key="topic_bytes_in",
                value=value,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
            )
        ]
    }

    result = _provider(source).attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_NETWORK_INGRESS", Decimal("1"), Decimal("10"))],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("__UNATTRIBUTED__", "invalid_topic_telemetry", Decimal("10.0000")),
    ]


def test_invalid_network_evidence_keeps_later_valid_topic_for_compute() -> None:
    source = MagicMock()
    source.query.return_value = {
        "topic_bytes_in": [
            MetricRow(
                timestamp=datetime(2026, 2, 2, tzinfo=UTC),
                metric_key="topic_bytes_in",
                value=-1,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "invalid"},
            ),
            MetricRow(
                timestamp=datetime(2026, 2, 2, tzinfo=UTC),
                metric_key="topic_bytes_in",
                value=1073741824,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders"},
            ),
        ]
    }

    result = _provider(source).attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[
            _billing_line("SELF_KAFKA_NETWORK_INGRESS", Decimal("1"), Decimal("10")),
            _billing_line("SELF_KAFKA_COMPUTE", Decimal("24"), Decimal("9")),
        ],
        resource_topics=frozenset(),
        metrics_step=timedelta(hours=1),
    )

    assert {(row.product_type, row.topic_name, row.attribution_method, row.amount) for row in result.rows} == {
        ("SELF_KAFKA_NETWORK_INGRESS", "__UNATTRIBUTED__", "invalid_topic_telemetry", Decimal("10.0000")),
        ("SELF_KAFKA_COMPUTE", "orders", "shared_even_v1", Decimal("9.0000")),
    }


def test_storage_rows_filter_the_exact_end_boundary_before_topic_averaging() -> None:
    source = MagicMock()
    source.query.return_value = {
        "topic_storage_bytes": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=1073741824,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders", "partition": "0"},
            ),
            MetricRow(
                timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=1073741824,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "payments", "partition": "0"},
            ),
            MetricRow(
                timestamp=datetime(2026, 2, 2, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=10737418240,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders", "partition": "0"},
            ),
        ]
    }
    provider = _provider(source)

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_STORAGE", Decimal("48"), Decimal("24"))],
        resource_topics=frozenset({"orders", "payments"}),
        metrics_step=timedelta(hours=1),
    )

    assert {(row.topic_name, row.attribution_method, row.amount) for row in result.rows} == {
        ("orders", "retained_bytes_ratio", Decimal("12.0000")),
        ("payments", "retained_bytes_ratio", Decimal("12.0000")),
    }


def test_storage_average_uses_every_in_window_timestamp_with_missing_topics_as_zero() -> None:
    source = MagicMock()
    source.query.return_value = {
        "topic_storage_bytes": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, 6, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=2147483648,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders", "partition": "0"},
            ),
            MetricRow(
                timestamp=datetime(2026, 2, 1, 18, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=2147483648,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "payments", "partition": "0"},
            ),
        ]
    }

    result = _provider(source).attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_STORAGE", Decimal("48"), Decimal("24"))],
        resource_topics=frozenset({"orders", "payments"}),
        metrics_step=timedelta(hours=1),
    )

    assert {(row.topic_name, row.amount) for row in result.rows} == {
        ("orders", Decimal("12.0000")),
        ("payments", Decimal("12.0000")),
    }


@pytest.mark.parametrize("value", [-1, float("inf")])
def test_invalid_raw_storage_sample_becomes_an_explicit_terminal_outcome(value: float) -> None:
    source = MagicMock()
    source.query.return_value = {
        "topic_storage_bytes": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, 12, tzinfo=UTC),
                metric_key="topic_storage_bytes",
                value=value,
                labels={"kafka_cluster_id": "kraft-a-001", "broker": "1", "topic": "orders", "partition": "0"},
            )
        ]
    }

    result = _provider(source).attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_STORAGE", Decimal("24"), Decimal("10"))],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("__UNATTRIBUTED__", "invalid_topic_telemetry", Decimal("10.0000")),
    ]


def test_absent_storage_evidence_is_valid_only_with_current_partitionless_inventory_proof() -> None:
    source = MagicMock()
    source.query.return_value = {"topic_storage_bytes": []}
    provider = _provider(source, inventory_is_partitionless=True)

    result = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_STORAGE", Decimal("0"), Decimal("0"))],
        resource_topics=frozenset(),
        metrics_step=timedelta(hours=1),
    )

    assert [(row.topic_name, row.attribution_method, row.amount) for row in result.rows] == [
        ("__UNATTRIBUTED__", "zero_cluster_usage", Decimal("0.0000")),
    ]


def test_provider_exposes_bounded_chunk_capability_without_changing_the_one_day_interface() -> None:
    from core.engine.topic_attribution_provider import ChunkedTopicEvidenceProvider

    source = MagicMock()
    provider = _provider(source)
    first_day = datetime(2026, 2, 1, tzinfo=UTC)
    windows = tuple((first_day + timedelta(days=index), first_day + timedelta(days=index + 1)) for index in range(31))

    assert isinstance(provider, ChunkedTopicEvidenceProvider)
    assert tuple(provider.iter_evidence_chunks(windows)) == (
        windows[:5],
        windows[5:10],
        windows[10:15],
        windows[15:20],
        windows[20:25],
        windows[25:30],
        windows[30:],
    )


def test_prepared_chunk_reuses_exact_day_evidence_without_another_topic_query() -> None:
    source = MagicMock()

    def query(
        *,
        queries: list[object],
        start: datetime,
        end: datetime,
        step: timedelta,
        **_: object,
    ) -> dict[str, list[MetricRow]]:
        rows: dict[str, list[MetricRow]] = {}
        for metric in queries:
            if metric.key == "topic_storage_bytes":
                rows[metric.key] = [
                    MetricRow(
                        timestamp=timestamp,
                        metric_key=metric.key,
                        value=1073741824,
                        labels={"topic": "orders", "partition": "0"},
                    )
                    for timestamp in _query_grid(start, end, step)
                ]
            else:
                rows[metric.key] = [
                    MetricRow(
                        timestamp=end,
                        metric_key=metric.key,
                        value=1073741824,
                        labels={"topic": "orders"},
                    )
                ]
        return rows

    source.query.side_effect = query
    provider = _provider(source)
    day_start = datetime(2026, 2, 1, tzinfo=UTC)
    window = (day_start, day_start + timedelta(days=1))

    provider.prepare_evidence_chunk((window,), timedelta(hours=1))
    calls_after_preparation = source.query.call_count
    outcome = provider.attribute_cluster(
        tenant_id="tenant-1",
        cluster_resource_id="billing-cluster-a",
        env_id="cluster-a",
        billing_lines=[_billing_line("SELF_KAFKA_NETWORK_INGRESS", Decimal("1"), Decimal("10"))],
        resource_topics=frozenset({"orders"}),
        metrics_step=timedelta(hours=1),
    )

    assert source.query.call_count == calls_after_preparation == 3
    assert [(row.topic_name, row.amount) for row in outcome.rows] == [("orders", Decimal("10.0000"))]


def test_non_divisor_chunk_uses_one_counter_request_per_family_and_one_gauge_group_per_day() -> None:
    source = MagicMock()

    def query(
        *,
        queries: list[object],
        start: datetime,
        end: datetime,
        step: timedelta,
        **_: object,
    ) -> dict[str, list[MetricRow]]:
        return {
            metric.key: [
                MetricRow(
                    timestamp=timestamp if metric.key == "topic_storage_bytes" else end,
                    metric_key=metric.key,
                    value=1073741824,
                    labels={"topic": "orders", "partition": "0"},
                )
                for timestamp in _query_grid(start, end, step)
            ]
            for metric in queries
        }

    source.query.side_effect = query
    provider = _provider(source)
    first_day = datetime(2026, 2, 1, tzinfo=UTC)
    windows = tuple((first_day + timedelta(days=index), first_day + timedelta(days=index + 1)) for index in range(3))

    provider.prepare_evidence_chunk(windows, timedelta(seconds=3601))

    assert source.query.call_count == 5
    assert all(call.kwargs["end"] - call.kwargs["start"] <= timedelta(days=30) for call in source.query.call_args_list)


def _query_grid(start: datetime, end: datetime, step: timedelta) -> tuple[datetime, ...]:
    timestamps: list[datetime] = []
    timestamp = start
    while timestamp <= end:
        timestamps.append(timestamp)
        timestamp += step
    return tuple(timestamps)
