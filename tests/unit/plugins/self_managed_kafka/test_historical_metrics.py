"""Tests for bounded historical self-managed Kafka metric acquisition."""

from __future__ import annotations

import gc
import weakref
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Literal
from unittest.mock import MagicMock

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery, MetricRow
from plugins.self_managed_kafka.historical_metrics import (
    bounded_window_chunks,
    collect_daily_evidence,
    iter_daily_evidence,
)


def _windows(days: int) -> tuple[tuple[datetime, datetime], ...]:
    start = datetime(2026, 2, 1, tzinfo=UTC)
    return tuple((start + timedelta(days=index), start + timedelta(days=index + 1)) for index in range(days))


def _query(key: str, mode: Literal["instant", "range"] = "instant") -> MetricQuery:
    return MetricQuery(
        key=key,
        query_expression=key,
        label_keys=(),
        resource_label=None,
        query_mode=mode,
    )


class _CountingRows(Sequence[MetricRow]):
    def __init__(self, rows: Sequence[MetricRow]) -> None:
        self._rows = tuple(rows)
        self.iterations = 0

    def __getitem__(self, index: int) -> MetricRow:
        return self._rows[index]

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[MetricRow]:
        self.iterations += 1
        return iter(self._rows)


def test_counter_rows_are_owned_only_by_exact_day_end_and_keep_zero_distinct_from_missing() -> None:
    windows = _windows(3)
    query = _query("counter")
    calls: list[tuple[datetime, datetime, timedelta]] = []

    def response(*, queries: list[MetricQuery], start: datetime, end: datetime, step: timedelta, **_: object):
        calls.append((start, end, step))
        return {
            "counter": [
                MetricRow(windows[0][0], "counter", 11.0),
                MetricRow(windows[0][0] + timedelta(hours=12), "counter", 12.0),
                MetricRow(windows[0][1], "counter", 13.0),
                MetricRow(windows[1][0] + timedelta(hours=12), "counter", 21.0),
                MetricRow(windows[1][1], "counter", 0.0),
                # The final day's endpoint is intentionally absent.
            ]
        }

    source = MagicMock()
    source.query.side_effect = response

    evidence = collect_daily_evidence(
        source,
        [query],
        windows,
        step=timedelta(hours=1),
        chunk_days=5,
    )

    assert calls == [(windows[0][1], windows[-1][1], timedelta(days=1))]
    assert [row.value for row in evidence[windows[0]]["counter"]] == [13.0]
    assert [row.value for row in evidence[windows[1]]["counter"]] == [0.0]
    assert evidence[windows[2]]["counter"] == []


def test_historical_range_conversion_preserves_alias_normalization_metadata() -> None:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.historical_metrics import _range_query
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
            "label_name_overrides": {"kafka_log_log_size": {"broker": "node"}},
        }
    )
    query = ResolvedTelemetryCatalog(config).bind_query(
        canonical_family="kafka_log_log_size",
        key="cluster_storage_bytes",
        query_expression="sum(kafka_log_log_size{})",
        canonical_label_keys=("broker",),
        passthrough_label_keys=("deployment",),
        resource_label="deployment",
        query_mode="instant",
    )

    ranged = _range_query(query)

    assert ranged.query_mode == "range"
    assert ranged.label_keys == ("node", "deployment")
    assert ranged.resource_label == "deployment"
    assert ranged.metadata == query.metadata


@pytest.mark.parametrize("mode", ["instant", "range"])
def test_each_bounded_response_is_partitioned_in_one_row_pass(mode: Literal["instant", "range"]) -> None:
    windows = _windows(5)
    query = _query("metric", mode=mode)
    if mode == "instant":
        rows = _CountingRows([MetricRow(window[1], "metric", float(index)) for index, window in enumerate(windows)])
    else:
        step = timedelta(hours=6)
        timestamp = windows[0][0]
        gauge_rows: list[MetricRow] = []
        while timestamp <= windows[-1][1]:
            gauge_rows.append(MetricRow(timestamp, "metric", float(timestamp.timestamp())))
            timestamp += step
        rows = _CountingRows(gauge_rows)

    source = MagicMock()
    source.query.return_value = {"metric": rows}
    evidence = collect_daily_evidence(
        source,
        [query],
        windows,
        step=timedelta(hours=6),
        chunk_days=5,
    )

    assert rows.iterations == 1
    assert all("metric" in evidence[window] for window in windows)


def test_counter_family_fallback_isolated_from_a_successful_sibling_family() -> None:
    windows = _windows(2)
    failing_query = _query("counter_failed")
    successful_query = _query("counter_ok")
    calls: list[tuple[str, datetime, datetime]] = []

    def response(*, queries: list[MetricQuery], start: datetime, end: datetime, step: timedelta, **_: object):
        key = queries[0].key
        calls.append((key, start, end))
        if key == "counter_failed" and (start, end) == (windows[0][1], windows[1][1]) and step == timedelta(days=1):
            raise MetricsQueryError("range unavailable")
        if key == "counter_ok" and (start, end) == (windows[0][1], windows[1][1]):
            return {
                key: [
                    MetricRow(windows[0][1], key, 1.0),
                    MetricRow(windows[1][1], key, 1.0),
                ]
            }
        return {key: [MetricRow(end, key, 1.0)]}

    source = MagicMock()
    source.query.side_effect = response

    evidence = collect_daily_evidence(
        source,
        [failing_query, successful_query],
        windows,
        step=timedelta(hours=1),
        chunk_days=5,
    )

    assert calls == [
        ("counter_failed", windows[0][1], windows[1][1]),
        ("counter_failed", windows[0][0], windows[0][1]),
        ("counter_failed", windows[1][0], windows[1][1]),
        ("counter_ok", windows[0][1], windows[1][1]),
    ]
    assert all(len(evidence[window]["counter_failed"]) == 1 for window in windows)
    assert all(len(evidence[window]["counter_ok"]) == 1 for window in windows)


def test_gauge_family_fallback_preserves_each_half_open_daily_grid() -> None:
    windows = _windows(2)
    query = _query("gauge", mode="range")
    calls: list[tuple[datetime, datetime]] = []

    def response(*, start: datetime, end: datetime, step: timedelta, **_: object):
        calls.append((start, end))
        if len(calls) == 1:
            raise MetricsQueryError("range unavailable")
        timestamps: list[datetime] = []
        current = start
        while current <= end:
            timestamps.append(current)
            current += step
        return {"gauge": [MetricRow(timestamp, "gauge", 1.0) for timestamp in timestamps]}

    source = MagicMock()
    source.query.side_effect = response

    evidence = collect_daily_evidence(
        source,
        [query],
        windows,
        step=timedelta(hours=6),
        chunk_days=5,
    )

    assert calls == [(windows[0][0], windows[-1][1]), (windows[0][0], windows[0][1]), (windows[1][0], windows[1][1])]
    assert [row.timestamp for row in evidence[windows[0]]["gauge"]] == [
        windows[0][0] + timedelta(hours=6 * offset) for offset in range(4)
    ]
    assert [row.timestamp for row in evidence[windows[1]]["gauge"]] == [
        windows[1][0] + timedelta(hours=6 * offset) for offset in range(4)
    ]


@pytest.mark.parametrize(
    "invalid_windows",
    [
        ((datetime(2026, 2, 1), datetime(2026, 2, 2)),),
        ((datetime(2026, 2, 1, 1, tzinfo=UTC), datetime(2026, 2, 2, 1, tzinfo=UTC)),),
        ((datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 1, 23, tzinfo=UTC)),),
        (
            (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 2, tzinfo=UTC)),
            (datetime(2026, 2, 1, 12, tzinfo=UTC), datetime(2026, 2, 2, 12, tzinfo=UTC)),
        ),
        (
            (datetime(2026, 2, 2, tzinfo=UTC), datetime(2026, 2, 3, tzinfo=UTC)),
            (datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 2, 2, tzinfo=UTC)),
        ),
    ],
)
def test_bounded_window_chunks_reject_invalid_calculation_windows(invalid_windows) -> None:
    with pytest.raises(ValueError):
        bounded_window_chunks(invalid_windows, chunk_days=5)


def test_bounded_window_chunks_preserve_ordered_discontiguous_runs() -> None:
    windows = _windows(4)
    selected = (windows[0], windows[1], windows[3])

    assert bounded_window_chunks(selected, chunk_days=2) == (
        (windows[0], windows[1]),
        (windows[3],),
    )


@pytest.mark.parametrize(
    ("chunk_days", "expected_lengths"),
    [(1, (1, 1, 1, 1, 1, 1, 1)), (5, (5, 2)), (6, (6, 1)), (30, (7,))],
)
def test_bounded_window_chunks_enforce_the_supported_h_matrix(
    chunk_days: int,
    expected_lengths: tuple[int, ...],
) -> None:
    chunks = bounded_window_chunks(_windows(7), chunk_days=chunk_days)

    assert tuple(len(chunk) for chunk in chunks) == expected_lengths


@pytest.mark.parametrize(
    ("chunk_days", "selected_indexes", "expected_lengths"),
    [
        (5, (0, 1, 2, 3, 4, 5), (5, 1)),
        (5, (0, 1, 3, 4, 5, 7), (2, 3, 1)),
        (6, (0, 1, 2, 3, 4, 5, 6), (6, 1)),
    ],
)
def test_chunk_formula_handles_h_plus_one_and_gaps(
    chunk_days: int,
    selected_indexes: tuple[int, ...],
    expected_lengths: tuple[int, ...],
) -> None:
    windows = _windows(max(selected_indexes) + 1)

    selected = tuple(windows[index] for index in selected_indexes)

    assert tuple(len(chunk) for chunk in bounded_window_chunks(selected, chunk_days)) == expected_lengths


@pytest.mark.parametrize(
    ("chunk_days", "cluster_lengths", "topic_lengths"),
    [
        (1, (1, 1, 1, 1, 1, 1, 1), (1, 1, 1)),
        (5, (5, 2), (2, 1)),
        (30, (7,), (2, 1)),
    ],
)
def test_independent_cluster_and_topic_date_sets_keep_h_bounds(
    chunk_days: int,
    cluster_lengths: tuple[int, ...],
    topic_lengths: tuple[int, ...],
) -> None:
    windows = _windows(7)
    topic_windows = (windows[0], windows[1], windows[4])

    assert tuple(len(chunk) for chunk in bounded_window_chunks(windows, chunk_days)) == cluster_lengths
    assert tuple(len(chunk) for chunk in bounded_window_chunks(topic_windows, chunk_days)) == topic_lengths


def test_cluster_and_topic_date_sets_are_chunked_independently() -> None:
    windows = _windows(7)
    cluster_windows = windows
    topic_windows = (windows[0], windows[1], windows[4])

    assert tuple(len(chunk) for chunk in bounded_window_chunks(cluster_windows, chunk_days=5)) == (5, 2)
    assert tuple(len(chunk) for chunk in bounded_window_chunks(topic_windows, chunk_days=5)) == (2, 1)


def test_non_divisor_gauge_values_match_each_legacy_daily_grid() -> None:
    windows = _windows(6)
    query = _query("gauge", mode="range")
    step = timedelta(hours=10)

    def response(*, start: datetime, end: datetime, step: timedelta, **_: object):
        rows: list[MetricRow] = []
        timestamp = start
        while timestamp <= end:
            rows.append(MetricRow(timestamp, "gauge", float(timestamp.timestamp())))
            timestamp += step
        return {"gauge": rows}

    source = MagicMock()
    source.query.side_effect = response
    evidence = collect_daily_evidence(
        source,
        [query],
        windows,
        step=step,
        chunk_days=6,
    )

    assert source.query.call_count == 5
    for window in windows:
        expected: list[tuple[datetime, float]] = []
        timestamp = window[0]
        while timestamp < window[1]:
            expected.append((timestamp, float(timestamp.timestamp())))
            timestamp += step
        assert [(row.timestamp, row.value) for row in evidence[window]["gauge"]] == expected


def test_iter_daily_evidence_queries_and_yields_one_chunk_at_a_time() -> None:
    windows = _windows(2)
    query = _query("counter")
    source = MagicMock()

    def response(*, queries: list[MetricQuery], end: datetime, **_: object):
        return {"counter": [MetricRow(end, "counter", 1.0)]}

    source.query.side_effect = response
    evidence = iter_daily_evidence(
        source,
        [query],
        windows,
        step=timedelta(hours=1),
        chunk_days=1,
    )

    first_chunk, first_result = next(evidence)
    assert first_chunk == (windows[0],)
    assert source.query.call_count == 1
    assert first_result[windows[0]]["counter"]

    second_chunk, second_result = next(evidence)
    assert second_chunk == (windows[1],)
    assert source.query.call_count == 2
    assert second_result[windows[1]]["counter"]

    with pytest.raises(StopIteration):
        next(evidence)


def test_iter_daily_evidence_releases_previous_chunk_before_next_acquisition() -> None:
    windows = _windows(2)
    query = _query("counter")
    source = MagicMock()
    calls = 0
    first_row_ref: weakref.ReferenceType[MetricRow] | None = None

    def response(*, end: datetime, **_: object) -> dict[str, list[MetricRow]]:
        nonlocal calls, first_row_ref
        calls += 1
        if calls == 1:
            first_row = MetricRow(end, "counter", 1.0)
            first_row_ref = weakref.ref(first_row)
            return {"counter": [first_row]}
        gc.collect()
        assert first_row_ref is not None
        assert first_row_ref() is None
        return {"counter": [MetricRow(end, "counter", 2.0)]}

    source.query.side_effect = response
    evidence = iter_daily_evidence(
        source,
        [query],
        windows,
        step=timedelta(hours=1),
        chunk_days=1,
    )

    _, first_result = next(evidence)
    del first_result
    next(evidence)
