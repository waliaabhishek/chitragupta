"""Prometheus transport measurements for self-managed Kafka acquisition."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs

import httpx
import pytest

from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
from core.models import MetricQuery


def _plugin_settings(
    *,
    chunk_days: int | None = None,
    metrics_step_seconds: int | None = None,
    metric_name_overrides: dict[str, str] | None = None,
) -> dict[str, object]:
    settings: dict[str, object] = {
        "cluster_id": "billing-cluster-a",
        "metrics_identifier": "kraft-a-001",
        "broker_count": 3,
        "metrics": {"url": "http://prometheus.test"},
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "topic_attribution": {"enabled": True, "compute_policy": "shared_even_v1"},
    }
    if chunk_days is not None:
        settings["historical_acquisition_chunk_days"] = chunk_days
    if metrics_step_seconds is not None:
        settings["metrics_step_seconds"] = metrics_step_seconds
    if metric_name_overrides is not None:
        settings["metric_name_overrides"] = metric_name_overrides
    return settings


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def _response_for(expression: str, start: datetime, end: datetime, step_seconds: int) -> dict[str, object]:
    labels: dict[str, str] = {}
    if expression.startswith("up{"):
        labels = {"kafka_cluster_id": "kraft-a-001"}
        timestamps = _grid(start, end, timedelta(seconds=step_seconds))
        value = "1"
    elif "sum by (topic)" in expression:
        labels = {"topic": "orders", "partition": "0"}
        timestamps = _grid(start, end, timedelta(seconds=step_seconds))
        value = "1073741824"
    elif "increase(" in expression or "kafka_log_log_size" in expression:
        timestamps = _grid(start, end, timedelta(seconds=step_seconds))
        value = "1073741824"
    else:
        timestamps = (end,)
        value = "1073741824"
    return {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "metric": labels,
                    "values": [[timestamp.timestamp(), value] for timestamp in timestamps],
                }
            ],
        },
    }


def _grid(start: datetime, end: datetime, step: timedelta) -> tuple[datetime, ...]:
    points: list[datetime] = []
    timestamp = start
    while timestamp <= end:
        points.append(timestamp)
        timestamp += step
    return tuple(points)


def _category(expression: str) -> str:
    if expression.startswith("up{"):
        return "target_up"
    scope = "topic" if "sum by (topic)" in expression else "cluster"
    if "bytesin" in expression:
        return f"{scope}_ingress"
    if "bytesout" in expression:
        return f"{scope}_egress"
    return f"{scope}_storage"


def _measured_source(
    *,
    retry_statuses: int = 0,
    cache_ttl_seconds: float | None = 0,
    fail_bounded_category: str | None = None,
) -> tuple[PrometheusMetricsSource, list[dict[str, str]]]:
    requests: list[dict[str, str]] = []
    remaining_retries = retry_statuses
    failed_bounded_request: tuple[str, str, str] | None = None

    def transport(request: httpx.Request) -> httpx.Response:
        nonlocal failed_bounded_request, remaining_retries
        data = {key: values[-1] for key, values in parse_qs(request.content.decode()).items()}
        requests.append(data)
        if remaining_retries:
            remaining_retries -= 1
            return httpx.Response(503, text="transient", request=request)
        start = _timestamp(data["start"] if "start" in data else data["time"])
        end = _timestamp(data["end"] if "end" in data else data["time"])
        category = _category(data["query"])
        request_key = (category, data.get("start", data.get("time", "")), data.get("end", data.get("time", "")))
        if fail_bounded_category == category and "start" in data and "end" in data and end - start > timedelta(days=1):
            if failed_bounded_request is None:
                failed_bounded_request = request_key
            if request_key == failed_bounded_request:
                return httpx.Response(503, text="bounded transient", request=request)
        return httpx.Response(
            200,
            json=_response_for(data["query"], start, end, int(data.get("step", "3600"))),
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(transport))
    source = PrometheusMetricsSource(
        PrometheusConfig(
            url="http://prometheus.test",
            cache_ttl_seconds=cache_ttl_seconds,
            max_retries=4,
            base_delay=0,
        ),
        client=client,
    )
    return source, requests


def _logical_family_count(logical_query: MagicMock) -> int:
    """Count PromQL families, not batched MetricsSource.query invocations."""
    return sum(len(_queries_from_call(call)) for call in logical_query.call_args_list)


def _queries_from_call(call: Any) -> Sequence[MetricQuery]:
    kwargs = call.kwargs
    if "queries" in kwargs:
        return kwargs["queries"]
    return call.args[0]


def _logical_family_categories(logical_query: MagicMock) -> Counter[str]:
    return Counter(
        _category(query.query_expression) for call in logical_query.call_args_list for query in _queries_from_call(call)
    )


def _effective_chunk_days(chunk_days: int | None) -> int:
    return chunk_days if chunk_days is not None else 5


def _assert_workload_response_spans(requests: list[dict[str, str]], chunk_days: int | None) -> None:
    max_span = timedelta(days=_effective_chunk_days(chunk_days))
    for request in requests:
        if "start" not in request or not _category(request["query"]).startswith(("cluster_", "topic_")):
            continue
        assert _timestamp(request["end"]) - _timestamp(request["start"]) <= max_span


def _assert_scope_response_spans(requests: list[dict[str, str]], chunk_days: int | None) -> None:
    max_span = timedelta(days=_effective_chunk_days(chunk_days))
    for request in requests:
        if "start" not in request or _category(request["query"]) != "target_up":
            continue
        assert _timestamp(request["end"]) - _timestamp(request["start"]) <= max_span


def _run_plugin_workload_measurement(
    *,
    chunk_days: int | None,
    cluster_days: int,
    topic_indexes: tuple[int, ...],
    metrics_step_seconds: int = 3600,
    recovery: bool = False,
    fail_bounded_category: str | None = None,
    metric_name_overrides: dict[str, str] | None = None,
) -> tuple[Counter[str], Counter[str], list[dict[str, str]], MagicMock, object, object, MagicMock]:
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source(fail_bounded_category=fail_bounded_category)
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(
            _plugin_settings(
                chunk_days=chunk_days,
                metrics_step_seconds=metrics_step_seconds,
                metric_name_overrides=metric_name_overrides,
            )
        )
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=cluster_days)
    windows = tuple((start + timedelta(days=index), start + timedelta(days=index + 1)) for index in range(cluster_days))
    topic_windows = tuple(windows[index] for index in topic_indexes)
    uow = MagicMock()
    if recovery:
        open_state = MagicMock(
            status="open",
            first_blocked_window_start=start,
            recovery_cursor_date=start.date(),
        )
        recovering_state = MagicMock(
            status="recovering",
            first_blocked_window_start=start,
            recovery_cursor_date=start.date(),
        )
        uow.self_managed_kafka_scope_state.get.side_effect = [open_state, recovering_state]
    else:
        uow.self_managed_kafka_scope_state.get.return_value = None

    with patch.object(source, "query", wraps=source.query) as logical_query:
        plugin.begin_scope_gate_run()
        initial_scope = plugin.prepare_gather_scope("tenant-1", start, end, uow)
        if recovery:
            plugin.persist_scope_recovery("tenant-1", initial_scope, uow)
            full_scope = plugin.prepare_post_recovery_gather_scope("tenant-1", start, end, uow)
        else:
            full_scope = initial_scope
        list(plugin.get_cost_input().gather("tenant-1", start, end, uow))
        provider = plugin.get_topic_attribution_provider()
        assert provider is not None
        for chunk in provider.iter_evidence_chunks(topic_windows):
            provider.prepare_evidence_chunk(chunk, timedelta(seconds=metrics_step_seconds))
            provider.clear_evidence_chunk()

    transport_categories = Counter(_category(request["query"]) for request in requests)
    return (
        transport_categories,
        _logical_family_categories(logical_query),
        requests,
        logical_query,
        initial_scope,
        full_scope,
        uow,
    )


def test_one_day_measurement_classifies_each_source_and_matches_cold_transport_attempts() -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source()
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = None

    with patch.object(source, "query", wraps=source.query) as logical_query:
        scope = plugin.prepare_gather_scope("tenant-1", start, end, uow)
        cost_lines = list(plugin.get_cost_input().gather("tenant-1", start, end, uow))
        provider = plugin.get_topic_attribution_provider()
        assert provider is not None
        provider.attribute_cluster(
            tenant_id="tenant-1",
            cluster_resource_id="billing-cluster-a",
            env_id="cluster-a",
            billing_lines=cost_lines,
            resource_topics=frozenset({"orders"}),
            metrics_step=timedelta(hours=1),
        )

    categories = Counter(_category(request["query"]) for request in requests)
    assert scope.decision is ScopeGateDecision.ALLOW
    assert categories == Counter(
        {
            "target_up": 1,
            "cluster_ingress": 1,
            "cluster_egress": 1,
            "cluster_storage": 1,
            "topic_ingress": 1,
            "topic_egress": 1,
            "topic_storage": 1,
        }
    )
    assert sum(categories.values()) == 7
    assert logical_query.call_count == 5
    assert _logical_family_count(logical_query) == 7
    assert _logical_family_categories(logical_query) == categories
    assert len(requests) == 7


def test_open_breaker_probe_has_one_logical_query_and_at_most_configured_transport_attempts() -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source(retry_statuses=4)
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
        status="open",
        first_blocked_window_start=start,
        recovery_cursor_date=start.date(),
    )

    with patch.object(source, "query", wraps=source.query) as logical_query:
        result = plugin.prepare_gather_scope("tenant-1", start, end, uow)

    assert result.decision is ScopeGateDecision.BLOCKED
    assert result.probe_only is True
    assert len(requests) == 4
    assert logical_query.call_count == 1
    assert _logical_family_count(logical_query) == 1
    assert {_category(request["query"]) for request in requests} == {"target_up"}
    assert {(request["start"], request["end"]) for request in requests} == {(end.isoformat(), end.isoformat())}


def test_recovery_probe_transient_then_success_keeps_one_logical_query() -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source(retry_statuses=1)
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
        status="open",
        first_blocked_window_start=start,
        recovery_cursor_date=start.date(),
    )

    with patch.object(source, "query", wraps=source.query) as logical_query:
        result = plugin.prepare_gather_scope("tenant-1", start, end, uow)

    assert result.decision is ScopeGateDecision.RECOVERY_READY
    assert len(requests) == 2
    assert logical_query.call_count == 1
    assert _logical_family_count(logical_query) == 1
    assert {_category(request["query"]) for request in requests} == {"target_up"}
    assert {(request["start"], request["end"]) for request in requests} == {(end.isoformat(), end.isoformat())}


@pytest.mark.parametrize(
    ("retry_statuses", "expected_attempts"),
    [(0, 1), (1, 2), (3, 4), (4, 4)],
)
def test_each_scope_workload_has_one_logical_query_and_bounded_transport_attempts(
    retry_statuses: int,
    expected_attempts: int,
) -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source(retry_statuses=retry_statuses)
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
        status="open",
        first_blocked_window_start=start,
        recovery_cursor_date=start.date(),
    )

    with patch.object(source, "query", wraps=source.query) as logical_query:
        result = plugin.prepare_gather_scope("tenant-1", start, end, uow)

    assert logical_query.call_count == 1
    assert _logical_family_count(logical_query) == 1
    assert len(requests) == expected_attempts
    assert result.decision in {ScopeGateDecision.BLOCKED, ScopeGateDecision.RECOVERY_READY}


def test_warm_prometheus_cache_separates_logical_queries_from_http_attempts() -> None:
    source, requests = _measured_source(cache_ttl_seconds=3600)
    query = MetricQuery(
        key="target_up",
        query_expression="up{}",
        label_keys=("kafka_cluster_id",),
        resource_label="kafka_cluster_id",
        query_mode="range",
    )
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=1)

    with patch.object(source, "query", wraps=source.query) as logical_query:
        source.query([query], start=start, end=end, step=timedelta(hours=1))
        source.query([query], start=start, end=end, step=timedelta(hours=1))

    assert logical_query.call_count == 2
    assert _logical_family_count(logical_query) == 2
    assert len(requests) == 1


@pytest.mark.parametrize(
    ("chunk_days", "cluster_days", "expected_chunks"),
    [
        (1, 12, 12),
        (None, 12, 3),
        (5, 6, 2),
        (30, 12, 1),
    ],
)
def test_divisor_request_matrix_counts_families_and_transport_attempts_per_side(
    chunk_days: int | None,
    cluster_days: int,
    expected_chunks: int,
) -> None:
    transport_categories, logical_categories, requests, logical_query, scope, _, _ = _run_plugin_workload_measurement(
        chunk_days=chunk_days,
        cluster_days=cluster_days,
        topic_indexes=tuple(range(cluster_days)),
    )
    expected = Counter(
        {
            "target_up": expected_chunks,
            "cluster_ingress": expected_chunks,
            "cluster_egress": expected_chunks,
            "cluster_storage": expected_chunks,
            "topic_ingress": expected_chunks,
            "topic_egress": expected_chunks,
            "topic_storage": expected_chunks,
        }
    )

    from core.plugin.protocols import ScopeGateDecision

    assert scope.decision is ScopeGateDecision.ALLOW
    assert logical_categories == expected
    assert transport_categories == expected
    assert len(requests) == sum(expected.values())
    assert _logical_family_count(logical_query) == sum(expected.values())
    _assert_workload_response_spans(requests, chunk_days)
    _assert_scope_response_spans(requests, chunk_days)


@pytest.mark.parametrize(
    ("chunk_days", "expected_cluster_chunks", "expected_topic_chunks"),
    [(1, 9, 4), (None, 2, 3), (30, 1, 3)],
)
def test_gapped_unequal_request_matrix_keeps_cluster_and_topic_counts_independent(
    chunk_days: int | None,
    expected_cluster_chunks: int,
    expected_topic_chunks: int,
) -> None:
    transport_categories, logical_categories, requests, logical_query, scope, _, _ = _run_plugin_workload_measurement(
        chunk_days=chunk_days,
        cluster_days=9,
        topic_indexes=(0, 1, 4, 8),
    )
    expected = Counter(
        {
            "target_up": expected_cluster_chunks,
            "cluster_ingress": expected_cluster_chunks,
            "cluster_egress": expected_cluster_chunks,
            "cluster_storage": expected_cluster_chunks,
            "topic_ingress": expected_topic_chunks,
            "topic_egress": expected_topic_chunks,
            "topic_storage": expected_topic_chunks,
        }
    )
    assert scope.decision.value == "allow"
    assert logical_categories == expected
    assert transport_categories == expected
    assert len(requests) == sum(expected.values())
    assert _logical_family_count(logical_query) == sum(expected.values())
    _assert_workload_response_spans(requests, chunk_days)
    _assert_scope_response_spans(requests, chunk_days)


def test_non_divisor_request_matrix_uses_two_counter_chunks_plus_gauge_grid_groups() -> None:
    transport_categories, logical_categories, requests, logical_query, scope, _, _ = _run_plugin_workload_measurement(
        chunk_days=5,
        cluster_days=6,
        topic_indexes=tuple(range(6)),
        metrics_step_seconds=3601,
    )
    from core.plugin.protocols import ScopeGateDecision

    expected = Counter(
        {
            "target_up": 2,
            "cluster_ingress": 2,
            "cluster_egress": 2,
            "cluster_storage": 6,
            "topic_ingress": 2,
            "topic_egress": 2,
            "topic_storage": 6,
        }
    )
    assert scope.decision is ScopeGateDecision.ALLOW
    assert logical_categories == expected
    assert transport_categories == expected
    assert len(requests) == sum(expected.values())
    assert _logical_family_count(logical_query) == sum(expected.values())
    _assert_workload_response_spans(requests, 5)
    _assert_scope_response_spans(requests, 5)


def test_family_local_fallback_matches_b_plus_f_logical_count_and_transport_attempts() -> None:
    transport_categories, logical_categories, requests, logical_query, scope, _, _ = _run_plugin_workload_measurement(
        chunk_days=5,
        cluster_days=6,
        topic_indexes=tuple(range(6)),
        fail_bounded_category="cluster_ingress",
    )
    from core.plugin.protocols import ScopeGateDecision

    expected_logical = Counter(
        {
            "target_up": 2,
            "cluster_ingress": 7,
            "cluster_egress": 2,
            "cluster_storage": 2,
            "topic_ingress": 2,
            "topic_egress": 2,
            "topic_storage": 2,
        }
    )
    expected_transport = expected_logical.copy()
    expected_transport["cluster_ingress"] = 10
    assert scope.decision is ScopeGateDecision.ALLOW
    assert logical_categories == expected_logical
    assert transport_categories == expected_transport
    assert len(requests) == sum(expected_transport.values())
    assert _logical_family_count(logical_query) == sum(expected_logical.values())
    _assert_workload_response_spans(requests, 5)
    _assert_scope_response_spans(requests, 5)


def test_successful_open_recovery_measures_probe_full_validation_and_all_workload_families() -> None:
    from core.plugin.protocols import ScopeGateDecision

    transport_categories, logical_categories, requests, logical_query, initial_scope, full_scope, uow = (
        _run_plugin_workload_measurement(
            chunk_days=5,
            cluster_days=6,
            topic_indexes=tuple(range(6)),
            recovery=True,
        )
    )
    expected = Counter(
        {
            "target_up": 3,
            "cluster_ingress": 2,
            "cluster_egress": 2,
            "cluster_storage": 2,
            "topic_ingress": 2,
            "topic_egress": 2,
            "topic_storage": 2,
        }
    )
    assert initial_scope.decision is ScopeGateDecision.RECOVERY_READY
    assert full_scope.decision is ScopeGateDecision.ALLOW
    assert logical_categories == expected
    assert transport_categories == expected
    assert len(requests) == sum(expected.values())
    assert _logical_family_count(logical_query) == sum(expected.values())
    _assert_workload_response_spans(requests, 5)
    _assert_scope_response_spans(requests, 5)
    uow.self_managed_kafka_scope_state.mark_recovering.assert_called_once()


def test_multi_day_backfill_measures_independent_bounded_cluster_and_topic_families() -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source()
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    end = start + timedelta(days=31)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = None
    windows = tuple((start + timedelta(days=index), start + timedelta(days=index + 1)) for index in range(31))

    with patch.object(source, "query", wraps=source.query) as logical_query:
        plugin.begin_scope_gate_run()
        scope = plugin.prepare_gather_scope("tenant-1", start, end, uow)
        list(plugin.get_cost_input().gather("tenant-1", start, end, uow))
        provider = plugin.get_topic_attribution_provider()
        assert provider is not None
        for chunk in provider.iter_evidence_chunks(windows):
            provider.prepare_evidence_chunk(chunk, timedelta(hours=1))
            provider.clear_evidence_chunk()

    categories = Counter(_category(request["query"]) for request in requests)
    assert scope.decision is ScopeGateDecision.ALLOW
    assert categories == Counter(
        {
            "target_up": 7,
            "cluster_ingress": 7,
            "cluster_egress": 7,
            "cluster_storage": 7,
            "topic_ingress": 7,
            "topic_egress": 7,
            "topic_storage": 7,
        }
    )
    _assert_workload_response_spans(requests, None)
    _assert_scope_response_spans(requests, None)
    assert logical_query.call_count == 49
    assert _logical_family_count(logical_query) == 49
    assert _logical_family_categories(logical_query) == categories
    assert len(requests) == 49


def test_recovery_measures_all_sources_with_unequal_discontiguous_cluster_and_topic_dates() -> None:
    from core.plugin.protocols import ScopeGateDecision
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    source, requests = _measured_source()
    plugin = SelfManagedKafkaPlugin()
    with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
        plugin.initialize(_plugin_settings())
    start = datetime(2026, 2, 1, tzinfo=UTC)
    cluster_end = start + timedelta(days=9)
    uow = MagicMock()
    uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
        status="retention_gap",
        first_blocked_window_start=start - timedelta(days=2),
        recovery_cursor_date=start.date(),
    )
    topic_windows = (
        (start, start + timedelta(days=1)),
        (start + timedelta(days=1), start + timedelta(days=2)),
        (start + timedelta(days=4), start + timedelta(days=5)),
        (start + timedelta(days=8), start + timedelta(days=9)),
    )

    with patch.object(source, "query", wraps=source.query) as logical_query:
        scope = plugin.prepare_gather_scope("tenant-1", start, cluster_end, uow)
        list(plugin.get_cost_input().gather("tenant-1", start, cluster_end, uow))
        provider = plugin.get_topic_attribution_provider()
        assert provider is not None
        for chunk in provider.iter_evidence_chunks(topic_windows):
            provider.prepare_evidence_chunk(chunk, timedelta(hours=1))
            provider.clear_evidence_chunk()

    categories = Counter(_category(request["query"]) for request in requests)
    assert scope.decision is ScopeGateDecision.ALLOW
    assert categories == Counter(
        {
            "target_up": 2,
            "cluster_ingress": 2,
            "cluster_egress": 2,
            "cluster_storage": 2,
            "topic_ingress": 3,
            "topic_egress": 3,
            "topic_storage": 3,
        }
    )
    assert scope.recovery_start == start
    assert scope.recovery_end == cluster_end
    assert logical_query.call_count == 17
    assert _logical_family_count(logical_query) == 17
    assert _logical_family_categories(logical_query) == categories
    assert len(requests) == sum(categories.values())


def test_name_aliases_do_not_change_logical_or_transport_request_counts() -> None:
    baseline = _run_plugin_workload_measurement(
        chunk_days=5,
        cluster_days=6,
        topic_indexes=(0, 2, 5),
    )
    overridden = _run_plugin_workload_measurement(
        chunk_days=5,
        cluster_days=6,
        topic_indexes=(0, 2, 5),
        metric_name_overrides={"kafka_log_log_size": "company_kafka_log_size"},
    )
    baseline_transport, baseline_logical, _, _, _, _, _ = baseline
    overridden_transport, overridden_logical, overridden_requests, _, _, _, _ = overridden

    assert overridden_transport == baseline_transport
    assert overridden_logical == baseline_logical
    assert sum(overridden_transport.values()) == sum(baseline_transport.values())
    assert any("company_kafka_log_size" in request["query"] for request in overridden_requests)
