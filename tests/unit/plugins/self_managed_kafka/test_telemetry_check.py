"""Tests for the self-managed Kafka operator telemetry checker."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery, MetricRow


def _config(
    *,
    identity_source: str = "prometheus",
    topic_attribution_enabled: bool = True,
    prometheus_resource_discovery: bool = True,
    metric_name_overrides: dict[str, str] | None = None,
    label_name_overrides: dict[str, dict[str, str]] | None = None,
) -> object:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    settings: dict[str, object] = {
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
        "identity_source": {"source": identity_source},
        "resource_source": (
            {"source": "prometheus"}
            if prometheus_resource_discovery
            else {"source": "admin_api", "bootstrap_servers": "kafka:9092"}
        ),
        "topic_attribution": {
            "enabled": topic_attribution_enabled,
            "compute_policy": "shared_even_v1" if topic_attribution_enabled else "disabled",
        },
        "discovery_window_hours": 6,
        "metrics_step_seconds": 900,
    }
    if metric_name_overrides is not None:
        settings["metric_name_overrides"] = metric_name_overrides
    if label_name_overrides is not None:
        settings["label_name_overrides"] = label_name_overrides
    return SelfManagedKafkaConfig.from_plugin_settings(settings)


class _CheckerMetricsSource:
    """Complete MetricsSource double for the checker query contract."""

    def __init__(
        self,
        responses: dict[str, list[MetricRow]],
        *,
        omitted_keys: frozenset[str] = frozenset(),
        failures: frozenset[str] = frozenset(),
    ) -> None:
        self.responses = responses
        self.omitted_keys = omitted_keys
        self.failures = failures
        self.calls: list[tuple[MetricQuery, datetime, datetime, timedelta, str | None]] = []
        self.closed = False

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        assert len(queries) == 1
        [query] = queries
        self.calls.append((query, start, end, step, resource_id_filter))
        if query.key in self.failures:
            raise MetricsQueryError("Prometheus unavailable")
        if query.key in self.omitted_keys:
            return {}
        return {query.key: self.responses.get(query.key, [])}

    def close(self) -> None:
        self.closed = True


def _row(
    metric_key: str,
    canonical_metric: str,
    *,
    value: float = 1.0,
    labels: dict[str, str] | None = None,
) -> MetricRow:
    physical_labels = {"__name__": canonical_metric, "deployment": "kafka-prod", **(labels or {})}
    return MetricRow(
        timestamp=datetime(2026, 8, 3, tzinfo=UTC),
        metric_key=metric_key,
        value=value,
        labels={key: label for key, label in physical_labels.items() if key != "__name__"},
        source_series=tuple(sorted(physical_labels.items())),
    )


def _complete_default_responses() -> dict[str, list[MetricRow]]:
    return {
        "telemetry_check_up": [_row("telemetry_check_up", "up")],
        "telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesin_total": [
            _row(
                "telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesin_total",
                "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
                labels={"broker": "1"},
            )
        ],
        "telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesout_total": [
            _row(
                "telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesout_total",
                "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
                labels={"broker": "1"},
            )
        ],
        "telemetry_check_kafka_log_log_size": [
            _row(
                "telemetry_check_kafka_log_log_size",
                "kafka_log_log_size",
                labels={"broker": "1", "topic": "orders", "partition": "0"},
            )
        ],
        "telemetry_check_kafka_server_brokertopicmetrics_bytesin_total": [
            _row(
                "telemetry_check_kafka_server_brokertopicmetrics_bytesin_total",
                "kafka_server_brokertopicmetrics_bytesin_total",
                labels={"broker": "1", "topic": "orders"},
            )
        ],
        "telemetry_check_kafka_server_brokertopicmetrics_bytesout_total": [
            _row(
                "telemetry_check_kafka_server_brokertopicmetrics_bytesout_total",
                "kafka_server_brokertopicmetrics_bytesout_total",
                labels={"broker": "1", "topic": "orders"},
            )
        ],
        "telemetry_check_kafka_server_quota_byte_rate": [
            _row(
                "telemetry_check_kafka_server_quota_byte_rate",
                "kafka_server_quota_byte_rate",
                labels={
                    "broker": "1",
                    "quota_type": "Produce",
                    "quota_scope": "user",
                    "user": "alice",
                    "client_id": "client-a",
                },
            )
        ],
        "telemetry_check_kafka_server_quota_throttle_time_ms": [
            _row(
                "telemetry_check_kafka_server_quota_throttle_time_ms",
                "kafka_server_quota_throttle_time_ms",
                labels={
                    "broker": "1",
                    "quota_type": "Produce",
                    "quota_scope": "user",
                    "user": "alice",
                    "client_id": "client-a",
                },
            )
        ],
    }


def _records_by_metric(records: Sequence[object]) -> dict[str, object]:
    return {record.canonical_metric: record for record in records}


def test_checker_queries_all_enabled_families_with_the_resolved_physical_selector_and_window() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    config = _config(
        metric_name_overrides={"kafka_log_log_size": "company_kafka_partition_size"},
        label_name_overrides={"kafka_log_log_size": {"broker": "node", "topic": "topic_name", "partition": "part"}},
    )
    responses = _complete_default_responses()
    responses["telemetry_check_kafka_log_log_size"] = [
        _row(
            "telemetry_check_kafka_log_log_size",
            "company_kafka_partition_size",
            labels={"node": "1", "topic_name": "orders", "part": "0"},
        )
    ]
    source = _CheckerMetricsSource(responses)
    window_end = datetime(2026, 8, 4, 12, tzinfo=UTC)

    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(tenant_name="kafka-dc1", config=config, window_end=window_end)

    expected = {
        "up": {
            "resolved_metric": "up",
            "expected_labels": {},
            "observed_labels": ("deployment",),
            "affected_feature": ("target_scope",),
        },
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total": {
            "resolved_metric": "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
            "expected_labels": {"broker": "broker"},
            "observed_labels": ("broker", "deployment"),
            "affected_feature": ("cluster_ingress",),
        },
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total": {
            "resolved_metric": "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
            "expected_labels": {"broker": "broker"},
            "observed_labels": ("broker", "deployment"),
            "affected_feature": ("cluster_egress",),
        },
        "kafka_log_log_size": {
            "resolved_metric": "company_kafka_partition_size",
            "expected_labels": {"broker": "node", "topic": "topic_name", "partition": "part"},
            "observed_labels": ("deployment", "node", "part", "topic_name"),
            "affected_feature": ("cluster_storage", "prometheus_discovery", "topic_storage"),
        },
        "kafka_server_brokertopicmetrics_bytesin_total": {
            "resolved_metric": "kafka_server_brokertopicmetrics_bytesin_total",
            "expected_labels": {"broker": "broker", "topic": "topic"},
            "observed_labels": ("broker", "deployment", "topic"),
            "affected_feature": ("prometheus_discovery", "topic_ingress"),
        },
        "kafka_server_brokertopicmetrics_bytesout_total": {
            "resolved_metric": "kafka_server_brokertopicmetrics_bytesout_total",
            "expected_labels": {"broker": "broker", "topic": "topic"},
            "observed_labels": ("broker", "deployment", "topic"),
            "affected_feature": ("prometheus_discovery", "topic_egress"),
        },
        "kafka_server_quota_byte_rate": {
            "resolved_metric": "kafka_server_quota_byte_rate",
            "expected_labels": {
                "broker": "broker",
                "quota_type": "quota_type",
                "quota_scope": "quota_scope",
                "user": "user",
                "client_id": "client_id",
            },
            "observed_labels": ("broker", "client_id", "deployment", "quota_scope", "quota_type", "user"),
            "affected_feature": ("principal_readiness", "principal_attribution"),
        },
        "kafka_server_quota_throttle_time_ms": {
            "resolved_metric": "kafka_server_quota_throttle_time_ms",
            "expected_labels": {
                "broker": "broker",
                "quota_type": "quota_type",
                "quota_scope": "quota_scope",
                "user": "user",
                "client_id": "client_id",
            },
            "observed_labels": ("broker", "client_id", "deployment", "quota_scope", "quota_type", "user"),
            "affected_feature": ("principal_readiness",),
        },
    }

    assert len(records) == len(expected) == 8
    assert [record.canonical_metric for record in records] == list(expected)
    for record in records:
        expected_record = expected[record.canonical_metric]
        assert record.tenant == "kafka-dc1"
        assert record.state is TelemetryCheckState.VALID
        assert record.resolved_metric == expected_record["resolved_metric"]
        assert record.selector == 'deployment="kafka-prod"'
        assert record.expected_labels == expected_record["expected_labels"]
        assert record.observed_labels == expected_record["observed_labels"]
        assert record.affected_feature == expected_record["affected_feature"]
        assert record.corrective_override is None
        assert record.warning is None
    log_size_call = next(call for call in source.calls if call[0].key == "telemetry_check_kafka_log_log_size")
    query, start, end, step, resource_filter = log_size_call
    assert query.query_expression == "company_kafka_partition_size{}"
    assert query.label_keys == ("node", "topic_name", "part", "deployment")
    assert query.resource_label == "deployment"
    assert query.query_mode == "range"
    assert start == window_end - timedelta(hours=6)
    assert end == window_end
    assert step == timedelta(seconds=900)
    assert resource_filter == "kafka-prod"
    assert source.closed is True


def test_checker_reports_feature_disabled_families_as_skipped_without_querying_them() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    source = _CheckerMetricsSource(_complete_default_responses())
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(
                identity_source="static",
                topic_attribution_enabled=False,
                prometheus_resource_discovery=False,
            ),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    checks = _records_by_metric(records)
    assert checks["kafka_server_brokertopicmetrics_bytesin_total"].state is TelemetryCheckState.SKIPPED
    assert checks["kafka_server_brokertopicmetrics_bytesout_total"].state is TelemetryCheckState.SKIPPED
    assert checks["kafka_server_quota_byte_rate"].state is TelemetryCheckState.SKIPPED
    assert checks["kafka_server_quota_throttle_time_ms"].state is TelemetryCheckState.SKIPPED
    assert checks["kafka_server_quota_byte_rate"].affected_feature == (
        "principal_readiness",
        "principal_attribution",
    )
    queried_keys = {query.key for query, *_ in source.calls}
    assert "telemetry_check_kafka_server_brokertopicmetrics_bytesin_total" not in queried_keys
    assert "telemetry_check_kafka_server_quota_byte_rate" not in queried_keys


def test_checker_marks_any_partial_or_blank_family_label_series_invalid_with_deterministic_correction() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    responses = _complete_default_responses()
    responses["telemetry_check_kafka_log_log_size"] = [
        _row(
            "telemetry_check_kafka_log_log_size",
            "kafka_log_log_size",
            labels={"broker": "1", "topic": "orders", "partition": "0", "instance": "broker-1"},
        ),
        _row(
            "telemetry_check_kafka_log_log_size",
            "kafka_log_log_size",
            labels={"broker": "", "instance": "broker-2"},
        ),
    ]
    source = _CheckerMetricsSource(responses)
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    check = _records_by_metric(records)["kafka_log_log_size"]
    assert check.state is TelemetryCheckState.INVALID
    assert check.observed_labels == ("broker", "deployment", "instance", "partition", "topic")
    assert check.corrective_override == {
        "label_name_overrides": {
            "kafka_log_log_size": {
                "broker": "<physical-label>",
                "partition": "<physical-label>",
                "topic": "<physical-label>",
            }
        }
    }
    assert check.warning == (
        "Observed series are missing or have blank resolved labels for canonical labels: broker, partition, topic."
    )


def test_checker_gives_missing_global_selector_precedence_over_family_label_correction() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    responses = _complete_default_responses()
    responses["telemetry_check_kafka_log_log_size"] = [
        MetricRow(
            timestamp=datetime(2026, 8, 3, tzinfo=UTC),
            metric_key="telemetry_check_kafka_log_log_size",
            value=1.0,
            labels={"broker": "1"},
            source_series=(("__name__", "kafka_log_log_size"), ("broker", "1")),
        )
    ]
    source = _CheckerMetricsSource(responses)
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    check = _records_by_metric(records)["kafka_log_log_size"]
    assert check.state is TelemetryCheckState.INVALID
    assert check.corrective_override is None
    assert check.warning == "Selected series is missing or has a blank global selector label deployment."


def test_checker_gives_blank_global_selector_precedence_over_family_label_correction() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    responses = _complete_default_responses()
    responses["telemetry_check_kafka_log_log_size"] = [
        MetricRow(
            timestamp=datetime(2026, 8, 3, tzinfo=UTC),
            metric_key="telemetry_check_kafka_log_log_size",
            value=1.0,
            labels={"deployment": "", "broker": "1"},
            source_series=(
                ("__name__", "kafka_log_log_size"),
                ("broker", "1"),
                ("deployment", ""),
            ),
        )
    ]
    source = _CheckerMetricsSource(responses)
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    check = _records_by_metric(records)["kafka_log_log_size"]
    assert check.state is TelemetryCheckState.INVALID
    assert check.observed_labels == ("broker", "deployment")
    assert check.corrective_override is None
    assert check.warning == "Selected series is missing or has a blank global selector label deployment."


def test_checker_deduplicates_identical_physical_series_before_validation() -> None:
    from plugins.self_managed_kafka.telemetry_check import (
        TelemetryCheckState,
        _TelemetryChecker,
        check_self_managed_telemetry,
    )

    responses = _complete_default_responses()
    duplicate = _row(
        "telemetry_check_kafka_log_log_size",
        "kafka_log_log_size",
        labels={"broker": "1", "topic": "orders", "partition": "0"},
    )
    responses["telemetry_check_kafka_log_log_size"] = [duplicate, duplicate]
    source = _CheckerMetricsSource(responses)
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    check = _records_by_metric(records)["kafka_log_log_size"]
    assert check.state is TelemetryCheckState.VALID
    assert check.observed_labels == ("broker", "deployment", "partition", "topic")
    assert check.corrective_override is None
    assert check.warning is None
    assert _TelemetryChecker._physical_series([duplicate, duplicate]) == (
        {
            "__name__": "kafka_log_log_size",
            "broker": "1",
            "deployment": "kafka-prod",
            "partition": "0",
            "topic": "orders",
        },
    )


@pytest.mark.parametrize("value", [0.0, math.nan, math.inf])
def test_checker_marks_unhealthy_up_samples_invalid_after_label_validation(value: float) -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    responses = _complete_default_responses()
    responses["telemetry_check_up"] = [_row("telemetry_check_up", "up", value=value)]
    source = _CheckerMetricsSource(responses)
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    check = _records_by_metric(records)["up"]
    assert check.state is TelemetryCheckState.INVALID
    assert check.corrective_override is None
    assert check.warning == "At least one selected up sample is non-finite or not equal to 1."


def test_checker_distinguishes_not_observed_missing_results_and_query_failures() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    source = _CheckerMetricsSource(
        _complete_default_responses(),
        omitted_keys=frozenset({"telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesout_total"}),
        failures=frozenset({"telemetry_check_kafka_server_quota_byte_rate"}),
    )
    source.responses["telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesin_total"] = []
    with patch("plugins.self_managed_kafka.telemetry_check.create_metrics_source", return_value=source):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=_config(),
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    checks = _records_by_metric(records)
    not_observed = checks["kafka_server_brokertopicmetrics_alltopics_bytesin_total"]
    omitted = checks["kafka_server_brokertopicmetrics_alltopics_bytesout_total"]
    failed = checks["kafka_server_quota_byte_rate"]
    assert not_observed.state is TelemetryCheckState.NOT_OBSERVED
    assert not_observed.corrective_override == {
        "metric_name_overrides": {"kafka_server_brokertopicmetrics_alltopics_bytesin_total": "<physical-metric-name>"}
    }
    assert not_observed.warning == (
        "No selected series was observed during the check window; historical coverage remains a warning only."
    )
    assert omitted.state is TelemetryCheckState.INCONCLUSIVE
    assert omitted.warning == (
        "Prometheus response omitted result key "
        "telemetry_check_kafka_server_brokertopicmetrics_alltopics_bytesout_total."
    )
    assert failed.state is TelemetryCheckState.INCONCLUSIVE
    assert failed.warning == "Prometheus family query failed: MetricsQueryError."
    assert source.closed is True


def test_checker_rejects_naive_windows_and_converts_source_construction_failures_to_records() -> None:
    from plugins.self_managed_kafka.telemetry_check import TelemetryCheckState, check_self_managed_telemetry

    config = _config(
        identity_source="static",
        topic_attribution_enabled=False,
        prometheus_resource_discovery=False,
    )
    with pytest.raises(ValueError, match="Naive datetime not allowed"):
        check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=config,
            window_end=datetime(2026, 8, 4),
        )

    with patch(
        "plugins.self_managed_kafka.telemetry_check.create_metrics_source",
        side_effect=RuntimeError("unavailable"),
    ):
        records = check_self_managed_telemetry(
            tenant_name="kafka-dc1",
            config=config,
            window_end=datetime(2026, 8, 4, tzinfo=UTC),
        )

    checks = _records_by_metric(records)
    assert checks["up"].state is TelemetryCheckState.INCONCLUSIVE
    assert checks["up"].warning == "Prometheus source construction failed before observation: RuntimeError."
    assert checks["kafka_server_brokertopicmetrics_bytesin_total"].state is TelemetryCheckState.SKIPPED
    assert checks["kafka_server_brokertopicmetrics_bytesin_total"].warning is None


def test_checker_renderer_emits_deterministic_json_lines_summary_and_final_newline() -> None:
    from plugins.self_managed_kafka.telemetry_check import (
        TelemetryCheckState,
        TelemetryFamilyCheck,
        render_telemetry_check_jsonl,
    )

    output = render_telemetry_check_jsonl(
        [
            TelemetryFamilyCheck(
                tenant="kafka-dc1",
                canonical_metric="up",
                state=TelemetryCheckState.VALID,
                resolved_metric="up",
                selector='deployment="kafka-prod"',
                expected_labels={},
                observed_labels=("deployment",),
                affected_feature=("target_scope",),
                corrective_override=None,
                warning=None,
            ),
            TelemetryFamilyCheck(
                tenant="kafka-dc2",
                canonical_metric="kafka_log_log_size",
                state=TelemetryCheckState.NOT_OBSERVED,
                resolved_metric="company_log_size",
                selector='deployment="kafka-dr"',
                expected_labels={"broker": "node", "partition": "partition", "topic": "topic"},
                observed_labels=(),
                affected_feature=("cluster_storage", "prometheus_discovery", "topic_storage"),
                corrective_override={"metric_name_overrides": {"kafka_log_log_size": "<physical-metric-name>"}},
                warning=(
                    "No selected series was observed during the check window; "
                    "historical coverage remains a warning only."
                ),
            ),
        ],
        tenant_count=2,
    )

    assert output == (
        '{"affected_feature":["target_scope"],"canonical_metric":"up","corrective_override":null,'
        '"expected_labels":{},"observed_labels":["deployment"],"resolved_metric":"up",'
        '"selector":"deployment=\\"kafka-prod\\"","state":"valid","tenant":"kafka-dc1","warning":null}\n'
        '{"affected_feature":["cluster_storage","prometheus_discovery","topic_storage"],'
        '"canonical_metric":"kafka_log_log_size","corrective_override":{"metric_name_overrides":'
        '{"kafka_log_log_size":"<physical-metric-name>"}},"expected_labels":{"broker":"node",'
        '"partition":"partition","topic":"topic"},"observed_labels":[],"resolved_metric":"company_log_size",'
        '"selector":"deployment=\\"kafka-dr\\"","state":"not_observed","tenant":"kafka-dc2",'
        '"warning":"No selected series was observed during the check window; historical coverage remains a warning '
        'only."}\n'
        '{"summary":{"inconclusive":0,"invalid":0,"not_observed":1,"skipped":0,"valid":1},"tenants":2}\n'
    )
