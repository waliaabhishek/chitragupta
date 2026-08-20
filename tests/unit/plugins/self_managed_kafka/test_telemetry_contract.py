"""Behavioral tests for self-managed Kafka telemetry evidence."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from math import nan
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.metrics.protocol import MetricsQueryError
from core.models import MetricRow


def _settings(*, identity_source: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "cluster_id": "billing-cluster-a",
        "metrics_identifier": "kraft-a-001",
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": identity_source or {"source": "prometheus"},
        "metrics": {"url": "http://prometheus:9090"},
    }


def _quota_row(
    key: str,
    value: float,
    *,
    quota_type: str = "Produce",
    quota_scope: str = "user",
    user: str = "user-only",
    client_id: str = "not_applicable",
) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels={
            "kafka_cluster_id": "kraft-a-001",
            "broker": "1",
            "quota_type": quota_type,
            "quota_scope": quota_scope,
            "user": user,
            "client_id": client_id,
        },
    )


def _handler(*, identity_source: dict[str, object] | None = None) -> tuple[object, MagicMock]:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

    source = MagicMock()
    config = SelfManagedKafkaConfig.from_plugin_settings(_settings(identity_source=identity_source))
    return SelfManagedKafkaHandler(config, source), source


def _healthy_target_rows(query: object, query_kwargs: dict[str, object]) -> list[MetricRow]:
    start = query_kwargs["start"]
    end = query_kwargs["end"]
    step = query_kwargs["step"]
    assert isinstance(start, datetime)
    assert isinstance(end, datetime)
    assert isinstance(step, timedelta)
    rows: list[MetricRow] = []
    timestamp = start
    while timestamp < end:
        rows.append(
            MetricRow(
                timestamp=timestamp,
                metric_key=query.key,
                value=1.0,
                labels={"kafka_cluster_id": "kraft-a-001"},
            )
        )
        timestamp += step
    rows.append(
        MetricRow(
            timestamp=end,
            metric_key=query.key,
            value=1.0,
            labels={"kafka_cluster_id": "kraft-a-001"},
        )
    )
    return rows


class TestPrincipalTelemetryEvidence:
    @pytest.mark.parametrize(
        ("quota_scope", "user", "client_id"),
        [
            ("user", "user-only", "not_applicable"),
            ("client-id", "not_applicable", "client-only"),
            ("user-client", "shared-user", "shared-client"),
        ],
    )
    def test_documented_quota_scope_shapes_are_observed(
        self,
        quota_scope: str,
        user: str,
        client_id: str,
    ) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [
                _quota_row("quota_byte_rate", 8192.0, quota_scope=quota_scope, user=user, client_id=client_id)
            ],
            "quota_throttle_time_ms": [
                _quota_row("quota_throttle_time_ms", 0.0, quota_scope=quota_scope, user=user, client_id=client_id)
            ],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        evidence = resolution.context["principal_telemetry_evidence"]
        assert resolution.context["principal_attribution_status"] == "observed"
        assert evidence.quota_scopes == frozenset({quota_scope})

    @pytest.mark.parametrize(
        ("quota_scope", "user", "client_id"),
        [
            ("user", "not_applicable", "client-only"),
            ("client-id", "user-only", "not_applicable"),
            ("user-client", "shared-user", "not_applicable"),
        ],
    )
    def test_quota_scope_shapes_reject_missing_required_identity_labels(
        self,
        quota_scope: str,
        user: str,
        client_id: str,
    ) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [
                _quota_row("quota_byte_rate", 8192.0, quota_scope=quota_scope, user=user, client_id=client_id)
            ],
            "quota_throttle_time_ms": [],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.context["principal_attribution_status"] == "invalid"

    @pytest.mark.parametrize("missing_label", ["quota_type", "quota_scope", "user", "client_id"])
    def test_quota_rows_require_every_declared_shape_label(self, missing_label: str) -> None:
        handler, source = _handler()
        row = _quota_row("quota_byte_rate", 8192.0)
        del row.labels[missing_label]
        source.query.return_value = {
            "quota_byte_rate": [row],
            "quota_throttle_time_ms": [],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.context["principal_attribution_status"] == "invalid"

    def test_missing_quota_rows_are_not_observed_without_creating_metric_identities(self) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [],
            "quota_throttle_time_ms": [],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.metrics_derived.ids() == frozenset()
        assert resolution.context["principal_attribution_status"] == "not_observed"
        assert resolution.context["principal_attribution_detail"] == "principal_telemetry_not_observed"
        _, kwargs = source.query.call_args
        assert kwargs["resource_id_filter"] == "kraft-a-001"
        assert {query.resource_label for query in kwargs["queries"]} == {"kafka_cluster_id"}

    def test_missing_quota_rows_do_not_open_the_target_scope_breaker(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if not key.startswith("quota_"):
                return {key: _healthy_target_rows(queries[0], kwargs)}
            return {"quota_byte_rate": [], "quota_throttle_time_ms": []}

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()

        scope_result = plugin.prepare_gather_scope(
            "tenant-1",
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            uow,
        )
        plugin.get_service_handlers()["kafka"].resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            uow,
        )

        assert scope_result.decision == ScopeGateDecision.ALLOW
        uow.self_managed_kafka_scope_state.open.assert_not_called()

    def test_non_finite_throttle_is_valid_when_quota_identity_rows_are_structurally_valid(self) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)],
            "quota_throttle_time_ms": [_quota_row("quota_throttle_time_ms", nan)],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.metrics_derived.ids() == frozenset()
        assert resolution.context["principal_attribution_status"] == "observed"
        assert resolution.context["principal_attribution_detail"] == "no_finite_positive_throttle_observed"

    def test_invalid_quota_labels_are_distinguished_from_missing_rows(self) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0, quota_scope="unknown")],
            "quota_throttle_time_ms": [],
        }

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.context["principal_attribution_status"] == "invalid"
        assert resolution.context["principal_attribution_detail"] == "principal_telemetry_invalid"

    def test_quota_query_failure_is_transient_and_not_an_unsupported_claim(self) -> None:
        handler, source = _handler()
        source.query.side_effect = MetricsQueryError("prometheus unavailable")

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.context["principal_attribution_status"] == "transient_failure"
        assert resolution.context["principal_attribution_detail"] == "metrics_fetch_failed"

    def test_transient_quota_failure_is_not_cached_and_is_retried(self) -> None:
        handler, source = _handler()
        source.query.side_effect = [
            MetricsQueryError("prometheus unavailable"),
            {"quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)], "quota_throttle_time_ms": []},
        ]

        first = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )
        second = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert first.context["principal_attribution_status"] == "transient_failure"
        assert second.context["principal_attribution_status"] == "observed"
        assert source.query.call_count == 2

    def test_cached_quota_evidence_can_be_cleared_between_orchestration_cycles(self) -> None:
        handler, source = _handler()
        source.query.return_value = {
            "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)],
            "quota_throttle_time_ms": [],
        }
        handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )
        source.query.side_effect = MetricsQueryError("prometheus unavailable")
        handler.clear_principal_telemetry_evidence()

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.context["principal_attribution_status"] == "transient_failure"
        assert source.query.call_count == 2

    def test_static_identity_policy_is_marked_without_querying_quota_metrics(self) -> None:
        handler, source = _handler(
            identity_source={
                "source": "static",
                "static_identities": [{"identity_id": "team-data", "identity_type": "team"}],
            }
        )

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            MagicMock(),
        )

        assert resolution.resource_active.ids() == frozenset({"team-data"})
        assert resolution.context["principal_attribution_status"] == "policy_only_configured"
        assert resolution.context["measured_usage"] is False
        source.query.assert_not_called()


class TestMetricContractTypes:
    def test_exported_lab_metrics_use_documented_labels_without_principal_on_topic_counters(self) -> None:
        fixture = Path("tests/fixtures/self_managed_kafka_telemetry_lab/exporter-cluster-a.metrics")
        source = fixture.read_text(encoding="utf-8")

        topic_lines = [line for line in source.splitlines() if line.startswith("kafka_server_brokertopicmetrics_")]
        assert topic_lines
        assert all('kafka_cluster_id="kraft-a-001"' in line for line in topic_lines)
        assert all('topic="shared-topic"' in line for line in topic_lines)
        assert all("principal=" not in line for line in topic_lines)

        quota_lines = [line for line in source.splitlines() if line.startswith("kafka_server_quota_byte_rate{")]
        assert quota_lines
        assert all('quota_type="' in line and 'quota_scope="' in line for line in quota_lines)
        assert all(float(line.rsplit(" ", maxsplit=1)[1]) > 0 for line in quota_lines)

    def test_raw_jmx_fixture_uses_numeric_topic_and_quota_values_without_principal_dimensions(self) -> None:
        fixture = Path("tests/fixtures/self_managed_kafka_telemetry_lab/raw-jmx-cluster-a.jsonl")
        entries = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()]
        topic_entries = [entry for entry in entries if "BrokerTopicMetrics" in entry["object_name"]]
        quota_entries = [entry for entry in entries if "kafka.server:type=Produce" in entry["object_name"]]

        assert topic_entries
        assert all(isinstance(entry["value"], int | float) for entry in topic_entries)
        assert all("principal" not in entry["object_name"] for entry in topic_entries)
        assert any("topic=shared-topic" in entry["object_name"] for entry in topic_entries)
        assert quota_entries
        assert all(isinstance(entry["value"], int | float) for entry in quota_entries)
        assert any("user=" in entry["object_name"] for entry in quota_entries)
        assert any("client-id=" in entry["object_name"] for entry in quota_entries)


class TestTargetScopeEvidence:
    def test_target_scope_query_is_cluster_scoped_and_allows_a_healthy_target(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            return {queries[0].key: _healthy_target_rows(queries[0], kwargs)}

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None

        result = plugin.prepare_gather_scope(
            "tenant-1",
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            uow,
        )

        assert result.decision == ScopeGateDecision.ALLOW
        _, kwargs = source.query.call_args
        assert kwargs["resource_id_filter"] == "kraft-a-001"
        assert kwargs["queries"][0].resource_label == "kafka_cluster_id"

    def test_validated_scope_evidence_reaches_identity_resolution(self) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if key == "target_up":
                return {key: _healthy_target_rows(queries[0], kwargs)}
            return {
                "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)],
                "quota_throttle_time_ms": [],
            }

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)

        plugin.prepare_calculation_scope("tenant-1", [(start, end)], uow)
        resolution = plugin.get_service_handlers()["kafka"].resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            start,
            timedelta(days=1),
            None,
            uow,
        )

        assert resolution.context["metrics_scope_status"] == "valid"
        assert resolution.context["metrics_scope_detail"] == (
            "expected Prometheus target label kafka_cluster_id=kraft-a-001: target healthy"
        )

    def test_calculation_scope_preserves_recovery_metadata_for_healthy_windows(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            return {queries[0].key: _healthy_target_rows(queries[0], kwargs)}

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        state = MagicMock(
            status="recovering",
            first_blocked_window_start=start,
            recovery_cursor_date=start.date(),
        )
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = state

        result = plugin.prepare_calculation_scope("tenant-1", [(start, end)], uow)

        assert result.decision is ScopeGateDecision.ALLOW
        assert result.recovery_start == start
        assert result.recovery_end == end

    def test_non_aligned_range_requires_only_start_plus_step_samples(self) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        start = datetime(2026, 8, 1, tzinfo=UTC)
        step = timedelta(hours=1)
        end = start + timedelta(hours=2, minutes=30)

        assert SelfManagedKafkaPlugin._expected_scope_timestamps(start, end, step) == (
            start,
            start + step,
            start + step * 2,
        )

    def test_custom_target_label_scopes_cost_discovery_quota_and_health_queries(self) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()
        settings = _settings()
        settings["metrics_identifier_label"] = "deployment"
        settings["metrics_identifier"] = "east-a"

        def target_rows(query: object, kwargs: dict[str, object]) -> list[MetricRow]:
            start = kwargs["start"]
            end = kwargs["end"]
            step = kwargs["step"]
            assert isinstance(start, datetime)
            assert isinstance(end, datetime)
            assert isinstance(step, timedelta)
            rows: list[MetricRow] = []
            timestamp = start
            while timestamp <= end:
                rows.append(MetricRow(timestamp, query.key, 1.0, {"deployment": "east-a"}))
                timestamp += step
            return rows

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            key = queries[0].key
            if key == "target_up":
                return {key: target_rows(queries[0], kwargs)}
            if key == "broker_topic_discovery":
                return {key: []}
            if key.startswith("quota_"):
                return {
                    "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)],
                    "quota_throttle_time_ms": [],
                }
            start = kwargs["start"]
            assert isinstance(start, datetime)
            return {metric.key: [MetricRow(start, metric.key, 1.0, {})] for metric in queries}

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(settings)
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)

        list(plugin.get_cost_input().gather("tenant-1", start, end, uow))
        plugin.build_shared_context("tenant-1")
        plugin.prepare_gather_scope("tenant-1", start, end, uow)
        plugin.get_service_handlers()["kafka"].resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            start,
            timedelta(days=1),
            None,
            uow,
        )

        observed_labels = {
            query.key: query.resource_label for call in source.query.call_args_list for query in call.kwargs["queries"]
        }
        assert observed_labels["cluster_bytes_in"] == "deployment"
        assert observed_labels["broker_topic_discovery"] == "deployment"
        assert observed_labels["quota_byte_rate"] == "deployment"
        assert observed_labels["quota_throttle_time_ms"] == "deployment"
        assert observed_labels["target_up"] == "deployment"
        assert {call.kwargs["resource_id_filter"] for call in source.query.call_args_list} == {"east-a"}

    def test_target_scope_mismatch_raises_core_blocking_error_with_exact_selector_detail(self) -> None:
        from core.plugin.protocols import ScopeBlockedError
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            return {
                queries[0].key: [
                    MetricRow(
                        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
                        metric_key=queries[0].key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-b-002"},
                    )
                ]
            }

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None

        with pytest.raises(ScopeBlockedError, match="kafka_cluster_id=kraft-a-001"):
            plugin.prepare_gather_scope(
                "tenant-1",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 2, tzinfo=UTC),
                uow,
            )

    @pytest.mark.parametrize(
        ("rows", "status"),
        [
            ("incomplete", "not_observed"),
            ("mixed", "target_down"),
        ],
    )
    def test_target_scope_requires_finite_healthy_coverage_for_every_window_sample(
        self,
        rows: str,
        status: str,
    ) -> None:
        from core.plugin.protocols import ScopeBlockedError
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()

        def query(queries: list[object], **kwargs: object) -> dict[str, list[MetricRow]]:
            healthy = _healthy_target_rows(queries[0], kwargs)
            if rows == "incomplete":
                healthy.pop()
            else:
                healthy[-1] = MetricRow(
                    timestamp=healthy[-1].timestamp,
                    metric_key=healthy[-1].metric_key,
                    value=0.0,
                    labels=healthy[-1].labels,
                )
            return {queries[0].key: healthy}

        source.query.side_effect = query
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None

        with pytest.raises(ScopeBlockedError) as raised:
            plugin.prepare_gather_scope(
                "tenant-1",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 2, tzinfo=UTC),
                uow,
            )

        assert raised.value.result.status == status
        assert raised.value.result.evidence.status.value == status

    def test_target_scope_query_failure_blocks_with_the_expected_selector_detail(self) -> None:
        from core.plugin.protocols import ScopeBlockedError
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()
        source.query.side_effect = MetricsQueryError("prometheus unavailable")
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())

        with pytest.raises(ScopeBlockedError, match="kafka_cluster_id=kraft-a-001"):
            plugin.prepare_gather_scope(
                "tenant-1",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 2, tzinfo=UTC),
                MagicMock(),
            )

    def test_plugin_persists_scope_state_only_through_the_unit_of_work_repository(self) -> None:
        from core.plugin.protocols import ScopeGateDecision, ScopeGateResult
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = MagicMock()
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        blocked_result = ScopeGateResult(
            ScopeGateDecision.BLOCKED,
            "billing-cluster-a",
            "expected Prometheus target label kafka_cluster_id=kraft-a-001",
            status="target_down",
            reason="target_scope_validation",
        )
        recovery_start = datetime(2026, 8, 1, tzinfo=UTC)
        recovery_result = ScopeGateResult(
            ScopeGateDecision.RECOVERY_READY,
            "billing-cluster-a",
            "scope recovered",
            recovery_start=recovery_start,
            recovery_end=datetime(2026, 8, 2, tzinfo=UTC),
            retention_gap_start=datetime(2026, 7, 1, tzinfo=UTC),
            retention_gap_end=recovery_start,
        )

        plugin.persist_scope_blocked("tenant-1", blocked_result, uow)
        plugin.persist_scope_probe("tenant-1", blocked_result, uow)
        plugin.persist_scope_recovery("tenant-1", recovery_result, uow)
        plugin.persist_scope_closed("tenant-1", recovery_result, uow)

        repository = uow.self_managed_kafka_scope_state
        repository.open.assert_called_once()
        open_kwargs = repository.open.call_args.kwargs
        assert open_kwargs["cluster_id"] == "billing-cluster-a"
        assert open_kwargs["metrics_identifier_label"] == "kafka_cluster_id"
        assert open_kwargs["metrics_identifier"] == "kraft-a-001"
        assert "kafka_cluster_id=kraft-a-001" in open_kwargs["detail"]
        assert open_kwargs["status"] == "target_down"
        repository.record_probe.assert_called_once()
        repository.mark_recovering.assert_called_once()
        repository.mark_retention_gap.assert_called_once()
        repository.close.assert_called_once_with(
            "self_managed_kafka",
            "tenant-1",
            "billing-cluster-a",
            recovery_cursor_date=datetime(2026, 8, 2, tzinfo=UTC).date(),
        )
