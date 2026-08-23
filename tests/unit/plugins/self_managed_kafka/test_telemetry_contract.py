"""Behavioral tests for self-managed Kafka telemetry evidence."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from math import nan
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import httpx
import pytest

from core.metrics.prometheus import PrometheusConfig, PrometheusMetricsSource
from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery, MetricRow

if TYPE_CHECKING:
    from core.storage.interface import UnitOfWork


def _settings(
    *,
    identity_source: dict[str, object] | None = None,
    principal_attribution: dict[str, object] | None = None,
) -> dict[str, object]:
    settings: dict[str, object] = {
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
    if principal_attribution is not None:
        settings["principal_attribution"] = principal_attribution
    return settings


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


def _complete_direction_rows(start: datetime, quota_type: str) -> list[MetricRow]:
    return [
        MetricRow(
            timestamp=start + timedelta(hours=offset),
            metric_key="quota_byte_rate",
            value=1.0,
            labels={
                "kafka_cluster_id": "kraft-a-001",
                "broker": "1",
                "quota_type": quota_type,
                "quota_scope": "user",
                "user": "alice",
                "client_id": "not_applicable",
            },
        )
        for offset in range(25)
    ]


class _MetricsSource:
    def __init__(
        self,
        response: Callable[
            [Sequence[MetricQuery], datetime, datetime, timedelta, str | None],
            dict[str, list[MetricRow]],
        ],
    ) -> None:
        self._response = response
        self.calls: list[tuple[tuple[MetricQuery, ...], datetime, datetime, timedelta, str | None]] = []

    def close(self) -> None:
        pass

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        definitions = tuple(queries)
        self.calls.append((definitions, start, end, step, resource_id_filter))
        return self._response(definitions, start, end, step, resource_id_filter)


def _unit_of_work() -> UnitOfWork:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelUnitOfWork
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    return SQLModelUnitOfWork("sqlite://", SelfManagedKafkaStorageModule())


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


def _expected_timestamps(start: datetime, end: datetime, step: timedelta) -> tuple[datetime, ...]:
    timestamps: list[datetime] = []
    timestamp = start
    while timestamp <= end:
        timestamps.append(timestamp)
        timestamp += step
    return tuple(timestamps)


class TestPrincipalTelemetryEvidence:
    @pytest.mark.parametrize("identity_source", [{"source": "prometheus"}, {"source": "both"}])
    @pytest.mark.parametrize("principal_attribution", [None, {"enabled": False}])
    def test_disabled_and_omitted_preserve_readiness_probe(
        self,
        identity_source: dict[str, object],
        principal_attribution: dict[str, object] | None,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        source = _MetricsSource(
            lambda *_: {
                "quota_byte_rate": [_quota_row("quota_byte_rate", 8192.0)],
                "quota_throttle_time_ms": [_quota_row("quota_throttle_time_ms", 0.0)],
            }
        )
        config = SelfManagedKafkaConfig.from_plugin_settings(
            _settings(
                identity_source=identity_source,
                principal_attribution=principal_attribution,
            )
        )
        handler = SelfManagedKafkaHandler(config, source)

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            datetime(2026, 8, 1, tzinfo=UTC),
            timedelta(days=1),
            None,
            _unit_of_work(),
        )

        queries = source.calls[0][0]
        assert [query.key for query in queries] == ["quota_byte_rate", "quota_throttle_time_ms"]
        assert resolution.context["principal_attribution_status"] == "observed"
        assert resolution.context["principal_attribution_detail"] == "quota_identity_observed"
        assert resolution.context["measured_usage"] is False

    def test_enabled_scope_block_suppresses_quota_acquisition_before_any_metrics_query(self) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.telemetry_contract import MetricsScopeEvidence, MetricsScopeStatus

        start = datetime(2026, 8, 1, tzinfo=UTC)
        scope = MetricsScopeEvidence(
            label="kafka_cluster_id",
            identifier="kraft-a-001",
            window_start=start,
            window_end=start + timedelta(days=1),
            status=MetricsScopeStatus.TARGET_DOWN,
            detail="target down",
        )
        source = _MetricsSource(lambda *_: (_ for _ in ()).throw(AssertionError("scope block queried Prometheus")))
        config = SelfManagedKafkaConfig.from_plugin_settings(
            _settings(
                identity_source={"source": "both"},
                principal_attribution={"enabled": True, "scrape_interval_seconds": 5, "max_gap_seconds": 10},
            )
        )
        handler = SelfManagedKafkaHandler(config, source, metrics_scope_evidence=lambda *_: scope)

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            start,
            timedelta(days=1),
            None,
            _unit_of_work(),
        )

        assert resolution.context["principal_attribution_status"] == "unavailable"
        assert resolution.context["principal_attribution_detail"] == "target_scope_blocked"
        assert source.calls == []

    def test_enabled_quota_acquisition_keeps_embedded_selectors_without_resource_filter_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.telemetry_contract import MetricsScopeEvidence, MetricsScopeStatus

        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        observed_queries: list[str] = []

        def post(_url: str, *, data: dict[str, str], headers: dict[str, str]) -> MagicMock:
            del headers
            query = data["query"]
            observed_queries.append(query)
            quota_type = "Produce" if 'quota_type="Produce"' in query else "Fetch"
            return MagicMock(
                status_code=200,
                text=json.dumps(
                    {
                        "status": "success",
                        "data": {
                            "resultType": "matrix",
                            "result": [
                                {
                                    "metric": {
                                        "broker": "1",
                                        "kafka_cluster_id": "kraft-a-001",
                                        "quota_type": quota_type,
                                        "quota_scope": "user",
                                        "user": "alice",
                                        "client_id": "not_applicable",
                                    },
                                    "values": [
                                        [(start + timedelta(hours=offset)).timestamp(), "1"] for offset in range(25)
                                    ],
                                }
                            ],
                        },
                    }
                ),
            )

        client = MagicMock(spec=httpx.Client)
        client.post.side_effect = post
        source = PrometheusMetricsSource(
            PrometheusConfig(url="http://prometheus:9090", max_retries=1, max_workers=1),
            client=client,
        )
        config = SelfManagedKafkaConfig.from_plugin_settings(
            _settings(
                identity_source={"source": "both"},
                principal_attribution={"enabled": True, "scrape_interval_seconds": 3600, "max_gap_seconds": 7200},
            )
        )
        scope = MetricsScopeEvidence(
            "kafka_cluster_id",
            "kraft-a-001",
            start,
            end,
            MetricsScopeStatus.VALID,
            "target healthy",
        )
        handler = SelfManagedKafkaHandler(config, source, metrics_scope_evidence=lambda *_: scope)

        with caplog.at_level(logging.WARNING, logger="core.metrics.prometheus"):
            resolution = handler.resolve_identities(
                "tenant-1",
                "billing-cluster-a",
                start,
                timedelta(days=1),
                None,
                _unit_of_work(),
            )

        assert observed_queries == [
            'kafka_server_quota_byte_rate{kafka_cluster_id="kraft-a-001",quota_type="Produce"}[93600s]',
            'kafka_server_quota_byte_rate{kafka_cluster_id="kraft-a-001",quota_type="Fetch"}[93600s]',
        ]
        assert resolution.context["principal_attribution_status"] == "observed"
        assert resolution.metrics_derived.ids() == frozenset({"User:alice"})
        assert not any("no {} placeholder" in record.getMessage() for record in caplog.records)

    @pytest.mark.parametrize(
        ("failed_quota_type", "surviving_quota_type", "expected_direction"),
        [
            ("Produce", "Fetch", "egress"),
            ("Fetch", "Produce", "ingress"),
        ],
    )
    def test_one_direction_query_failure_does_not_discard_the_other_direction(
        self,
        failed_quota_type: str,
        surviving_quota_type: str,
        expected_direction: str,
    ) -> None:
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.telemetry_contract import MetricsScopeEvidence, MetricsScopeStatus

        start = datetime(2026, 8, 1, tzinfo=UTC)

        def query(
            queries: Sequence[MetricQuery],
            _start: datetime,
            _end: datetime,
            _step: timedelta,
            _resource_id_filter: str | None,
        ) -> dict[str, list[MetricRow]]:
            assert len(queries) == 1
            definition = queries[0]
            if f'quota_type="{failed_quota_type}"' in definition.query_expression:
                raise MetricsQueryError(f"{failed_quota_type} unavailable")
            if f'quota_type="{surviving_quota_type}"' in definition.query_expression:
                return {definition.key: _complete_direction_rows(start, surviving_quota_type)}
            raise AssertionError(f"unexpected query {definition.query_expression}")

        source = _MetricsSource(query)
        config = SelfManagedKafkaConfig.from_plugin_settings(
            _settings(
                identity_source={"source": "both"},
                principal_attribution={"enabled": True, "scrape_interval_seconds": 3600, "max_gap_seconds": 7200},
            )
        )
        scope = MetricsScopeEvidence(
            "kafka_cluster_id",
            "kraft-a-001",
            start,
            start + timedelta(days=1),
            MetricsScopeStatus.VALID,
            "target healthy",
        )
        handler = SelfManagedKafkaHandler(config, source, metrics_scope_evidence=lambda *_: scope)

        resolution = handler.resolve_identities(
            "tenant-1",
            "billing-cluster-a",
            start,
            timedelta(days=1),
            None,
            _unit_of_work(),
        )

        assert resolution.metrics_derived.ids() == frozenset({"User:alice"})
        assert resolution.context["principal_attribution_status"] == "unavailable"
        evidence = resolution.context["principal_telemetry_evidence"]
        assert getattr(evidence, expected_direction).state.value == "ready"
        assert len(source.calls) == 2

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

        broker_wide_lines = [
            line for line in source.splitlines() if line.startswith("kafka_server_brokertopicmetrics_alltopics_")
        ]
        topic_lines = [
            line
            for line in source.splitlines()
            if line.startswith(
                (
                    "kafka_server_brokertopicmetrics_bytesin_total{",
                    "kafka_server_brokertopicmetrics_bytesout_total{",
                )
            )
        ]
        assert broker_wide_lines
        assert topic_lines
        assert all('kafka_cluster_id="kraft-a-001"' in line for line in broker_wide_lines + topic_lines)
        assert all("topic=" not in line for line in broker_wide_lines)
        assert all('topic="shared-topic"' in line for line in topic_lines)
        assert all("principal=" not in line for line in broker_wide_lines + topic_lines)

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
    def test_scope_validation_uses_bounded_queries_for_a_long_historical_window(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        settings = _settings()
        settings["historical_acquisition_chunk_days"] = 30

        def response(
            queries: Sequence[MetricQuery],
            start: datetime,
            end: datetime,
            step: timedelta,
            resource_id_filter: str | None,
        ) -> dict[str, list[MetricRow]]:
            assert resource_id_filter == "kraft-a-001"
            return {
                query.key: [
                    MetricRow(
                        timestamp=timestamp,
                        metric_key=query.key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-a-001"},
                    )
                    for timestamp in _expected_timestamps(start, end, step)
                ]
                for query in queries
            }

        source = _MetricsSource(response)
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(settings)
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None
        start = datetime(2025, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=364)

        plugin.begin_scope_gate_run()
        result = plugin.prepare_gather_scope("tenant-1", start, end, uow)

        assert result.decision is ScopeGateDecision.ALLOW
        assert len(source.calls) == 13
        assert all(call[2] - call[1] <= timedelta(days=30) for call in source.calls)
        assert all(len(_expected_timestamps(call[1], call[2], call[3])) <= 721 for call in source.calls)

    def test_scope_validation_reuses_only_an_exact_run_local_scope_key(self) -> None:
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = _MetricsSource(
            lambda queries, start, end, step, resource_id_filter: {
                query.key: [
                    MetricRow(
                        timestamp=timestamp,
                        metric_key=query.key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-a-001"},
                    )
                    for timestamp in _expected_timestamps(start, end, step)
                ]
                for query in queries
            }
        )
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)

        plugin.begin_scope_gate_run()
        plugin.prepare_gather_scope("tenant-1", start, end, uow)
        plugin.prepare_gather_scope("tenant-1", start, end, uow)

        assert len(source.calls) == 1
        assert uow.self_managed_kafka_scope_state.get.call_count == 2

    def test_scope_validation_key_changes_when_each_scope_component_changes(self) -> None:
        from plugins.self_managed_kafka.telemetry_contract import MetricsScopeRequest

        start = datetime(2026, 8, 1, tzinfo=UTC)
        base = MetricsScopeRequest(
            tenant_id="tenant-a",
            metrics_identifier="kraft-a-001",
            metrics_identifier_label="kafka_cluster_id",
            step=timedelta(hours=1),
            start=start,
            end=start + timedelta(days=1),
        )
        variants = (
            MetricsScopeRequest(
                "tenant-b", base.metrics_identifier, base.metrics_identifier_label, base.step, base.start, base.end
            ),
            MetricsScopeRequest(
                base.tenant_id, "kraft-b-002", base.metrics_identifier_label, base.step, base.start, base.end
            ),
            MetricsScopeRequest(base.tenant_id, base.metrics_identifier, "deployment", base.step, base.start, base.end),
            MetricsScopeRequest(
                base.tenant_id,
                base.metrics_identifier,
                base.metrics_identifier_label,
                timedelta(minutes=30),
                base.start,
                base.end,
            ),
            MetricsScopeRequest(
                base.tenant_id,
                base.metrics_identifier,
                base.metrics_identifier_label,
                base.step,
                base.start + timedelta(hours=1),
                base.end,
            ),
            MetricsScopeRequest(
                base.tenant_id,
                base.metrics_identifier,
                base.metrics_identifier_label,
                base.step,
                base.start,
                base.end + timedelta(hours=1),
            ),
        )

        assert len({base, *variants}) == 7

    def test_scope_cache_behavior_includes_each_request_component(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        def response(
            queries: Sequence[MetricQuery],
            start: datetime,
            end: datetime,
            step: timedelta,
            resource_id_filter: str | None,
        ) -> dict[str, list[MetricRow]]:
            assert resource_id_filter is not None
            return {
                query.key: [
                    MetricRow(
                        timestamp=timestamp,
                        metric_key=query.key,
                        value=1.0,
                        labels={query.resource_label: resource_id_filter} if query.resource_label is not None else {},
                    )
                    for timestamp in _expected_timestamps(start, end, step)
                ]
                for query in queries
            }

        source = _MetricsSource(response)
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = None
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        config = plugin._config
        assert config is not None

        plugin.begin_scope_gate_run()
        baseline = plugin.prepare_gather_scope("tenant-a", start, end, uow)
        plugin.prepare_gather_scope("tenant-a", start, end, uow)
        config.metrics_identifier = "kraft-b-002"
        identifier_variant = plugin.prepare_gather_scope("tenant-a", start, end, uow)
        config.metrics_identifier = "kraft-a-001"
        config.metrics_identifier_label = "deployment"
        label_variant = plugin.prepare_gather_scope("tenant-a", start, end, uow)
        config.metrics_identifier_label = "kafka_cluster_id"
        config.metrics_step_seconds = 1800
        step_variant = plugin.prepare_gather_scope("tenant-a", start, end, uow)
        config.metrics_step_seconds = 3600
        start_variant = plugin.prepare_gather_scope("tenant-a", start + timedelta(hours=1), end, uow)
        end_variant = plugin.prepare_gather_scope("tenant-a", start, end + timedelta(hours=1), uow)
        tenant_variant = plugin.prepare_gather_scope("tenant-b", start, end, uow)

        assert baseline.decision is ScopeGateDecision.ALLOW
        assert all(
            result.decision is ScopeGateDecision.ALLOW
            for result in (identifier_variant, label_variant, step_variant, start_variant, end_variant, tenant_variant)
        )
        assert len(source.calls) == 7

    def test_open_scope_uses_a_single_newest_point_probe_before_full_recovery_validation(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = _MetricsSource(
            lambda queries, start, end, step, resource_id_filter: {
                query.key: [
                    MetricRow(
                        timestamp=start,
                        metric_key=query.key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-a-001"},
                    )
                ]
                for query in queries
            }
        )
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = start + timedelta(days=1)
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
            status="open",
            first_blocked_window_start=start,
            recovery_cursor_date=start.date(),
        )

        plugin.begin_scope_gate_run()
        result = plugin.prepare_gather_scope("tenant-1", start, end, uow)

        assert result.decision is ScopeGateDecision.RECOVERY_READY
        assert source.calls == [
            (
                source.calls[0][0],
                end,
                end,
                timedelta(hours=1),
                "kraft-a-001",
            )
        ]

    def test_open_calculation_scope_probes_newest_point_once_for_multiple_windows(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = _MetricsSource(
            lambda queries, start, end, step, resource_id_filter: {
                query.key: [
                    MetricRow(
                        timestamp=end,
                        metric_key=query.key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-a-001"},
                    )
                ]
                for query in queries
            }
        )
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        start = datetime(2026, 8, 1, tzinfo=UTC)
        windows = (
            (start, start + timedelta(days=1)),
            (start + timedelta(days=3), start + timedelta(days=4)),
        )
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.return_value = MagicMock(
            status="open",
            first_blocked_window_start=start,
            recovery_cursor_date=start.date(),
        )

        plugin.begin_scope_gate_run()
        result = plugin.prepare_calculation_scope("tenant-1", windows, uow)

        assert result.decision is ScopeGateDecision.RECOVERY_READY
        assert len(source.calls) == 1
        assert source.calls[0][1:] == (
            windows[1][1],
            windows[1][1],
            timedelta(hours=1),
            "kraft-a-001",
        )

    def test_one_point_recovery_evidence_is_reused_by_full_validation(self) -> None:
        from core.plugin.protocols import ScopeGateDecision
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        source = _MetricsSource(
            lambda queries, start, end, step, resource_id_filter: {
                query.key: [
                    MetricRow(
                        timestamp=end,
                        metric_key=query.key,
                        value=1.0,
                        labels={"kafka_cluster_id": "kraft-a-001"},
                    )
                ]
                for query in queries
            }
        )
        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=source):
            plugin.initialize(_settings())
        point = datetime(2026, 8, 1, tzinfo=UTC)
        state = MagicMock(status="open", first_blocked_window_start=point, recovery_cursor_date=point.date())
        recovering = MagicMock(status="recovering", first_blocked_window_start=point, recovery_cursor_date=point.date())
        uow = MagicMock()
        uow.self_managed_kafka_scope_state.get.side_effect = [state, recovering, recovering]

        plugin.begin_scope_gate_run()
        probe = plugin.prepare_gather_scope("tenant-1", point, point, uow)
        validated = plugin.prepare_calculation_scope("tenant-1", [(point, point)], uow)

        assert probe.decision is ScopeGateDecision.RECOVERY_READY
        assert validated.decision is ScopeGateDecision.ALLOW
        assert len(source.calls) == 1

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
            if key.startswith("broker_topic_discovery"):
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
