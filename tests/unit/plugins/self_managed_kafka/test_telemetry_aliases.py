"""Tests for self-managed Kafka Prometheus telemetry aliases."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from core.models import MetricQuery, MetricRow


def _config(
    *,
    metric_name_overrides: dict[str, str] | None = None,
    label_name_overrides: dict[str, dict[str, str]] | None = None,
    metrics_identifier_label: str = "kafka_cluster_id",
) -> object:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    settings: dict[str, object] = {
        "cluster_id": "billing-cluster-a",
        "metrics_identifier": "kraft-a-001",
        "metrics_identifier_label": metrics_identifier_label,
        "broker_count": 3,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "metrics": {"url": "http://prometheus:9090"},
    }
    if metric_name_overrides is not None:
        settings["metric_name_overrides"] = metric_name_overrides
    if label_name_overrides is not None:
        settings["label_name_overrides"] = label_name_overrides
    return SelfManagedKafkaConfig.from_plugin_settings(settings)


class _RecordingMetricsSource:
    """Complete MetricsSource double retaining calls and physical rows."""

    def __init__(self, rows: dict[str, list[MetricRow]]) -> None:
        self.rows = rows
        self.calls: list[tuple[tuple[MetricQuery, ...], datetime, datetime, timedelta, str | None]] = []
        self.closed = False

    def query(
        self,
        queries: Sequence[MetricQuery],
        start: datetime,
        end: datetime,
        step: timedelta = timedelta(hours=1),
        resource_id_filter: str | None = None,
    ) -> dict[str, list[MetricRow]]:
        self.calls.append((tuple(queries), start, end, step, resource_id_filter))
        return {query.key: self.rows.get(query.key, []) for query in queries}

    def close(self) -> None:
        self.closed = True


def test_catalog_uses_all_canonical_defaults_when_overrides_are_omitted() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

    catalog = ResolvedTelemetryCatalog(_config())

    canonical_families = (
        "up",
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total",
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total",
        "kafka_log_log_size",
        "kafka_server_brokertopicmetrics_bytesin_total",
        "kafka_server_brokertopicmetrics_bytesout_total",
        "kafka_server_quota_byte_rate",
        "kafka_server_quota_throttle_time_ms",
    )
    assert {family: catalog.metric_name(family) for family in canonical_families} == {
        family: family for family in canonical_families
    }
    assert catalog.label_name("kafka_log_log_size", "broker") == "broker"
    assert catalog.label_name("kafka_server_quota_byte_rate", "client_id") == "client_id"


def test_catalog_overlays_partial_aliases_without_changing_unconfigured_names() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

    catalog = ResolvedTelemetryCatalog(
        _config(
            metric_name_overrides={"kafka_log_log_size": "company_kafka_partition_size"},
            label_name_overrides={
                "kafka_log_log_size": {
                    "broker": "node",
                    "topic": "topic_name",
                }
            },
        )
    )

    assert catalog.metric_name("kafka_log_log_size") == "company_kafka_partition_size"
    assert catalog.metric_name("kafka_server_brokertopicmetrics_bytesin_total") == (
        "kafka_server_brokertopicmetrics_bytesin_total"
    )
    assert catalog.label_name("kafka_log_log_size", "broker") == "node"
    assert catalog.label_name("kafka_log_log_size", "topic") == "topic_name"
    assert catalog.label_name("kafka_log_log_size", "partition") == "partition"


def test_catalog_resolves_complete_metric_and_label_override_inventory() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

    metric_overrides = {
        "up": "smk_up",
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total": "smk_alltopics_in",
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total": "smk_alltopics_out",
        "kafka_log_log_size": "smk_log_size",
        "kafka_server_brokertopicmetrics_bytesin_total": "smk_topic_in",
        "kafka_server_brokertopicmetrics_bytesout_total": "smk_topic_out",
        "kafka_server_quota_byte_rate": "smk_quota_rate",
        "kafka_server_quota_throttle_time_ms": "smk_quota_throttle",
    }
    label_overrides = {
        "kafka_server_brokertopicmetrics_alltopics_bytesin_total": {"broker": "node"},
        "kafka_server_brokertopicmetrics_alltopics_bytesout_total": {"broker": "node"},
        "kafka_log_log_size": {"broker": "node", "topic": "topic_name", "partition": "partition_number"},
        "kafka_server_brokertopicmetrics_bytesin_total": {"broker": "node", "topic": "topic_name"},
        "kafka_server_brokertopicmetrics_bytesout_total": {"broker": "node", "topic": "topic_name"},
        "kafka_server_quota_byte_rate": {
            "broker": "node",
            "quota_type": "quota_kind",
            "quota_scope": "quota_scope_name",
            "user": "principal_name",
            "client_id": "client_name",
        },
        "kafka_server_quota_throttle_time_ms": {
            "broker": "node",
            "quota_type": "quota_kind",
            "quota_scope": "quota_scope_name",
            "user": "principal_name",
            "client_id": "client_name",
        },
    }

    catalog = ResolvedTelemetryCatalog(
        _config(metric_name_overrides=metric_overrides, label_name_overrides=label_overrides)
    )

    assert {family: catalog.metric_name(family) for family in metric_overrides} == metric_overrides
    assert catalog.family("up").canonical_to_physical_labels == {}
    assert catalog.family("kafka_log_log_size").canonical_to_physical_labels == {
        "broker": "node",
        "topic": "topic_name",
        "partition": "partition_number",
    }
    assert catalog.family("kafka_server_quota_byte_rate").canonical_to_physical_labels == {
        "broker": "node",
        "quota_type": "quota_kind",
        "quota_scope": "quota_scope_name",
        "user": "principal_name",
        "client_id": "client_name",
    }


def test_bound_query_uses_physical_promql_labels_and_retains_canonicalization_metadata() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

    catalog = ResolvedTelemetryCatalog(
        _config(
            metric_name_overrides={"kafka_server_brokertopicmetrics_bytesin_total": "company_topic_in"},
            label_name_overrides={
                "kafka_server_brokertopicmetrics_bytesin_total": {"broker": "node", "topic": "topic_name"}
            },
            metrics_identifier_label="deployment",
        )
    )

    query = catalog.bind_query(
        canonical_family="kafka_server_brokertopicmetrics_bytesin_total",
        key="topic_bytes_in",
        query_expression="group by (node, topic_name) (company_topic_in{})",
        canonical_label_keys=("broker", "topic"),
        passthrough_label_keys=("deployment",),
        resource_label="deployment",
        query_mode="instant",
        metadata={"window": "daily"},
    )

    assert query.query_expression == "group by (node, topic_name) (company_topic_in{})"
    assert query.label_keys == ("node", "topic_name", "deployment")
    assert query.resource_label == "deployment"
    assert query.query_mode == "instant"
    assert query.metadata["window"] == "daily"


def test_canonicalizing_source_normalizes_bound_physical_labels_without_changing_raw_provenance() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource, ResolvedTelemetryCatalog

    catalog = ResolvedTelemetryCatalog(
        _config(
            label_name_overrides={
                "kafka_log_log_size": {
                    "broker": "node",
                    "topic": "topic_name",
                    "partition": "partition_number",
                }
            },
            metrics_identifier_label="deployment",
        )
    )
    query = catalog.bind_query(
        canonical_family="kafka_log_log_size",
        key="topic_storage_bytes",
        query_expression="sum by (topic_name) (kafka_log_log_size{})",
        canonical_label_keys=("broker", "topic", "partition"),
        passthrough_label_keys=("deployment",),
        resource_label="deployment",
    )
    physical_row = MetricRow(
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        metric_key="topic_storage_bytes",
        value=42.0,
        labels={"node": "1", "topic_name": "orders", "partition_number": "0", "deployment": "kafka-prod"},
        source_series=(
            ("__name__", "kafka_log_log_size"),
            ("deployment", "kafka-prod"),
            ("node", "1"),
            ("partition_number", "0"),
            ("topic_name", "orders"),
        ),
    )
    raw_source = _RecordingMetricsSource({"topic_storage_bytes": [physical_row]})
    source = CanonicalizingMetricsSource(raw_source)

    result = source.query(
        [query],
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 2, tzinfo=UTC),
        step=timedelta(hours=1),
        resource_id_filter="kafka-prod",
    )

    [row] = result["topic_storage_bytes"]
    assert row.metric_key == "topic_storage_bytes"
    assert row.labels == {"broker": "1", "topic": "orders", "partition": "0", "deployment": "kafka-prod"}
    assert row.source_series == physical_row.source_series
    assert raw_source.calls == [
        (
            (query,),
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
            timedelta(hours=1),
            "kafka-prod",
        )
    ]


def test_canonicalizing_source_leaves_queries_without_alias_metadata_unchanged_and_closes_owned_source() -> None:
    from plugins.self_managed_kafka.telemetry_aliases import CanonicalizingMetricsSource

    query = MetricQuery(
        key="compatibility_query",
        query_expression="other_metric{}",
        label_keys=("node",),
        resource_label="deployment",
    )
    original = MetricRow(
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        metric_key="compatibility_query",
        value=3.0,
        labels={"node": "1"},
        source_series=(("node", "1"),),
    )
    raw_source = _RecordingMetricsSource({"compatibility_query": [original]})
    source = CanonicalizingMetricsSource(raw_source)

    result = source.query(
        [query],
        start=datetime(2026, 8, 1, tzinfo=UTC),
        end=datetime(2026, 8, 1, 1, tzinfo=UTC),
    )
    source.close()

    assert result == {"compatibility_query": [original]}
    assert raw_source.closed is True
