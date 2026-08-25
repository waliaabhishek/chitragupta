"""Topic-level cost attribution backed by self-managed Kafka telemetry."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Literal

from core.engine.topic_attribution_models import (
    TopicAttributionRowOutputContext,
    build_reconciled_topic_rows,
)
from core.engine.topic_attribution_provider import TopicAttributionClusterOutcome
from core.metrics.protocol import MetricsQueryError
from plugins.self_managed_kafka.historical_metrics import (
    bounded_window_chunks,
    collect_daily_evidence,
    validate_utc_day_windows,
)

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricQuery
    from core.models.billing import BillingLineItem
    from core.models.metrics import MetricRow
    from core.models.topic_attribution import TopicAttributionRow
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig, SelfManagedTopicAttributionConfig
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

_BYTES_PER_GIB = Decimal("1073741824")
_CENT = Decimal("0.0001")
_NETWORK_PRODUCT_QUERIES = {
    "SELF_KAFKA_NETWORK_INGRESS": (
        "topic_bytes_in",
        "kafka_server_brokertopicmetrics_bytesin_total",
    ),
    "SELF_KAFKA_NETWORK_EGRESS": (
        "topic_bytes_out",
        "kafka_server_brokertopicmetrics_bytesout_total",
    ),
}
_SUPPORTED_PRODUCT_TYPES = frozenset(
    {
        "SELF_KAFKA_COMPUTE",
        "SELF_KAFKA_STORAGE",
        "SELF_KAFKA_NETWORK_INGRESS",
        "SELF_KAFKA_NETWORK_EGRESS",
    }
)
_INVALID_EVIDENCE_TOPIC = "__invalid_topic_evidence__"


class SelfManagedKafkaTopicAttributionProvider:
    """Allocate self-managed Kafka cost pools using topic-scoped evidence."""

    def __init__(
        self,
        *,
        config: SelfManagedKafkaConfig,
        metrics_source: MetricsSource,
        inventory_is_partitionless: Callable[[], bool],
        telemetry_catalog: ResolvedTelemetryCatalog | None = None,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source
        self._inventory_is_partitionless = inventory_is_partitionless
        if telemetry_catalog is None:
            from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog

            telemetry_catalog = ResolvedTelemetryCatalog(config)
        self._telemetry_catalog = telemetry_catalog
        self._prepared_evidence: dict[tuple[datetime, datetime], dict[str, list[MetricRow]]] = {}

    @property
    def config(self) -> SelfManagedTopicAttributionConfig:
        """Return the validated overlay output configuration."""
        return self._config.topic_attribution

    @property
    def supported_product_types(self) -> frozenset[str]:
        """Return product types this provider owns."""
        return _SUPPORTED_PRODUCT_TYPES

    @property
    def replace_date_on_completion(self) -> bool:
        """Replace the date snapshot once every line reaches a terminal outcome."""
        return True

    def iter_evidence_chunks(
        self,
        windows: Sequence[tuple[datetime, datetime]],
    ) -> tuple[tuple[tuple[datetime, datetime], ...], ...]:
        """Yield bounded contiguous windows in chronological order."""
        validate_utc_day_windows(windows)
        return bounded_window_chunks(windows, self._config.historical_acquisition_chunk_days)

    def prepare_evidence_chunk(
        self,
        windows: Sequence[tuple[datetime, datetime]],
        metrics_step: timedelta,
    ) -> None:
        """Acquire and retain reduced evidence for one bounded chunk."""
        self.clear_evidence_chunk()
        try:
            validate_utc_day_windows(windows)
            queries = [
                self._topic_query(
                    canonical_family=metric_name,
                    key=key,
                    query_expression=(
                        f"sum by ({self._telemetry_catalog.label_name(metric_name, 'topic')}) "
                        f"(increase({self._telemetry_catalog.metric_name(metric_name)}{{}}[86400s]))"
                    ),
                    query_mode="instant",
                )
                for key, metric_name in _NETWORK_PRODUCT_QUERIES.values()
            ]
            queries.append(
                self._topic_query(
                    canonical_family="kafka_log_log_size",
                    key="topic_storage_bytes",
                    query_expression=(
                        f"sum by ({self._telemetry_catalog.label_name('kafka_log_log_size', 'topic')}) "
                        f"({self._telemetry_catalog.metric_name('kafka_log_log_size')}{{}})"
                    ),
                )
            )
            self._prepared_evidence = collect_daily_evidence(
                self._metrics_source,
                queries,
                windows,
                step=metrics_step,
                chunk_days=self._config.historical_acquisition_chunk_days,
                resource_id_filter=self._config.metrics_identifier,
            )
        except Exception:
            self.clear_evidence_chunk()
            raise

    def clear_evidence_chunk(self) -> None:
        """Release reduced evidence retained for the active chunk."""
        self._prepared_evidence.clear()

    def attribute_cluster(
        self,
        *,
        tenant_id: str,
        cluster_resource_id: str,
        env_id: str,
        billing_lines: Sequence[BillingLineItem],
        resource_topics: frozenset[str],
        metrics_step: timedelta,
    ) -> TopicAttributionClusterOutcome:
        """Return terminal rows and retryable measured lines for one cluster."""
        measured_usage: dict[int, tuple[dict[str, Decimal], str]] = {}
        retry_lines: list[BillingLineItem] = []
        active_topics = set(resource_topics)

        for index, line in enumerate(billing_lines):
            try:
                usage, method = self._measured_usage(line, metrics_step)
            except MetricsQueryError:
                retry_lines.append(line)
                continue
            if usage is None or method is None:
                continue
            measured_usage[index] = (usage, method)
            active_topics.update(topic for topic, value in usage.items() if value.is_finite() and value >= 0)

        rows: list[TopicAttributionRow] = []
        for index, line in enumerate(billing_lines):
            context = TopicAttributionRowOutputContext(
                ecosystem="self_managed_kafka",
                tenant_id=tenant_id,
                timestamp=_utc_day_start(line.timestamp),
                env_id=env_id,
                cluster_resource_id=cluster_resource_id,
                product_category=line.product_category,
                product_type=line.product_type,
                cluster_cost=line.total_cost,
            )
            if index in measured_usage:
                usage, method = measured_usage[index]
                rows.extend(
                    build_reconciled_topic_rows(
                        context,
                        cluster_quantity=line.quantity,
                        pool_usage=_pool_usage(line),
                        topic_usage=usage,
                        attribution_method=method,
                        residual_method="incomplete_topic_telemetry",
                    )
                )
            elif line.product_type == "SELF_KAFKA_COMPUTE":
                rows.extend(self._compute_rows(context, line.quantity, active_topics))

        return TopicAttributionClusterOutcome(rows=tuple(rows), retry_lines=tuple(retry_lines))

    def _measured_usage(
        self,
        line: BillingLineItem,
        metrics_step: timedelta,
    ) -> tuple[dict[str, Decimal] | None, str | None]:
        if line.product_type in _NETWORK_PRODUCT_QUERIES:
            key, metric_name = _NETWORK_PRODUCT_QUERIES[line.product_type]
            return self._network_usage(line, metrics_step, key, metric_name), "bytes_ratio"
        if line.product_type == "SELF_KAFKA_STORAGE":
            return self._storage_usage(line, metrics_step), "retained_bytes_ratio"
        return None, None

    def _network_usage(
        self,
        line: BillingLineItem,
        metrics_step: timedelta,
        key: str,
        metric_name: str,
    ) -> dict[str, Decimal]:
        day_start, day_end = _day_bounds(line.timestamp)
        query = self._topic_query(
            canonical_family=metric_name,
            key=key,
            query_expression=(
                f"sum by ({self._telemetry_catalog.label_name(metric_name, 'topic')}) "
                f"(increase({self._telemetry_catalog.metric_name(metric_name)}{{}}[86400s]))"
            ),
            query_mode="instant",
        )
        prepared = self._prepared_evidence.get((day_start, day_end))
        if prepared is not None and key in prepared:
            return _topic_values(prepared[key])
        results = self._metrics_source.query(
            queries=[query],
            start=day_start,
            end=day_end,
            step=metrics_step,
            resource_id_filter=self._config.metrics_identifier,
        )
        return _topic_values(results.get(key, []))

    def _storage_usage(self, line: BillingLineItem, metrics_step: timedelta) -> dict[str, Decimal]:
        day_start, day_end = _day_bounds(line.timestamp)
        query = self._topic_query(
            canonical_family="kafka_log_log_size",
            key="topic_storage_bytes",
            query_expression=(
                f"sum by ({self._telemetry_catalog.label_name('kafka_log_log_size', 'topic')}) "
                f"({self._telemetry_catalog.metric_name('kafka_log_log_size')}{{}})"
            ),
        )
        prepared = self._prepared_evidence.get((day_start, day_end))
        if prepared is not None and query.key in prepared:
            rows = [row for row in prepared[query.key] if day_start <= row.timestamp < day_end]
            if not rows and not self._inventory_is_partitionless():
                raise MetricsQueryError("Missing required storage metric family: kafka_log_log_size")
            return _average_partition_storage(rows)
        results = self._metrics_source.query(
            queries=[query],
            start=day_start,
            end=day_end,
            step=metrics_step,
            resource_id_filter=self._config.metrics_identifier,
        )
        rows = [row for row in results.get(query.key, []) if day_start <= row.timestamp < day_end]
        if not rows and not self._inventory_is_partitionless():
            raise MetricsQueryError("Missing required storage metric family: kafka_log_log_size")
        return _average_partition_storage(rows)

    def _topic_query(
        self,
        *,
        canonical_family: str,
        key: str,
        query_expression: str,
        query_mode: Literal["instant", "range"] = "range",
    ) -> MetricQuery:
        """Bind one topic query's canonical topic dimension to physical PromQL."""
        return self._telemetry_catalog.bind_query(
            canonical_family=canonical_family,
            key=key,
            query_expression=query_expression,
            canonical_label_keys=("topic",),
            passthrough_label_keys=(self._config.metrics_identifier_label,),
            resource_label=self._config.metrics_identifier_label,
            query_mode=query_mode,
        )

    def _compute_rows(
        self,
        context: TopicAttributionRowOutputContext,
        cluster_quantity: Decimal,
        active_topics: set[str],
    ) -> list[TopicAttributionRow]:
        if self.config.compute_policy == "disabled":
            return [_unattributed_row(context, "compute_policy_disabled")]
        if not active_topics:
            return [_unattributed_row(context, "no_topic_inventory")]
        usage = {topic: Decimal(1) for topic in active_topics}
        return build_reconciled_topic_rows(
            context,
            cluster_quantity=cluster_quantity,
            pool_usage=Decimal(len(usage)),
            topic_usage=usage,
            attribution_method="shared_even_v1",
            residual_method="incomplete_topic_telemetry",
        )


def _day_bounds(timestamp: datetime) -> tuple[datetime, datetime]:
    start = _utc_day_start(timestamp)
    return start, start + timedelta(days=1)


def _utc_day_start(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        raise ValueError(f"Naive datetime not allowed: {timestamp!r}")
    return timestamp.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _pool_usage(line: BillingLineItem) -> Decimal:
    if line.product_type in _NETWORK_PRODUCT_QUERIES:
        return line.quantity * _BYTES_PER_GIB
    if line.product_type == "SELF_KAFKA_STORAGE":
        return line.quantity * _BYTES_PER_GIB / Decimal(24)
    raise ValueError(f"Unsupported measured product type: {line.product_type}")


def _topic_values(rows: Sequence[MetricRow]) -> dict[str, Decimal]:
    values: dict[str, Decimal] = {}
    invalid_evidence = False
    for row in rows:
        topic = row.labels.get("topic")
        if topic:
            value = Decimal(str(row.value))
            if not value.is_finite() or value < 0:
                invalid_evidence = True
            else:
                values[topic] = values.get(topic, Decimal(0)) + value
    if invalid_evidence:
        values[_INVALID_EVIDENCE_TOPIC] = Decimal("-1")
    return values


def _average_partition_storage(rows: Sequence[MetricRow]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = {}
    timestamps: set[datetime] = set()
    invalid_evidence = False
    for row in rows:
        topic = row.labels.get("topic")
        if topic:
            value = Decimal(str(row.value))
            if not value.is_finite() or value < 0:
                invalid_evidence = True
                continue
            timestamps.add(row.timestamp)
            totals[topic] = totals.get(topic, Decimal(0)) + value

    sample_count = Decimal(len(timestamps))
    if sample_count == 0:
        return {_INVALID_EVIDENCE_TOPIC: Decimal("-1")} if invalid_evidence else {}
    averages = {topic: total / sample_count for topic, total in totals.items()}
    if invalid_evidence:
        averages[_INVALID_EVIDENCE_TOPIC] = Decimal("-1")
    return averages


def _unattributed_row(
    context: TopicAttributionRowOutputContext,
    method: str,
) -> TopicAttributionRow:
    from core.models.topic_attribution import TopicAttributionRow

    return TopicAttributionRow(
        ecosystem=context.ecosystem,
        tenant_id=context.tenant_id,
        timestamp=context.timestamp,
        env_id=context.env_id,
        cluster_resource_id=context.cluster_resource_id,
        topic_name="__UNATTRIBUTED__",
        product_category=context.product_category,
        product_type=context.product_type,
        attribution_method=method,
        amount=context.cluster_cost.quantize(_CENT, rounding=ROUND_HALF_UP),
    )
