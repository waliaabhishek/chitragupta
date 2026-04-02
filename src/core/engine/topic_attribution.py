from __future__ import annotations

import fnmatch
import logging
from collections.abc import Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models.billing import BillingLineItem
    from core.models.metrics import MetricQuery
    from core.models.topic_attribution import TopicAttributionRow
    from core.storage.interface import UnitOfWork

from core.engine.topic_attribution_models import (
    TopicAttributionConfigProtocol,
    TopicAttributionContext,
    resolve_topic_attribution_models,
)

logger = logging.getLogger(__name__)

_QUERY_TEMPLATES: dict[str, str] = {
    "topic_bytes_in": "sum by (kafka_id, topic) ({metric_name}{{}})",
    "topic_bytes_out": "sum by (kafka_id, topic) ({metric_name}{{}})",
    "topic_retained_bytes": "sum by (kafka_id, topic) ({metric_name}{{}})",
}

_DEFAULT_METRIC_NAMES: dict[str, str] = {
    "topic_bytes_in": "confluent_kafka_server_received_bytes",
    "topic_bytes_out": "confluent_kafka_server_sent_bytes",
    "topic_retained_bytes": "confluent_kafka_server_retained_bytes",
}


def build_metric_queries(overrides: dict[str, str]) -> list[MetricQuery]:
    """Build MetricQuery list from default metric names + user overrides."""
    from core.models.metrics import MetricQuery

    merged = {**_DEFAULT_METRIC_NAMES, **overrides}
    queries = []
    for key, metric_name in merged.items():
        template = _QUERY_TEMPLATES[key]
        expression = template.format(metric_name=metric_name)
        value_type = "gauge" if key == "topic_retained_bytes" else "delta_gauge"
        queries.append(
            MetricQuery(
                key=key,
                query_expression=expression,
                label_keys=("kafka_id", "topic"),
                resource_label="kafka_id",
                query_mode="range",
                metadata={"value_type": value_type},
            )
        )
    return queries


def _build_topic_filter(exclude_patterns: list[str]) -> Callable[[str], bool]:
    def _filter(topic_name: str) -> bool:
        return all(not fnmatch.fnmatch(topic_name, p) for p in exclude_patterns)

    return _filter


def _granularity_to_duration(granularity: str) -> timedelta:
    match granularity:
        case "hourly":
            return timedelta(hours=1)
        case _:
            return timedelta(days=1)


class TopicAttributionPhase:
    """Computes topic-level cost attribution from billing + Prometheus metrics."""

    def __init__(
        self,
        ecosystem: str,
        tenant_id: str,
        metrics_source: MetricsSource | None,
        config: TopicAttributionConfigProtocol,
        metrics_step: timedelta,
    ) -> None:
        self._ecosystem = ecosystem
        self._tenant_id = tenant_id
        self._metrics_source = metrics_source
        self._config = config
        self._metrics_step = metrics_step
        self._topic_filter = _build_topic_filter(config.exclude_topic_patterns)
        self._attribution_models = resolve_topic_attribution_models(config.cost_mapping_overrides)
        self._metric_queries = build_metric_queries(config.metric_name_overrides)

    def run(self, uow: UnitOfWork, tracking_date: date) -> int:
        """Compute topic attribution for all Kafka clusters on tracking_date.

        Returns count of attribution rows written.
        """
        billing_lines = uow.billing.find_by_date(self._ecosystem, self._tenant_id, tracking_date)

        clusters: dict[tuple[str, str], list[BillingLineItem]] = {}
        for line in billing_lines:
            if not getattr(line, "resource_id", "").startswith("lkc-"):
                continue
            if line.product_type not in self._attribution_models:
                continue
            env_id = getattr(line, "env_id", "")
            key = (line.resource_id, env_id)
            clusters.setdefault(key, []).append(line)

        all_rows: list[TopicAttributionRow] = []
        infra_failure = False
        for (cluster_id, env_id), lines in clusters.items():
            rows = self._attribute_cluster(uow, cluster_id, env_id, lines, tracking_date)
            if rows is None:
                infra_failure = True
            else:
                all_rows.extend(rows)

        count = 0
        if all_rows:
            count = uow.topic_attributions.upsert_batch(all_rows)

        if not infra_failure:
            uow.pipeline_state.mark_topic_attribution_calculated(
                self._ecosystem,
                self._tenant_id,
                tracking_date,
            )
        return count

    def _attribute_cluster(
        self,
        uow: UnitOfWork,
        cluster_id: str,
        env_id: str,
        billing_lines: list[BillingLineItem],
        tracking_date: date,
    ) -> list[TopicAttributionRow] | None:
        first_line = billing_lines[0]
        b_start = first_line.timestamp
        b_end = b_start + _granularity_to_duration(first_line.granularity)

        all_topics = self._get_cluster_topics(uow, cluster_id, b_start, b_end)
        if not all_topics:
            logger.warning(
                "No topics in resources table for cluster=%s — skipping attribution",
                cluster_id,
            )
            return []

        topic_metrics = self._fetch_topic_metrics(cluster_id, b_start, b_end)
        if topic_metrics is None:
            logger.warning(
                "Skipping attribution for cluster=%s date=%s — metrics infrastructure unavailable",
                cluster_id,
                tracking_date,
            )
            return None

        rows: list[TopicAttributionRow] = []
        for line in billing_lines:
            model = self._attribution_models.get(line.product_type)
            if not model:
                continue

            ctx = TopicAttributionContext(
                ecosystem=self._ecosystem,
                tenant_id=self._tenant_id,
                env_id=env_id,
                cluster_resource_id=cluster_id,
                timestamp=line.timestamp,
                product_category=line.product_category,
                product_type=line.product_type,
                cluster_cost=Decimal(str(line.total_cost)),
                topics=all_topics,
                topic_metrics=topic_metrics,
                config=self._config,
            )
            topic_rows = model.attribute(ctx)
            if topic_rows:
                rows.extend(topic_rows)

        return rows

    def _fetch_topic_metrics(
        self,
        cluster_id: str,
        b_start: datetime,
        b_end: datetime,
    ) -> dict[str, dict[str, float]] | None:
        """Fetch topic-level metrics for a cluster.

        Returns None on infrastructure failure (Prometheus unreachable).
        Returns {} if Prometheus healthy but no data.
        Raises RuntimeError if called with no metrics source configured.
        """
        if not self._metrics_source:
            raise RuntimeError(
                "TopicAttributionPhase._fetch_topic_metrics() called without a metrics_source — "
                "this should have been caught at config validation"
            )

        try:
            raw = self._metrics_source.query(
                queries=self._metric_queries,
                start=b_start,
                end=b_end,
                step=self._metrics_step,
                resource_id_filter=cluster_id,
            )
        except Exception:
            logger.warning(
                "Topic metrics fetch failed for cluster=%s — infrastructure unavailable",
                cluster_id,
                exc_info=True,
            )
            return None

        metric_query_lookup = {mq.key: mq for mq in self._metric_queries}
        result: dict[str, dict[str, float]] = {}

        for metric_key, rows in raw.items():
            mq = metric_query_lookup.get(metric_key)
            is_gauge = mq and mq.metadata.get("value_type") == "gauge"

            if is_gauge:
                topic_max: dict[str, float] = {}
                for row in rows:
                    topic = row.labels.get("topic")
                    if not topic or not self._topic_filter(topic):
                        continue
                    if row.value > topic_max.get(topic, 0.0):
                        topic_max[topic] = row.value
                result[metric_key] = topic_max
            else:
                topic_values: dict[str, float] = {}
                for row in rows:
                    topic = row.labels.get("topic")
                    if not topic or not self._topic_filter(topic):
                        continue
                    topic_values[topic] = topic_values.get(topic, 0.0) + row.value
                result[metric_key] = topic_values

        return result

    def _get_cluster_topics(
        self, uow: UnitOfWork, cluster_id: str, b_start: datetime, b_end: datetime
    ) -> frozenset[str]:
        """Get topic names for a cluster that existed during the billing window [b_start, b_end)."""
        resources, _ = uow.resources.find_by_period(
            self._ecosystem,
            self._tenant_id,
            b_start,
            b_end,
            parent_id=cluster_id,
            resource_type="topic",
            count=False,
        )
        return frozenset(r.display_name for r in resources if r.display_name and self._topic_filter(r.display_name))
