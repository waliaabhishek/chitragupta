"""Prometheus-based resource and identity discovery for self-managed Kafka."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.metrics.protocol import MetricsQueryError
from core.models import CoreIdentity, CoreResource, Identity, MetricQuery, Resource

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from plugins.self_managed_kafka.config import IdentitySourceConfig
    from plugins.self_managed_kafka.telemetry_aliases import ResolvedTelemetryCatalog
logger = logging.getLogger(__name__)


def _broker_topic_discovery_queries(
    telemetry_catalog: ResolvedTelemetryCatalog | str | None = None,
    metrics_identifier_label: str | None = None,
) -> list[MetricQuery]:
    """Build the independent topic-evidence discovery queries.

    Kafka creates the bytes-in and bytes-out meters lazily, while log-size is
    present for loaded local partitions. Discovery therefore uses their union.
    """
    catalog = None if isinstance(telemetry_catalog, (str, type(None))) else telemetry_catalog
    selector_label = telemetry_catalog if isinstance(telemetry_catalog, str) else metrics_identifier_label
    if selector_label is None:
        raise ValueError("metrics_identifier_label is required")

    families = (
        ("broker_topic_discovery_bytes_in", "kafka_server_brokertopicmetrics_bytesin_total"),
        ("broker_topic_discovery_bytes_out", "kafka_server_brokertopicmetrics_bytesout_total"),
        ("broker_topic_discovery_log_size", "kafka_log_log_size"),
    )
    queries: list[MetricQuery] = []
    for key, family in families:
        metric_name = family if catalog is None else catalog.metric_name(family)
        broker_label = "broker" if catalog is None else catalog.label_name(family, "broker")
        topic_label = "topic" if catalog is None else catalog.label_name(family, "topic")
        expression = f"group by ({broker_label}, {topic_label}) ({metric_name}{{}})"
        if catalog is None:
            queries.append(
                MetricQuery(
                    key=key,
                    query_expression=expression,
                    label_keys=("broker", "topic", selector_label),
                    resource_label=selector_label,
                )
            )
        else:
            queries.append(
                catalog.bind_query(
                    canonical_family=family,
                    key=key,
                    query_expression=expression,
                    canonical_label_keys=("broker", "topic"),
                    passthrough_label_keys=(selector_label,),
                    resource_label=selector_label,
                )
            )
    return queries


def _legacy_broker_topic_discovery_query(
    telemetry_catalog: ResolvedTelemetryCatalog | str | None = None,
    metrics_identifier_label: str | None = None,
) -> MetricQuery:
    """Build the pre-overlay discovery query used when the overlay is disabled."""
    catalog = None if isinstance(telemetry_catalog, (str, type(None))) else telemetry_catalog
    selector_label = telemetry_catalog if isinstance(telemetry_catalog, str) else metrics_identifier_label
    if selector_label is None:
        raise ValueError("metrics_identifier_label is required")
    family = "kafka_server_brokertopicmetrics_bytesin_total"
    metric_name = family if catalog is None else catalog.metric_name(family)
    broker_label = "broker" if catalog is None else catalog.label_name(family, "broker")
    topic_label = "topic" if catalog is None else catalog.label_name(family, "topic")
    expression = f"group by ({broker_label}, {topic_label}) ({metric_name}{{}})"
    if catalog is None:
        return MetricQuery(
            key="broker_topic_discovery",
            query_expression=expression,
            label_keys=("broker", "topic", selector_label),
            resource_label=selector_label,
        )
    return catalog.bind_query(
        canonical_family=family,
        key="broker_topic_discovery",
        query_expression=expression,
        canonical_label_keys=("broker", "topic"),
        passthrough_label_keys=(selector_label,),
        resource_label=selector_label,
    )


def gather_cluster_resource(
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
    broker_count: int,
    region: str | None = None,
    display_name: str | None = None,
) -> Resource:
    """Create the cluster resource (parent of brokers/topics).

    The cluster resource must always be created first since all billing lines
    reference resource_id = cluster_id.
    """
    return CoreResource(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        resource_id=cluster_id,
        resource_type="cluster",
        display_name=display_name or cluster_id,
        parent_id=None,
        created_at=None,
        deleted_at=None,
        last_seen_at=datetime.now(UTC),
        metadata={"broker_count": broker_count, "region": region},
    )


def run_broker_topic_discovery(
    metrics_source: MetricsSource,
    *,
    metrics_identifier_label: str,
    metrics_identifier: str,
    step: timedelta,
    discovery_window_hours: int = 1,
    include_topic_evidence: bool = True,
    telemetry_catalog: ResolvedTelemetryCatalog | None = None,
) -> tuple[frozenset[str], frozenset[str]]:
    """Discover broker and topic labels without deriving any identities."""
    now = datetime.now(UTC)
    queries = (
        _broker_topic_discovery_queries(telemetry_catalog or metrics_identifier_label, metrics_identifier_label)
        if include_topic_evidence
        else [
            _legacy_broker_topic_discovery_query(
                telemetry_catalog or metrics_identifier_label,
                metrics_identifier_label,
            )
        ]
    )
    results = metrics_source.query(
        queries=queries,
        start=now - timedelta(hours=discovery_window_hours),
        end=now,
        step=step,
        resource_id_filter=metrics_identifier,
    )
    missing_keys = [query.key for query in queries if query.key not in results]
    if missing_keys:
        raise MetricsQueryError("Missing required topic discovery result families: " + ", ".join(missing_keys))
    brokers: set[str] = set()
    topics: set[str] = set()
    for rows in (results.get(query.key, []) for query in queries):
        for row in rows:
            if b := row.labels.get("broker"):
                brokers.add(b)
            if t := row.labels.get("topic"):
                topics.add(t)
    return frozenset(brokers), frozenset(topics)


def brokers_to_resources(
    broker_ids: frozenset[str],
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
) -> Iterable[Resource]:
    """Convert a set of broker label values to Resource objects."""
    for broker_id in broker_ids:
        yield CoreResource(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            resource_id=f"{cluster_id}:broker:{broker_id}",
            resource_type="broker",
            display_name=broker_id,
            parent_id=cluster_id,
            created_at=None,
            deleted_at=None,
            last_seen_at=datetime.now(UTC),
            metadata={"cluster_id": cluster_id},
        )


def topics_to_resources(
    topic_names: frozenset[str],
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
) -> Iterable[Resource]:
    """Convert a set of topic label values to Resource objects."""
    for topic_name in topic_names:
        yield CoreResource(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            resource_id=f"{cluster_id}:topic:{topic_name}",
            resource_type="topic",
            display_name=topic_name,
            parent_id=cluster_id,
            created_at=None,
            deleted_at=None,
            last_seen_at=datetime.now(UTC),
            metadata={"cluster_id": cluster_id},
        )


def load_static_identities(
    identity_config: IdentitySourceConfig,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Convert StaticIdentityConfig entries to Identity objects."""
    for static in identity_config.static_identities:
        yield CoreIdentity(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            identity_id=static.identity_id,
            identity_type=static.identity_type,
            display_name=static.display_name or static.identity_id,
            created_at=None,
            deleted_at=None,
            last_seen_at=datetime.now(UTC),
            metadata={"team": static.team} if static.team else {},
        )
