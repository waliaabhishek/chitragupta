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
logger = logging.getLogger(__name__)


def _broker_topic_discovery_queries(metrics_identifier_label: str) -> list[MetricQuery]:
    """Build the independent topic-evidence discovery queries.

    Kafka creates the bytes-in and bytes-out meters lazily, while log-size is
    present for loaded local partitions. Discovery therefore uses their union.
    """
    return [
        MetricQuery(
            key="broker_topic_discovery_bytes_in",
            query_expression="group by (broker, topic) (kafka_server_brokertopicmetrics_bytesin_total{})",
            label_keys=("broker", "topic"),
            resource_label=metrics_identifier_label,
        ),
        MetricQuery(
            key="broker_topic_discovery_bytes_out",
            query_expression="group by (broker, topic) (kafka_server_brokertopicmetrics_bytesout_total{})",
            label_keys=("broker", "topic"),
            resource_label=metrics_identifier_label,
        ),
        MetricQuery(
            key="broker_topic_discovery_log_size",
            query_expression="group by (broker, topic) (kafka_log_log_size{})",
            label_keys=("broker", "topic"),
            resource_label=metrics_identifier_label,
        ),
    ]


def _legacy_broker_topic_discovery_query(metrics_identifier_label: str) -> MetricQuery:
    """Build the pre-overlay discovery query used when the overlay is disabled."""
    return MetricQuery(
        key="broker_topic_discovery",
        query_expression="group by (broker, topic) (kafka_server_brokertopicmetrics_bytesin_total{})",
        label_keys=("broker", "topic"),
        resource_label=metrics_identifier_label,
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
) -> tuple[frozenset[str], frozenset[str]]:
    """Discover broker and topic labels without deriving any identities."""
    now = datetime.now(UTC)
    queries = (
        _broker_topic_discovery_queries(metrics_identifier_label)
        if include_topic_evidence
        else [_legacy_broker_topic_discovery_query(metrics_identifier_label)]
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
