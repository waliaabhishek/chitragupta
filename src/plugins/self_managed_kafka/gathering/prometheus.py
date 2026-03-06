"""Prometheus-based resource and identity discovery for self-managed Kafka."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.models import Identity, MetricQuery, Resource

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from plugins.self_managed_kafka.config import IdentitySourceConfig
logger = logging.getLogger(__name__)

# PromQL expressions for resource/identity discovery.
# {} placeholder is replaced by _inject_resource_filter when a filter is needed.
# For cluster-wide queries, no filter is used (pass resource_id_filter=None).
_BROKERS_QUERY = MetricQuery(
    key="distinct_brokers",
    query_expression="group by (broker) (kafka_server_brokertopicmetrics_bytesin_total{})",
    label_keys=("broker",),
    resource_label="broker",
)

_TOPICS_QUERY = MetricQuery(
    key="distinct_topics",
    query_expression="group by (topic) (kafka_server_brokertopicmetrics_bytesin_total{})",
    label_keys=("topic",),
    resource_label="topic",
)

PRINCIPALS_QUERY = MetricQuery(
    key="distinct_principals",
    query_expression="group by (principal) (kafka_server_brokertopicmetrics_bytesin_total{})",
    label_keys=("principal",),
    resource_label="principal",
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
    return Resource(
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


def gather_brokers_from_metrics(
    metrics_source: MetricsSource,
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
    step: timedelta = timedelta(hours=1),
) -> Iterable[Resource]:
    """Query Prometheus for distinct broker labels → Resource objects."""
    now = datetime.now(UTC)
    # Query a short window to discover current brokers
    start = now - timedelta(hours=1)

    results = metrics_source.query(
        queries=[_BROKERS_QUERY],
        start=start,
        end=now,
        step=step,
    )

    seen_brokers: set[str] = set()
    for row in results.get("distinct_brokers", []):
        broker_id = row.labels.get("broker")
        if broker_id and broker_id not in seen_brokers:
            seen_brokers.add(broker_id)
            yield Resource(
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


def gather_topics_from_metrics(
    metrics_source: MetricsSource,
    ecosystem: str,
    tenant_id: str,
    cluster_id: str,
    step: timedelta = timedelta(hours=1),
) -> Iterable[Resource]:
    """Query Prometheus for distinct topic labels → Resource objects."""
    now = datetime.now(UTC)
    start = now - timedelta(hours=1)

    results = metrics_source.query(
        queries=[_TOPICS_QUERY],
        start=start,
        end=now,
        step=step,
    )

    seen_topics: set[str] = set()
    for row in results.get("distinct_topics", []):
        topic_name = row.labels.get("topic")
        if topic_name and topic_name not in seen_topics:
            seen_topics.add(topic_name)
            yield Resource(
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


def gather_principals_from_metrics(
    metrics_source: MetricsSource,
    ecosystem: str,
    tenant_id: str,
    identity_config: IdentitySourceConfig,
    step: timedelta = timedelta(hours=1),
) -> Iterable[Identity]:
    """Query Prometheus for distinct principal labels → Identity objects."""
    now = datetime.now(UTC)
    start = now - timedelta(hours=1)

    results = metrics_source.query(
        queries=[PRINCIPALS_QUERY],
        start=start,
        end=now,
        step=step,
    )

    seen_principals: set[str] = set()
    for row in results.get("distinct_principals", []):
        principal_id = row.labels.get("principal")
        if principal_id and principal_id not in seen_principals:
            seen_principals.add(principal_id)
            yield from _make_principal_identity(principal_id, ecosystem, tenant_id, identity_config)


def _make_principal_identity(
    principal_id: str,
    ecosystem: str,
    tenant_id: str,
    identity_config: IdentitySourceConfig,
) -> Iterable[Identity]:
    """Create an Identity from a principal ID, applying team mapping if configured."""
    team_name = identity_config.principal_to_team.get(principal_id, identity_config.default_team)
    yield Identity(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        identity_id=principal_id,
        identity_type="principal",
        display_name=team_name if team_name != identity_config.default_team else principal_id,
        created_at=None,
        deleted_at=None,
        last_seen_at=datetime.now(UTC),
        metadata={"raw_principal": principal_id, "team": team_name},
    )


def load_static_identities(
    identity_config: IdentitySourceConfig,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Convert StaticIdentityConfig entries to Identity objects."""
    for static in identity_config.static_identities:
        yield Identity(
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


def extract_principals_from_metrics_data(
    metrics_data: dict[str, list[MetricRow]],
    ecosystem: str,
    tenant_id: str,
    identity_config: IdentitySourceConfig,
) -> Iterable[Identity]:
    """Extract distinct principals from metrics_data (billing-window metrics).

    Used in resolve_identities() to find principals active during billing window.
    """
    seen: set[str] = set()
    for rows in metrics_data.values():
        for row in rows:
            principal_id = row.labels.get("principal")
            if principal_id and principal_id not in seen:
                seen.add(principal_id)
                yield from _make_principal_identity(principal_id, ecosystem, tenant_id, identity_config)
