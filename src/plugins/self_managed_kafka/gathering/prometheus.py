"""Prometheus-based resource and identity discovery for self-managed Kafka."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.models import CoreIdentity, CoreResource, Identity, MetricQuery, Resource

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from plugins.self_managed_kafka.config import IdentitySourceConfig
logger = logging.getLogger(__name__)

# Combined discovery query — single round-trip to discover brokers, topics, and principals.
_COMBINED_DISCOVERY_QUERY = MetricQuery(
    key="combined_discovery",
    query_expression="group by (broker, topic, principal) (kafka_server_brokertopicmetrics_bytesin_total{})",
    label_keys=("broker", "topic", "principal"),
    resource_label=None,
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


def run_combined_discovery(
    metrics_source: MetricsSource,
    step: timedelta,
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Issue a single query to discover brokers, topics, and principals simultaneously.

    Returns:
        (brokers, topics, principals) as frozensets of label values.
        Empty string values are excluded; missing labels are skipped.
    """
    now = datetime.now(UTC)
    results = metrics_source.query(
        queries=[_COMBINED_DISCOVERY_QUERY],
        start=now - timedelta(hours=1),
        end=now,
        step=step,
    )
    brokers: set[str] = set()
    topics: set[str] = set()
    principals: set[str] = set()
    for row in results.get("combined_discovery", []):
        if b := row.labels.get("broker"):
            brokers.add(b)
        if t := row.labels.get("topic"):
            topics.add(t)
        if p := row.labels.get("principal"):
            principals.add(p)
    return frozenset(brokers), frozenset(topics), frozenset(principals)


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


def principals_to_identities(
    principal_ids: frozenset[str],
    ecosystem: str,
    tenant_id: str,
    identity_config: IdentitySourceConfig,
) -> Iterable[Identity]:
    """Convert a set of principal label values to Identity objects."""
    for principal_id in principal_ids:
        yield from _make_principal_identity(principal_id, ecosystem, tenant_id, identity_config)


def _make_principal_identity(
    principal_id: str,
    ecosystem: str,
    tenant_id: str,
    identity_config: IdentitySourceConfig,
) -> Iterable[Identity]:
    """Create an Identity from a principal ID, applying team mapping if configured."""
    team_name = identity_config.principal_to_team.get(principal_id, identity_config.default_team)
    yield CoreIdentity(
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
