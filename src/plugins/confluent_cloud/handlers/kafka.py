"""Kafka service handler for CCloud.

Handles all Kafka-related product types:
- KAFKA_NUM_CKU/CKUS: Cluster capacity units (hybrid allocation)
- KAFKA_NETWORK_READ/WRITE: Network I/O (usage-based allocation)
- KAFKA_BASE/PARTITION/STORAGE: Fixed costs (even split)
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.models import MetricQuery
from plugins.confluent_cloud.allocators.kafka_allocators import (
    kafka_base_allocator,
    kafka_network_allocator,
    kafka_num_cku_allocator,
)
from plugins.confluent_cloud.handlers.identity_resolution import (
    resolve_kafka_sr_identities,
)

if TYPE_CHECKING:
    from core.models import Identity, IdentityResolution, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

_KAFKA_PRODUCT_TYPES: tuple[str, ...] = (
    "KAFKA_NUM_CKU",
    "KAFKA_NUM_CKUS",
    "KAFKA_BASE",
    "KAFKA_PARTITION",
    "KAFKA_STORAGE",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
)

# Prometheus metrics for usage-based allocation
# Placeholders {resource_id} and {step} resolved by orchestrator before query
_BYTES_IN_QUERY = (
    "sum by (kafka_id, principal_id)"
    '(increase(confluent_kafka_server_received_bytes{{kafka_id="{resource_id}"}}[{step}]))'
)
_BYTES_OUT_QUERY = (
    'sum by (kafka_id, principal_id)(increase(confluent_kafka_server_sent_bytes{{kafka_id="{resource_id}"}}[{step}]))'
)

_KAFKA_USAGE_METRICS: list[MetricQuery] = [
    MetricQuery(
        key="bytes_in",
        query_expression=_BYTES_IN_QUERY,
        label_keys=("kafka_id", "principal_id"),
        resource_label="kafka_id",
    ),
    MetricQuery(
        key="bytes_out",
        query_expression=_BYTES_OUT_QUERY,
        label_keys=("kafka_id", "principal_id"),
        resource_label="kafka_id",
    ),
]

# Map product types to allocator functions.
# CostAllocator is a Protocol — dict values satisfy it via structural typing.
_ALLOCATOR_MAP: dict[str, CostAllocator] = {
    "KAFKA_NUM_CKU": kafka_num_cku_allocator,
    "KAFKA_NUM_CKUS": kafka_num_cku_allocator,
    "KAFKA_BASE": kafka_base_allocator,
    "KAFKA_PARTITION": kafka_base_allocator,
    "KAFKA_STORAGE": kafka_base_allocator,
    "KAFKA_NETWORK_READ": kafka_network_allocator,
    "KAFKA_NETWORK_WRITE": kafka_network_allocator,
}


class KafkaHandler:
    """Service handler for Kafka product types.

    Implements the ServiceHandler protocol for Kafka clusters.
    Gathers environments and Kafka clusters as resources.
    Gathers service accounts, users, and API keys as identities.
    Resolves identities via API key ownership with temporal filtering.
    """

    def __init__(
        self,
        connection: CCloudConnection | None,
        config: CCloudPluginConfig | None,
        ecosystem: str,
    ) -> None:
        self._connection = connection
        self._config = config
        self._ecosystem = ecosystem

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _KAFKA_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Resource]:
        """Gather Kafka clusters (and environments).

        Also yields environments as resources since Kafka is typically
        the first handler iterated. Other handlers skip environment yielding.
        """
        from plugins.confluent_cloud.gathering import (
            gather_environments,
            gather_kafka_clusters,
        )

        if self._connection is None:
            return

        env_ids: list[str] = []
        for env in gather_environments(self._connection, self._ecosystem, tenant_id):
            yield env  # Environment is also a resource
            env_ids.append(env.resource_id)

        yield from gather_kafka_clusters(self._connection, self._ecosystem, tenant_id, env_ids)

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Gather org-level identities (SAs, users, API keys).

        Kafka handler gathers ALL org-scoped identities since they're
        shared across service types. Other handlers (SR, ksqlDB, etc.)
        can skip identity gathering to avoid duplicates.
        """
        from plugins.confluent_cloud.gathering import (
            gather_api_keys,
            gather_service_accounts,
            gather_users,
        )

        if self._connection is None:
            return

        yield from gather_service_accounts(self._connection, self._ecosystem, tenant_id)
        yield from gather_users(self._connection, self._ecosystem, tenant_id)
        yield from gather_api_keys(self._connection, self._ecosystem, tenant_id)

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution:
        """Resolve identities for a Kafka cluster at billing time.

        Uses temporal filtering: only API keys that existed during the
        billing window are considered. Metrics principal IDs are extracted
        and resolved to identities or sentinels.
        """
        billing_end = billing_timestamp + billing_duration
        return resolve_kafka_sr_identities(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            metrics_data=metrics_data,
            uow=uow,
            ecosystem=self._ecosystem,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return PromQL queries for this product type.

        CKU and network types need bytes_in/bytes_out metrics.
        Base, partition, and storage types don't need metrics (even split).
        """
        if product_type in (
            "KAFKA_NUM_CKU",
            "KAFKA_NUM_CKUS",
            "KAFKA_NETWORK_READ",
            "KAFKA_NETWORK_WRITE",
        ):
            return _KAFKA_USAGE_METRICS
        # Base, partition, storage don't need metrics
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Return allocator function for this product type."""
        allocator = _ALLOCATOR_MAP.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator
