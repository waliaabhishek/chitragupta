"""Self-managed Kafka service handler.

Single handler covering all product types:
- SELF_KAFKA_COMPUTE: fixed compute costs (even split)
- SELF_KAFKA_STORAGE: storage costs (even split)
- SELF_KAFKA_NETWORK_INGRESS: ingress costs (usage ratio)
- SELF_KAFKA_NETWORK_EGRESS: egress costs (usage ratio)
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.models import Identity, IdentityResolution, IdentitySet, MetricQuery, Resource
from plugins.self_managed_kafka.allocators.kafka_allocators import (
    self_kafka_compute_allocator,
    self_kafka_network_allocator,
    self_kafka_storage_allocator,
)

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

_SELF_KAFKA_PRODUCT_TYPES: tuple[str, ...] = (
    "SELF_KAFKA_COMPUTE",
    "SELF_KAFKA_STORAGE",
    "SELF_KAFKA_NETWORK_INGRESS",
    "SELF_KAFKA_NETWORK_EGRESS",
)

# Per-principal bytes metrics for identity resolution and network allocation.
# {} placeholder stripped when no resource filter is applied (cluster-wide).
_BYTES_IN_PER_PRINCIPAL = MetricQuery(
    key="bytes_in_per_principal",
    query_expression="sum by (principal) (increase(kafka_server_brokertopicmetrics_bytesin_total[1h]))",
    label_keys=("principal",),
    resource_label="principal",
)

_BYTES_OUT_PER_PRINCIPAL = MetricQuery(
    key="bytes_out_per_principal",
    query_expression="sum by (principal) (increase(kafka_server_brokertopicmetrics_bytesout_total[1h]))",
    label_keys=("principal",),
    resource_label="principal",
)

_PRINCIPAL_USAGE_METRICS: list[MetricQuery] = [_BYTES_IN_PER_PRINCIPAL, _BYTES_OUT_PER_PRINCIPAL]

_ALLOCATOR_MAP: dict[str, CostAllocator] = {
    "SELF_KAFKA_COMPUTE": self_kafka_compute_allocator,
    "SELF_KAFKA_STORAGE": self_kafka_storage_allocator,
    "SELF_KAFKA_NETWORK_INGRESS": self_kafka_network_allocator,
    "SELF_KAFKA_NETWORK_EGRESS": self_kafka_network_allocator,
}


class SelfManagedKafkaHandler:
    """Service handler for self-managed Kafka clusters.

    Implements the ServiceHandler protocol. Single handler covering all product
    types since all costs flow through one cluster resource.

    Dispatches resource/identity gathering based on resource_source.source
    and identity_source.source configuration.
    """

    def __init__(
        self,
        config: SelfManagedKafkaConfig,
        metrics_source: MetricsSource,
        admin_client: Any = None,
    ) -> None:
        """Initialize handler with config and discovery clients.

        Args:
            config: Plugin configuration.
            metrics_source: Prometheus client for resource/identity discovery.
            admin_client: Kafka AdminClient for resource discovery (optional).
        """
        self._config = config
        self._metrics_source = metrics_source
        self._admin_client = admin_client
        self._ecosystem = "self_managed_kafka"

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _SELF_KAFKA_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Resource]:
        """Gather cluster, brokers, and topics based on resource_source config.

        Cluster resource is always created first, since all billing lines reference
        resource_id = cluster_id.
        """
        from plugins.self_managed_kafka.gathering.prometheus import (
            gather_cluster_resource,
        )

        cluster = gather_cluster_resource(
            ecosystem=self._ecosystem,
            tenant_id=tenant_id,
            cluster_id=self._config.cluster_id,
            broker_count=self._config.broker_count,
            region=self._config.region,
        )
        yield cluster

        if self._config.resource_source.source == "admin_api":
            yield from self._gather_resources_from_admin(tenant_id)
        else:
            yield from self._gather_resources_from_prometheus(tenant_id)

    def _gather_resources_from_prometheus(self, tenant_id: str) -> Iterable[Resource]:
        """Gather brokers and topics from Prometheus metrics."""
        from plugins.self_managed_kafka.gathering.prometheus import (
            gather_brokers_from_metrics,
            gather_topics_from_metrics,
        )

        yield from gather_brokers_from_metrics(
            self._metrics_source, self._ecosystem, tenant_id, self._config.cluster_id
        )
        yield from gather_topics_from_metrics(self._metrics_source, self._ecosystem, tenant_id, self._config.cluster_id)

    def _gather_resources_from_admin(self, tenant_id: str) -> Iterable[Resource]:
        """Gather brokers and topics from Kafka Admin API."""
        from plugins.self_managed_kafka.gathering.admin_api import (
            gather_brokers_from_admin,
            gather_topics_from_admin,
        )

        if self._admin_client is None:
            return

        yield from gather_brokers_from_admin(self._admin_client, self._ecosystem, tenant_id, self._config.cluster_id)
        yield from gather_topics_from_admin(self._admin_client, self._ecosystem, tenant_id, self._config.cluster_id)

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Gather principals/teams based on identity_source config."""
        source = self._config.identity_source.source

        if source == "prometheus":
            yield from self._gather_identities_from_prometheus(tenant_id)
        elif source == "static":
            yield from self._gather_static_identities(tenant_id)
        else:  # "both"
            yield from self._gather_identities_from_prometheus(tenant_id)
            yield from self._gather_static_identities(tenant_id)

    def _gather_identities_from_prometheus(self, tenant_id: str) -> Iterable[Identity]:
        """Gather principals from Prometheus metrics."""
        from plugins.self_managed_kafka.gathering.prometheus import gather_principals_from_metrics

        yield from gather_principals_from_metrics(
            self._metrics_source,
            self._ecosystem,
            tenant_id,
            self._config.identity_source,
        )

    def _gather_static_identities(self, tenant_id: str) -> Iterable[Identity]:
        """Load static identities from config."""
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        yield from load_static_identities(self._config.identity_source, self._ecosystem, tenant_id)

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

        Extracts active principals from billing-window metrics (metrics_data)
        or returns static identities when configured.

        Args:
            tenant_id: The tenant ID.
            resource_id: The cluster ID (should match config.cluster_id).
            billing_timestamp: Start of billing window.
            billing_duration: Length of billing window.
            metrics_data: Per-principal Prometheus metrics for the billing window.
            uow: Unit of work (not used for DB lookup in self-managed plugin).

        Returns:
            IdentityResolution with principals in resource_active and/or metrics_derived.
        """
        resource_active = IdentitySet()
        metrics_derived = IdentitySet()
        tenant_period = IdentitySet()

        source = self._config.identity_source.source

        # Load identities from metrics data (active principals in billing window)
        if source in ("prometheus", "both") and metrics_data:
            from plugins.self_managed_kafka.gathering.prometheus import (
                extract_principals_from_metrics_data,
            )

            for identity in extract_principals_from_metrics_data(
                metrics_data,
                self._ecosystem,
                tenant_id,
                self._config.identity_source,
            ):
                metrics_derived.add(identity)

        # Load static identities into resource_active
        if source in ("static", "both"):
            from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

            for identity in load_static_identities(self._config.identity_source, self._ecosystem, tenant_id):
                resource_active.add(identity)

        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=tenant_period,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return PromQL queries for this product type.

        All types return per-principal bytes metrics for identity discovery.
        Network types also use these for usage-ratio allocation.
        COMPUTE/STORAGE use even split but still need principal data for identity resolution.
        """
        if product_type in _SELF_KAFKA_PRODUCT_TYPES:
            # Only query per-principal metrics if identity source includes prometheus
            if self._config.identity_source.source in ("prometheus", "both"):
                return _PRINCIPAL_USAGE_METRICS
            # Static-only: no metrics needed for allocation or identity resolution
            return []
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Return allocator function for this product type."""
        allocator = _ALLOCATOR_MAP.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator
