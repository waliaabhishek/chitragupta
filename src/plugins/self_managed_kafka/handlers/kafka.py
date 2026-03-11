"""Self-managed Kafka service handler.

Single handler covering all product types:
- SELF_KAFKA_COMPUTE: fixed compute costs (even split)
- SELF_KAFKA_STORAGE: storage costs (even split)
- SELF_KAFKA_NETWORK_INGRESS: ingress costs (usage ratio)
- SELF_KAFKA_NETWORK_EGRESS: egress costs (usage ratio)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from core.engine.helpers import allocate_evenly_with_fallback
from core.models import Identity, IdentityResolution, IdentitySet, MetricQuery, Resource
from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL, SMK_INGRESS_MODEL

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

logger = logging.getLogger(__name__)


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
    "SELF_KAFKA_COMPUTE": allocate_evenly_with_fallback,
    "SELF_KAFKA_STORAGE": allocate_evenly_with_fallback,
    "SELF_KAFKA_NETWORK_INGRESS": SMK_INGRESS_MODEL,
    "SELF_KAFKA_NETWORK_EGRESS": SMK_EGRESS_MODEL,
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
        prometheus_principals_available: bool = True,
    ) -> None:
        """Initialize handler with config and discovery clients.

        Args:
            config: Plugin configuration.
            metrics_source: Prometheus client for resource/identity discovery.
            admin_client: Kafka AdminClient for resource discovery (optional).
            prometheus_principals_available: Whether 'principal' label is present in
                Prometheus metrics. Set to False when validation fails at startup.
                Defaults to True (optimistic) to preserve existing tests that don't pass the flag.
        """
        self._config = config
        self._metrics_source = metrics_source
        self._admin_client = admin_client
        self._prometheus_principals_available = prometheus_principals_available
        self._ecosystem = "self_managed_kafka"
        self._current_gather_ctx: SMKSharedContext | None = None

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _SELF_KAFKA_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather cluster, brokers, and topics.

        Cluster resource comes from shared_ctx (pre-built in Phase 1).
        Broker and topic gathering proceeds as before via admin_api or Prometheus.
        """
        logger.debug("Gathering %s resources for tenant %s", self.service_type, tenant_id)
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        if not isinstance(shared_ctx, SMKSharedContext):
            return

        self._current_gather_ctx = shared_ctx
        yield shared_ctx.cluster_resource

        if self._config.resource_source.source == "admin_api":
            yield from self._gather_resources_from_admin(tenant_id)
        else:
            yield from self._gather_resources_from_prometheus(tenant_id)

    def _gather_resources_from_prometheus(self, tenant_id: str) -> Iterable[Resource]:
        """Gather brokers and topics from cached discovery sets in shared context."""
        ctx = self._current_gather_ctx
        if ctx is None or ctx.discovered_brokers is None or ctx.discovered_topics is None:
            return

        from plugins.self_managed_kafka.gathering.prometheus import brokers_to_resources, topics_to_resources

        yield from brokers_to_resources(ctx.discovered_brokers, self._ecosystem, tenant_id, self._config.cluster_id)
        yield from topics_to_resources(ctx.discovered_topics, self._ecosystem, tenant_id, self._config.cluster_id)

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
        """Gather principals/teams based on identity_source config.

        When _prometheus_principals_available is False and source includes Prometheus,
        falls back to static identities if configured. If no static identities are
        configured, the Prometheus path is still attempted (costs will go to UNALLOCATED).
        """
        logger.debug("Gathering %s identities for tenant %s", self.service_type, tenant_id)
        source = self._config.identity_source.source
        use_prometheus = source in ("prometheus", "both") and self._prometheus_principals_available

        if use_prometheus:
            yield from self._gather_identities_from_prometheus(tenant_id)

        if source in ("static", "both") or (not use_prometheus and self._config.identity_source.static_identities):
            yield from self._gather_static_identities(tenant_id)

    def _gather_identities_from_prometheus(self, tenant_id: str) -> Iterable[Identity]:
        """Gather principals from cached discovery sets in shared context."""
        ctx = self._current_gather_ctx
        if ctx is None or ctx.discovered_principals is None:
            return

        from plugins.self_managed_kafka.gathering.prometheus import principals_to_identities

        yield from principals_to_identities(
            ctx.discovered_principals, self._ecosystem, tenant_id, self._config.identity_source
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
        context: ResolveContext | None = None,
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
        logger.debug(
            "Resolving %s identities resource=%s timestamp=%s", self.service_type, resource_id, billing_timestamp
        )
        resource_active = IdentitySet()
        metrics_derived = IdentitySet()
        tenant_period = IdentitySet()

        source = self._config.identity_source.source
        use_prometheus = source in ("prometheus", "both") and self._prometheus_principals_available

        # Load identities from metrics data (active principals in billing window)
        if use_prometheus and metrics_data:
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
        if source in ("static", "both") or (not use_prometheus and self._config.identity_source.static_identities):
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
