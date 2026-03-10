"""Kafka service handler for CCloud.

Handles all Kafka-related product types:
- KAFKA_NUM_CKU/CKUS: Cluster capacity units (hybrid allocation)
- KAFKA_NETWORK_READ/WRITE: Network I/O (usage-based allocation)
- KAFKA_BASE/PARTITION/STORAGE: Fixed costs (even split)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.models import MetricQuery
from core.plugin.base import BaseServiceHandler
from plugins.confluent_cloud.allocators.kafka_allocators import (
    kafka_base_allocator,
    kafka_cku_allocator,
    kafka_network_read_allocator,
    kafka_network_write_allocator,
    kafka_partition_allocator,
)
from plugins.confluent_cloud.handlers.identity_resolution import (
    resolve_kafka_sr_identities,
)

if TYPE_CHECKING:
    from core.models import Identity, IdentityResolution, MetricRow, Resource
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork

logger = logging.getLogger(__name__)

_KAFKA_PRODUCT_TYPES: tuple[str, ...] = (
    "KAFKA_NUM_CKU",
    "KAFKA_NUM_CKUS",
    "KAFKA_BASE",
    "KAFKA_PARTITION",
    "KAFKA_STORAGE",
    "KAFKA_NETWORK_READ",
    "KAFKA_NETWORK_WRITE",
)

# Prometheus metrics for usage-based allocation.
# {} placeholder is replaced by _inject_resource_filter with {kafka_id="lkc-xxx"}.
# Reference uses request_bytes (produce/write) and response_bytes (consume/read).
_BYTES_IN_QUERY = "sum by (kafka_id, principal_id) (confluent_kafka_server_request_bytes{})"
_BYTES_OUT_QUERY = "sum by (kafka_id, principal_id) (confluent_kafka_server_response_bytes{})"

# bytes_out = response bytes = read direction (consume)
_KAFKA_READ_METRICS: list[MetricQuery] = [
    MetricQuery(
        key="bytes_out",
        query_expression=_BYTES_OUT_QUERY,
        label_keys=("kafka_id", "principal_id"),
        resource_label="kafka_id",
        query_mode="range",  # range: sum delta values across billing window
    ),
]

# bytes_in = request bytes = write direction (produce)
_KAFKA_WRITE_METRICS: list[MetricQuery] = [
    MetricQuery(
        key="bytes_in",
        query_expression=_BYTES_IN_QUERY,
        label_keys=("kafka_id", "principal_id"),
        resource_label="kafka_id",
        query_mode="range",  # range: sum delta values across billing window
    ),
]

# CKU uses both directions (blended compute)
_KAFKA_CKU_METRICS: list[MetricQuery] = _KAFKA_READ_METRICS + _KAFKA_WRITE_METRICS

# Map product types to allocator functions.
# CostAllocator is a Protocol — dict values satisfy it via structural typing.
_KAFKA_ALLOCATORS: dict[str, CostAllocator] = {
    "KAFKA_NUM_CKU": kafka_cku_allocator,
    "KAFKA_NUM_CKUS": kafka_cku_allocator,
    "KAFKA_BASE": kafka_base_allocator,
    "KAFKA_PARTITION": kafka_partition_allocator,
    "KAFKA_STORAGE": kafka_base_allocator,
    "KAFKA_NETWORK_READ": kafka_network_read_allocator,
    "KAFKA_NETWORK_WRITE": kafka_network_write_allocator,
}


class KafkaHandler(BaseServiceHandler["CCloudConnection | None", "CCloudPluginConfig | None"]):
    """Service handler for Kafka product types.

    Implements the ServiceHandler protocol for Kafka clusters.
    Gathers environments and Kafka clusters as resources.
    Gathers service accounts, users, and API keys as identities.
    Resolves identities via API key ownership with temporal filtering.
    """

    _ALLOCATOR_MAP = _KAFKA_ALLOCATORS

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _KAFKA_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Yield environment and Kafka cluster resources from shared context.

        Environments and clusters are pre-fetched by the plugin's build_shared_context().
        This handler yields them to UoW; it no longer fetches them independently.
        """
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None or not isinstance(shared_ctx, CCloudSharedContext):
            return

        yield from shared_ctx.environment_resources
        yield from shared_ctx.kafka_cluster_resources

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Gather org-level identities (SAs, users, API keys).

        Kafka handler gathers ALL org-scoped identities since they're
        shared across service types. Other handlers (SR, ksqlDB, etc.)
        can skip identity gathering to avoid duplicates.
        """
        logger.debug("Gathering %s identities for tenant %s", self.service_type, tenant_id)
        from plugins.confluent_cloud.gathering import (
            gather_api_keys,
            gather_identity_pools,
            gather_identity_providers,
            gather_service_accounts,
            gather_users,
        )

        if self._connection is None:
            return

        yield from gather_service_accounts(self._connection, self._ecosystem, tenant_id)
        yield from gather_users(self._connection, self._ecosystem, tenant_id)
        yield from gather_api_keys(self._connection, self._ecosystem, tenant_id)

        # NEW: identity providers + pools
        providers = list(gather_identity_providers(self._connection, self._ecosystem, tenant_id))
        yield from providers
        provider_ids = [p.identity_id for p in providers]
        yield from gather_identity_pools(self._connection, self._ecosystem, tenant_id, provider_ids)

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

        Uses temporal filtering: only API keys that existed during the
        billing window are considered. Metrics principal IDs are extracted
        and resolved to identities or sentinels.
        """
        logger.debug(
            "Resolving %s identities resource=%s timestamp=%s",
            self.service_type,
            resource_id,
            billing_timestamp,
        )
        cached_identities = context.get("cached_identities") if context else None
        billing_end = billing_timestamp + billing_duration
        return resolve_kafka_sr_identities(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            metrics_data=metrics_data,
            uow=uow,
            ecosystem=self._ecosystem,
            cached_identities=cached_identities,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return PromQL queries for this product type.

        Direction-specific for network types:
        - KAFKA_NETWORK_READ: bytes_out (response/consume)
        - KAFKA_NETWORK_WRITE: bytes_in (request/produce)
        - KAFKA_NUM_CKU/CKUS: both (blended compute)
        """
        match product_type:
            case "KAFKA_NETWORK_READ":
                return _KAFKA_READ_METRICS
            case "KAFKA_NETWORK_WRITE":
                return _KAFKA_WRITE_METRICS
            case "KAFKA_NUM_CKU" | "KAFKA_NUM_CKUS":
                return _KAFKA_CKU_METRICS
            case _:
                return []

    # get_allocator() inherited from BaseServiceHandler
