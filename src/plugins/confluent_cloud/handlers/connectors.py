"""Connector service handler for CCloud.

Handles all Kafka Connect-related product types:
- CONNECT_CAPACITY: Infrastructure cost (even split)
- CONNECT_NUM_TASKS: Task-based cost (even split)
- CONNECT_THROUGHPUT: Throughput-based cost (even split)
- CUSTOM_CONNECT_NUM_TASKS: Custom connector task-based cost (even split)
- CUSTOM_CONNECT_THROUGHPUT: Custom connector throughput cost (even split)
- CUSTOM_CONNECT_PLUGIN: Custom plugin cost (infrastructure, even split)
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from plugins.confluent_cloud.allocators.connector_allocators import (
    connect_capacity_allocator,
    connect_tasks_allocator,
    connect_throughput_allocator,
)
from plugins.confluent_cloud.handlers.connector_identity import (
    resolve_connector_identity,
)

if TYPE_CHECKING:
    from core.models import Identity, IdentityResolution, MetricQuery, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

_CONNECTOR_PRODUCT_TYPES: tuple[str, ...] = (
    "CONNECT_CAPACITY",
    "CONNECT_NUM_TASKS",
    "CONNECT_THROUGHPUT",
    "CUSTOM_CONNECT_PLUGIN",
    "CUSTOM_CONNECT_NUM_TASKS",
    "CUSTOM_CONNECT_THROUGHPUT",
)

# Map product types to allocator functions.
# CostAllocator is a Protocol — dict values satisfy it via structural typing.
_ALLOCATOR_MAP: dict[str, CostAllocator] = {
    "CONNECT_CAPACITY": connect_capacity_allocator,
    "CONNECT_NUM_TASKS": connect_tasks_allocator,
    "CONNECT_THROUGHPUT": connect_throughput_allocator,
    "CUSTOM_CONNECT_NUM_TASKS": connect_tasks_allocator,
    "CUSTOM_CONNECT_THROUGHPUT": connect_tasks_allocator,
    "CUSTOM_CONNECT_PLUGIN": connect_capacity_allocator,  # Infrastructure cost
}


class ConnectorHandler:
    """Service handler for Kafka Connect product types.

    Implements the ServiceHandler protocol for connectors.
    Gathers connectors via Kafka cluster enumeration.
    Does not gather identities (Kafka handler gathers org-level identities).
    Resolves identities via connector auth mode (SERVICE_ACCOUNT or KAFKA_API_KEY).
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
        return "connector"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _CONNECTOR_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather connectors for all Kafka clusters via shared context.

        Replaces UoW full-table scan for kafka_cluster resources. Cluster list
        comes from build_shared_context(), which fetched it in Phase 1.
        """
        from plugins.confluent_cloud.gathering import gather_connectors
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None or not isinstance(shared_ctx, CCloudSharedContext):
            return

        yield from gather_connectors(self._connection, self._ecosystem, tenant_id, shared_ctx.kafka_cluster_pairs)

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Return empty — Kafka handler gathers all org-level identities.

        Connectors don't have their own identity types. They reference
        service accounts or API keys that are gathered by the Kafka handler.
        """
        yield from ()

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution:
        """Resolve identity for a connector at billing time.

        Delegates to resolve_connector_identity which inspects the connector's
        auth mode (SERVICE_ACCOUNT or KAFKA_API_KEY) to determine the owner.
        metrics_data is ignored — connectors don't use metrics for identity.
        """
        billing_end = billing_timestamp + billing_duration
        return resolve_connector_identity(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            uow=uow,
            ecosystem=self._ecosystem,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return empty list — connectors don't need metrics.

        Connect costs are allocated via even split based on connector ownership,
        not usage metrics.
        """
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Return allocator function for this product type."""
        allocator = _ALLOCATOR_MAP.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator
