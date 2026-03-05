"""Schema Registry service handler for CCloud.

Handles Schema Registry product types:
- SCHEMA_REGISTRY: Base SR costs
- GOVERNANCE_BASE: Stream governance base costs
- NUM_RULES: Schema rules/constraints

All use even split allocation - no metrics needed.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.plugin.base import BaseServiceHandler
from plugins.confluent_cloud.allocators.sr_allocators import schema_registry_allocator
from plugins.confluent_cloud.handlers.identity_resolution import (
    resolve_kafka_sr_identities,
)

if TYPE_CHECKING:
    from core.models import IdentityResolution, MetricQuery, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork

_SR_PRODUCT_TYPES: tuple[str, ...] = ("SCHEMA_REGISTRY", "GOVERNANCE_BASE", "NUM_RULES")

_SR_ALLOCATORS: dict[str, CostAllocator] = {
    "SCHEMA_REGISTRY": schema_registry_allocator,
    "GOVERNANCE_BASE": schema_registry_allocator,
    "NUM_RULES": schema_registry_allocator,
}


class SchemaRegistryHandler(BaseServiceHandler["CCloudConnection | None", "CCloudPluginConfig | None"]):
    """Service handler for Schema Registry product types.

    Implements the ServiceHandler protocol for Schema Registry.
    Gathers SR clusters as resources (environments yielded by Kafka handler).
    Does NOT gather identities - Kafka handler gathers all org-level identities.
    Resolves identities via API key ownership with temporal filtering.
    """

    _ALLOCATOR_MAP = _SR_ALLOCATORS

    @property
    def service_type(self) -> str:
        return "schema_registry"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _SR_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather Schema Registry clusters using env_ids from shared context.

        No longer calls gather_environments() directly — eliminates the redundant
        API round-trip that occurred because this handler could not trust UoW ordering.
        """
        from plugins.confluent_cloud.gathering import gather_schema_registries
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None or not isinstance(shared_ctx, CCloudSharedContext):
            return

        yield from gather_schema_registries(self._connection, self._ecosystem, tenant_id, shared_ctx.env_ids)

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution:
        """Resolve identities for SR - same logic as Kafka.

        Uses temporal filtering: only API keys that existed during the
        billing window are considered.
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
        """SR doesn't need metrics - uses even split."""
        return []

    # gather_identities() and get_allocator() inherited from BaseServiceHandler
