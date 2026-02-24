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

from plugins.confluent_cloud.allocators.sr_allocators import schema_registry_allocator
from plugins.confluent_cloud.handlers.identity_resolution import (
    resolve_kafka_sr_identities,
)

if TYPE_CHECKING:
    from core.models import Identity, IdentityResolution, MetricQuery, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

_SR_PRODUCT_TYPES: tuple[str, ...] = ("SCHEMA_REGISTRY", "GOVERNANCE_BASE", "NUM_RULES")


class SchemaRegistryHandler:
    """Service handler for Schema Registry product types.

    Implements the ServiceHandler protocol for Schema Registry.
    Gathers SR clusters as resources (environments yielded by Kafka handler).
    Does NOT gather identities - Kafka handler gathers all org-level identities.
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
        return "schema_registry"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _SR_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Resource]:
        """Gather Schema Registry clusters.

        Note: Does NOT yield environments - KafkaHandler yields those.
        This is intentional handler ordering: Kafka must be iterated first.
        """
        from plugins.confluent_cloud.gathering import (
            gather_environments,
            gather_schema_registries,
        )

        if self._connection is None:
            return

        # Get environment IDs but don't yield environments (Kafka already did)
        env_ids: list[str] = []
        for env in gather_environments(self._connection, self._ecosystem, tenant_id):
            env_ids.append(env.resource_id)

        yield from gather_schema_registries(self._connection, self._ecosystem, tenant_id, env_ids)

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """SR uses same identities as Kafka - intentionally empty.

        Design decision: KafkaHandler gathers all org-scoped identities
        (service accounts, users, API keys). SR doesn't need additional
        identities. Returning empty iterable is intentional coordination,
        not a gap.
        """
        return iter([])

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

    def get_allocator(self, product_type: str) -> CostAllocator:
        """All SR types use the same allocator."""
        if product_type not in _SR_PRODUCT_TYPES:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return schema_registry_allocator
