"""Flink service handler for CCloud.

Handles Flink product types:
- FLINK_NUM_CFU: CFU-based cost (usage-ratio by statement owner)
- FLINK_NUM_CFUS: Alternate spelling (same allocator)

Unlike other handlers, Flink uses metrics for identity resolution:
metrics identify active statements, then statement resources provide owner info.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator
from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

_LOGGER = logging.getLogger(__name__)
_EPOCH_START = datetime(2000, 1, 1, tzinfo=UTC)

if TYPE_CHECKING:
    from core.models import Identity, IdentityResolution, MetricQuery, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

_FLINK_PRODUCT_TYPES: tuple[str, ...] = (
    "FLINK_NUM_CFU",
    "FLINK_NUM_CFUS",  # Alternate spelling
)

# Map product types to allocator functions.
# CostAllocator is a Protocol — dict values satisfy it via structural typing.
_ALLOCATOR_MAP: dict[str, CostAllocator] = {
    "FLINK_NUM_CFU": flink_cfu_allocator,
    "FLINK_NUM_CFUS": flink_cfu_allocator,
}


class FlinkHandler:
    """Service handler for Flink product types.

    Implements the ServiceHandler protocol for Flink.
    Gathers Flink compute pools and statements via environment enumeration.
    Does not gather identities (Kafka handler gathers org-level identities).
    Resolves identities via metrics (CFU per statement) + statement owner lookup.
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
        # Build region lookup from config (used by gather_resources)
        self._flink_regions: dict[str, tuple[str, str]] = {}
        if config and config.flink:
            for region_config in config.flink:
                key = region_config.region_id.lower().strip()
                self._flink_regions[key] = (
                    region_config.key,
                    region_config.secret.get_secret_value(),
                )

    @property
    def service_type(self) -> str:
        return "flink"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _FLINK_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Resource]:
        """Gather Flink compute pools and statements for all environments.

        Two-phase gathering:
        1. Gather compute pools per environment
        2. For allocatable pools (with regional credentials), gather statements
        """
        from plugins.confluent_cloud.gathering import (
            gather_flink_compute_pools,
            gather_flink_statements,
        )

        if self._connection is None:
            return

        # Find all environments for this tenant
        now = datetime.now(UTC)
        resources, _ = uow.resources.find_by_period(
            ecosystem=self._ecosystem,
            tenant_id=tenant_id,
            start=_EPOCH_START,
            end=now,
        )
        env_ids: list[str] = [r.resource_id for r in resources if r.resource_type == "environment"]

        # Phase 1: Gather compute pools
        pools = list(
            gather_flink_compute_pools(self._connection, self._ecosystem, tenant_id, env_ids, self._flink_regions)
        )
        yield from pools

        # Phase 2: Gather statements from allocatable pools
        # TD-036: Log pools skipped due to missing regional credentials
        allocatable_pools: list[tuple[Resource, str, str]] = []
        for pool in pools:
            region = pool.metadata.get("region", "")
            if region in self._flink_regions:
                api_key, api_secret = self._flink_regions[region]
                allocatable_pools.append((pool, api_key, api_secret))
            elif region:
                _LOGGER.info(
                    "Flink pool %s in region %s skipped: no regional credentials configured",
                    pool.resource_id,
                    region,
                )

        yield from gather_flink_statements(self._ecosystem, tenant_id, allocatable_pools)

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Return empty — Kafka handler gathers all org-level identities.

        Flink statements reference service accounts/users that are gathered
        by the Kafka handler at the org level.
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
        """Resolve identities for a Flink compute pool at billing time.

        Uses metrics to identify active statements, then looks up statement
        owners from resource metadata. Passes stmt_owner_cfu to allocator
        via IdentityResolution.context.
        """
        billing_end = billing_timestamp + billing_duration
        return resolve_flink_identity(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            metrics_data=metrics_data,
            uow=uow,
            ecosystem=self._ecosystem,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return CFU metrics query for Flink product types.

        Flink needs metrics to identify active statements and their CFU usage.
        """
        from core.models import MetricQuery

        return [
            MetricQuery(
                key="confluent_flink_num_cfu",
                query_expression=(
                    'sum by (compute_pool_id, flink_statement_name)(confluent_flink_num_cfu{resource_id=~"lfcp-.+"})'
                ),
                label_keys=("compute_pool_id", "flink_statement_name"),
                resource_label="compute_pool_id",
            )
        ]

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Return allocator function for this product type."""
        allocator = _ALLOCATOR_MAP.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator
