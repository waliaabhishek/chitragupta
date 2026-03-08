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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.models import MetricQuery
from core.plugin.base import BaseServiceHandler
from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator
from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.models import IdentityResolution, MetricRow, Resource
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig
    from plugins.confluent_cloud.connections import CCloudConnection

_FLINK_METRICS_PRIMARY = MetricQuery(
    key="flink_cfu_primary",
    query_expression="sum by (compute_pool_id, flink_statement_name)(confluent_flink_num_cfu{})",
    label_keys=("compute_pool_id", "flink_statement_name"),
    resource_label="compute_pool_id",
    query_mode="instant",
)
_FLINK_METRICS_FALLBACK = MetricQuery(
    key="flink_cfu_fallback",
    query_expression=(
        "sum by (compute_pool_id, flink_statement_name)(confluent_flink_statement_utilization_cfu_minutes_consumed{})"
    ),
    label_keys=("compute_pool_id", "flink_statement_name"),
    resource_label="compute_pool_id",
    query_mode="instant",
)

_FLINK_PRODUCT_TYPES: tuple[str, ...] = (
    "FLINK_NUM_CFU",
    "FLINK_NUM_CFUS",  # Alternate spelling
)

# Map product types to allocator functions.
# CostAllocator is a Protocol — dict values satisfy it via structural typing.
_FLINK_ALLOCATORS: dict[str, CostAllocator] = {
    "FLINK_NUM_CFU": flink_cfu_allocator,
    "FLINK_NUM_CFUS": flink_cfu_allocator,
}


class FlinkHandler(BaseServiceHandler["CCloudConnection | None", "CCloudPluginConfig | None"]):
    """Service handler for Flink product types.

    Implements the ServiceHandler protocol for Flink.
    Gathers Flink compute pools and statements via environment enumeration.
    Does not gather identities (Kafka handler gathers org-level identities).
    Resolves identities via metrics (CFU per statement) + statement owner lookup.
    """

    _ALLOCATOR_MAP = _FLINK_ALLOCATORS

    def __init__(
        self,
        connection: CCloudConnection | None,
        config: CCloudPluginConfig | None,
        ecosystem: str,
    ) -> None:
        super().__init__(connection, config, ecosystem)
        # Flink-specific: build region lookup from config
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

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather Flink compute pools and statements using env_ids from shared context.

        Replaces UoW full-table scan for environment resources.
        Two-phase internal gathering preserved: pools first, then statements.
        """
        from plugins.confluent_cloud.gathering import (
            gather_flink_compute_pools,
            gather_flink_statements,
        )
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None or not isinstance(shared_ctx, CCloudSharedContext):
            return

        pools = list(
            gather_flink_compute_pools(
                self._connection,
                self._ecosystem,
                tenant_id,
                shared_ctx.env_ids,
                self._flink_regions,
            )
        )
        yield from pools

        allocatable_pools: list[tuple[Resource, str, str]] = []
        for pool in pools:
            region = pool.metadata.get("region", "")
            if region in self._flink_regions:
                api_key, api_secret = self._flink_regions[region]
                allocatable_pools.append((pool, api_key, api_secret))
            elif region:
                logger.info(
                    "Flink pool %s in region %s skipped: no regional credentials configured",
                    pool.resource_id,
                    region,
                )

        yield from gather_flink_statements(self._ecosystem, tenant_id, allocatable_pools)

    # gather_identities() inherited from BaseServiceHandler (returns empty iterable)

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
        """Resolve identities for a Flink compute pool at billing time.

        Uses metrics to identify active statements, then looks up statement
        owners from resource metadata. Passes stmt_owner_cfu to allocator
        via IdentityResolution.context.
        """
        cached_resources = context.get("cached_resources") if context else None
        billing_end = billing_timestamp + billing_duration
        return resolve_flink_identity(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            metrics_data=metrics_data,
            uow=uow,
            ecosystem=self._ecosystem,
            cached_resources=cached_resources,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return CFU metrics queries for Flink product types.

        Returns primary (new metric name) and fallback (legacy metric name) so
        tenants still exporting the legacy metric name are covered.
        """
        return [_FLINK_METRICS_PRIMARY, _FLINK_METRICS_FALLBACK]

    # get_allocator() inherited from BaseServiceHandler
