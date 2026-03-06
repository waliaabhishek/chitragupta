"""Default handler for CCloud catch-all product types.

Handles known product types that lack meaningful identity resolution:
- TABLEFLOW_*: Tableflow processing/storage costs
- CLUSTER_LINKING_*: Cluster linking costs

TABLEFLOW types use the default_allocator (SHARED cost type, resource_id identity).
CLUSTER_LINKING types use the cluster_linking_allocator (USAGE cost type, resource_id identity).

Note: truly unknown product types (not listed here) bypass this handler
entirely and go to the orchestrator's fallback UNALLOCATED path.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet, MetricQuery
from plugins.confluent_cloud.allocators.default_allocators import (
    cluster_linking_allocator,
    default_allocator,
)

if TYPE_CHECKING:
    from core.models import Identity, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork

logger = logging.getLogger(__name__)

_DEFAULT_PRODUCT_TYPES: tuple[str, ...] = (
    "TABLEFLOW_DATA_PROCESSED",
    "TABLEFLOW_NUM_TOPICS",
    "TABLEFLOW_STORAGE",
    "CLUSTER_LINKING_PER_LINK",
    "CLUSTER_LINKING_READ",
    "CLUSTER_LINKING_WRITE",
)

_CLUSTER_LINKING_TYPES: frozenset[str] = frozenset(
    {
        "CLUSTER_LINKING_PER_LINK",
        "CLUSTER_LINKING_READ",
        "CLUSTER_LINKING_WRITE",
    }
)


class DefaultHandler:
    """Service handler for known catch-all product types.

    Handles TABLEFLOW_* and CLUSTER_LINKING_* product types that have no
    identity resolution. No resource gathering, identity gathering, or
    metrics collection is performed.

    Does NOT require connection/config since it performs no API calls.
    """

    def __init__(self, ecosystem: str) -> None:
        self._ecosystem = ecosystem

    @property
    def service_type(self) -> str:
        return "default"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _DEFAULT_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        logger.debug("Gathering %s resources for tenant %s (no-op)", self.service_type, tenant_id)
        return iter([])

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
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
        logger.debug("Resolving %s identities resource=%s (no-op)", self.service_type, resource_id)
        return IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        if product_type not in _DEFAULT_PRODUCT_TYPES:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        if product_type in _CLUSTER_LINKING_TYPES:
            return cluster_linking_allocator
        return default_allocator
