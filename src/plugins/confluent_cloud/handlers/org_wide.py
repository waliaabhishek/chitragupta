"""Org-wide cost handler for CCloud.

Handles org-level product types that are not tied to any specific resource:
- AUDIT_LOG_READ: Audit log ingestion costs
- SUPPORT: Support plan costs

These costs are split evenly across all tenant-period identities by the
org_wide_allocator. The handler performs no resource gathering or identity
resolution — the orchestrator injects tenant_period identities.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.models import IdentityResolution, IdentitySet, MetricQuery
from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

if TYPE_CHECKING:
    from core.models import Identity, MetricRow, Resource
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork

_ORG_WIDE_PRODUCT_TYPES: tuple[str, ...] = (
    "AUDIT_LOG_READ",
    "SUPPORT",
)


class OrgWideCostHandler:
    """Service handler for org-wide product types (AUDIT_LOG_READ, SUPPORT).

    No resource gathering or identity resolution is performed by this handler.
    The orchestrator injects tenant_period identities from the Kafka handler's
    identity resolution pass. Costs are split evenly across all identities.
    """

    def __init__(self, ecosystem: str) -> None:
        self._ecosystem = ecosystem

    @property
    def service_type(self) -> str:
        return "org_wide"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _ORG_WIDE_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
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
        return IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return []

    def get_allocator(self, product_type: str) -> CostAllocator:
        if product_type not in _ORG_WIDE_PRODUCT_TYPES:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return org_wide_allocator
