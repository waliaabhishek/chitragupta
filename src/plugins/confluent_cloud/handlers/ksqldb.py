"""ksqlDB service handler for CCloud.

Handles ksqlDB product types:
- KSQL_NUM_CSU: CSU-based cost (even split)
- KSQL_NUM_CSUS: Alternate spelling (same allocator)
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from core.plugin.base import BaseServiceHandler
from plugins.confluent_cloud.allocators.ksqldb_allocators import (
    ksqldb_csu_allocator,
)
from plugins.confluent_cloud.handlers.ksqldb_identity import (
    resolve_ksqldb_identity,
)

if TYPE_CHECKING:
    from core.models import IdentityResolution, MetricQuery, MetricRow, Resource
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork
    from plugins.confluent_cloud.config import CCloudPluginConfig  # noqa: F401
    from plugins.confluent_cloud.connections import CCloudConnection  # noqa: F401

logger = logging.getLogger(__name__)

_KSQLDB_PRODUCT_TYPES: tuple[str, ...] = (
    "KSQL_NUM_CSU",
    "KSQL_NUM_CSUS",  # Alternate spelling
)

# Map product types to allocator functions.
# CostAllocator is a Protocol - dict values satisfy it via structural typing.
_KSQLDB_ALLOCATORS: dict[str, CostAllocator] = {
    "KSQL_NUM_CSU": ksqldb_csu_allocator,
    "KSQL_NUM_CSUS": ksqldb_csu_allocator,
}


class KsqldbHandler(BaseServiceHandler["CCloudConnection | None", "CCloudPluginConfig | None"]):
    """Service handler for ksqlDB product types.

    Implements the ServiceHandler protocol for ksqlDB.
    Gathers ksqlDB clusters via environment enumeration.
    Does not gather identities (Kafka handler gathers org-level identities).
    Resolves identities via direct owner_id lookup (credential_identity from API).
    """

    _ALLOCATOR_MAP = _KSQLDB_ALLOCATORS

    @property
    def service_type(self) -> str:
        return "ksqldb"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _KSQLDB_PRODUCT_TYPES

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather ksqlDB clusters using env_ids from shared context.

        Replaces UoW full-table scan for environment resources.
        """
        logger.debug("Gathering %s resources for tenant %s", self.service_type, tenant_id)
        from plugins.confluent_cloud.gathering import gather_ksqldb_clusters
        from plugins.confluent_cloud.shared_context import CCloudSharedContext

        if self._connection is None or not isinstance(shared_ctx, CCloudSharedContext):
            return

        yield from gather_ksqldb_clusters(self._connection, self._ecosystem, tenant_id, shared_ctx.env_ids)

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
        """Resolve identity for a ksqlDB app at billing time.

        Delegates to resolve_ksqldb_identity which looks up the owner_id
        from the resource's credential_identity field.

        metrics_data is ignored - ksqlDB doesn't use metrics for identity.
        """
        logger.debug(
            "Resolving %s identities resource=%s timestamp=%s", self.service_type, resource_id, billing_timestamp
        )
        billing_end = billing_timestamp + billing_duration
        return resolve_ksqldb_identity(
            tenant_id=tenant_id,
            resource_id=resource_id,
            billing_start=billing_timestamp,
            billing_end=billing_end,
            uow=uow,
            ecosystem=self._ecosystem,
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """Return empty list - ksqlDB doesn't need metrics.

        ksqlDB costs are allocated via even split based on owner,
        not usage metrics.
        """
        return []

    # gather_identities() and get_allocator() inherited from BaseServiceHandler
