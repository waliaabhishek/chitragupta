"""Default and cluster-linking allocators for CCloud cost distribution.

Default allocator: assigns full cost to the resource itself as SHARED.
Used for product types with no meaningful identity resolution
(TABLEFLOW_DATA_PROCESSED, TABLEFLOW_NUM_TOPICS, TABLEFLOW_STORAGE).

Cluster-linking allocator: assigns full cost to the resource itself as USAGE.
Used for CLUSTER_LINKING_* product types where cost is direct resource
usage attributed to the resource.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.engine.allocation import AllocationResult
from core.engine.helpers import make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail
from plugins.confluent_cloud.constants import CLUSTER_LINKING_COST

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext
logger = logging.getLogger(__name__)


def default_allocator(ctx: AllocationContext) -> AllocationResult:
    """Assign full cost to the resource itself.

    Used for product types with no identity resolution
    (TABLEFLOW_DATA_PROCESSED, TABLEFLOW_NUM_TOPICS, TABLEFLOW_STORAGE).
    Mirrors reference DefaultAllocator: principal=cluster_id, addl_details=USING_DEFAULT_ALLOCATOR.
    """
    logger.debug(
        "default_allocator resource=%s product=%s amount=%s",
        ctx.billing_line.resource_id,
        ctx.billing_line.product_type,
        ctx.split_amount,
    )
    row = make_row(
        ctx=ctx,
        identity_id=ctx.billing_line.resource_id,  # ← resource, not UNALLOCATED
        cost_type=CostType.SHARED,
        amount=ctx.split_amount,
        allocation_method="default",
        allocation_detail=AllocationDetail.USING_DEFAULT_ALLOCATOR,
    )
    return AllocationResult(rows=[row])


def unknown_allocator(ctx: AllocationContext) -> AllocationResult:
    """Assign full cost to resource_id for unrecognized product types.

    Mirrors reference UnknownAllocator: principal=cluster_id, shared_cost,
    USING_UNKNOWN_ALLOCATOR detail. Logs a warning so operators can detect
    new product types that need a dedicated handler.
    """
    logger.warning(
        "unknown_allocator: unrecognized product_type=%s resource=%s — allocating to resource_id",
        ctx.billing_line.product_type,
        ctx.billing_line.resource_id,
    )
    row = make_row(
        ctx=ctx,
        identity_id=ctx.billing_line.resource_id,
        cost_type=CostType.SHARED,
        amount=ctx.split_amount,
        allocation_method="unknown",
        allocation_detail=AllocationDetail.USING_UNKNOWN_ALLOCATOR,
    )
    return AllocationResult(rows=[row])


def cluster_linking_allocator(ctx: AllocationContext) -> AllocationResult:
    """Assign full cost to the resource itself.

    Cluster-linking costs are direct resource usage. Assign to resource_id
    to preserve lineage; cost_type=USAGE reflects direct consumption.
    """
    logger.debug(
        "cluster_linking_allocator resource=%s product=%s amount=%s",
        ctx.billing_line.resource_id,
        ctx.billing_line.product_type,
        ctx.split_amount,
    )
    row = make_row(
        ctx=ctx,
        identity_id=ctx.billing_line.resource_id,  # ← resource, not UNALLOCATED
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="cluster_linking",
        allocation_detail=CLUSTER_LINKING_COST,
    )
    return AllocationResult(rows=[row])
