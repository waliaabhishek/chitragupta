"""Default and cluster-linking allocators for CCloud cost distribution.

Default allocator: assigns full cost to UNALLOCATED as SHARED.
Used for product types with no meaningful identity resolution
(TABLEFLOW_DATA_PROCESSED, TABLEFLOW_NUM_TOPICS, TABLEFLOW_STORAGE).

Cluster-linking allocator: assigns full cost to UNALLOCATED as USAGE.
Used for CLUSTER_LINKING_* product types where cost is direct resource
usage but no identity resolution is available.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly, make_row
from core.models import CostType

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def default_allocator(ctx: AllocationContext) -> AllocationResult:
    """Assign full cost to UNALLOCATED as SHARED.

    No identity resolution is available for these product types.
    The allocate_evenly helper with an empty list handles the
    UNALLOCATED fallback with SHARED cost type.
    """
    return allocate_evenly(ctx, [])


def cluster_linking_allocator(ctx: AllocationContext) -> AllocationResult:
    """Assign full cost to UNALLOCATED identity as USAGE cost type.

    Cluster-linking costs are direct resource usage, but we don't have
    identity information, so we attribute to UNALLOCATED.
    """
    from core.engine.allocation import AllocationResult

    row = make_row(
        ctx=ctx,
        identity_id="UNALLOCATED",
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="cluster_linking",
        allocation_detail="no identity resolution; allocated to UNALLOCATED",
    )
    return AllocationResult(rows=[row])
