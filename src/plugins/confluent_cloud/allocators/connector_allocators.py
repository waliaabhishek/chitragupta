"""Connect allocators for CCloud cost distribution.

These allocators handle Kafka Connect product types:
- CONNECT_CAPACITY: Even split, SHARED cost type (infrastructure cost)
- CONNECT_NUM_TASKS: Even split, USAGE cost type (task-based cost)
- CONNECT_THROUGHPUT: Delegates to connect_tasks_allocator
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from core.engine.helpers import allocate_evenly
from core.models import OWNER_IDENTITY_TYPES, CostType

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def connect_capacity_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities for Connect capacity costs.

    Uses SHARED cost type (infrastructure cost).

    Fallback chain:
    1. merged_active (resource_active union metrics_derived)
    2. tenant_period filtered to owner types (SAs, users, pools — not API keys)
    3. UNALLOCATED (no identities found)
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES))
    return allocate_evenly(ctx, identity_ids)


def connect_tasks_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities for Connect task-based costs.

    Uses USAGE cost type (task-based cost).

    Fallback chain:
    1. merged_active (resource_active union metrics_derived)
    2. tenant_period filtered to owner types (SAs, users, pools — not API keys)
    3. UNALLOCATED (no identities found)
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES))

    result = allocate_evenly(ctx, identity_ids)

    # allocate_evenly uses SHARED cost type, but tasks should be USAGE
    # Create new rows with USAGE cost type
    result.rows = [replace(row, cost_type=CostType.USAGE) for row in result.rows]

    return result


def connect_throughput_allocator(ctx: AllocationContext) -> AllocationResult:
    """Allocator for Connect throughput costs.

    Delegates to connect_tasks_allocator since throughput-based costs
    follow the same allocation pattern as task-based costs.
    """
    return connect_tasks_allocator(ctx)
