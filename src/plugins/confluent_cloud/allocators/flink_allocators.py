"""Flink allocators for CCloud cost distribution.

Flink CFU costs use usage-ratio allocation by statement owner CFU consumption.
CFU (Confluent Flink Units) represent compute capacity per statement.

Fallback chain:
1. Usage-ratio by statement owner (from IdentityResolution.context["stmt_owner_cfu"])
2. Even split across merged_active (resource_active union metrics_derived)
3. Even split across tenant_period
4. UNALLOCATED
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly
from core.models import CostType

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def flink_cfu_allocator(ctx: AllocationContext) -> AllocationResult:
    """Allocate Flink CFU costs proportionally by statement owner usage.

    Reads stmt_owner_cfu from ctx.identities.context["stmt_owner_cfu"]
    (populated by resolve_flink_identity). Falls back to even split.

    Fallback chain:
    1. stmt_owner_cfu ratio (USAGE cost type)
    2. merged_active even split (USAGE cost type)
    3. tenant_period even split (USAGE cost type)
    4. UNALLOCATED
    """
    # Try usage-ratio allocation from statement owner CFU map
    stmt_owner_cfu: dict[str, float] = ctx.identities.context.get("stmt_owner_cfu", {})
    if stmt_owner_cfu:
        # allocate_by_usage_ratio already uses USAGE cost type
        return allocate_by_usage_ratio(ctx, stmt_owner_cfu)

    # Fallback: even split across active identities
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())

    result = allocate_evenly(ctx, identity_ids)

    # Override cost type: allocate_evenly defaults to SHARED, but CFU is
    # compute consumption, so USAGE is semantically correct.
    result.rows = [replace(row, cost_type=CostType.USAGE) for row in result.rows]

    return result
