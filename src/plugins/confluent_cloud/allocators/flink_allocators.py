"""Flink allocators for CCloud cost distribution.

Flink CFU costs use usage-ratio allocation by statement owner CFU consumption.
CFU (Confluent Flink Units) represent compute capacity per statement.

Fallback chain:
1. Usage-ratio by statement owner (from IdentityResolution.context["stmt_owner_cfu"])
2. Even split across merged_active (resource_active union metrics_derived)
3. UNALLOCATED with specific detail codes
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from core.engine.allocation import AllocationResult
from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly, make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext


def flink_cfu_allocator(ctx: AllocationContext) -> AllocationResult:
    """Allocate Flink CFU costs proportionally by statement owner usage.

    Reads stmt_owner_cfu from ctx.identities.context["stmt_owner_cfu"]
    (populated by resolve_flink_identity). Falls back to even split across
    merged_active when CFU data is absent but identities are known.

    Fallback chain:
    1. stmt_owner_cfu ratio (USAGE cost type) — primary path
    2. Even split across merged_active (USAGE cost type) — zero-CFU fallback
    3. UNALLOCATED with NO_METRICS_LOCATED — if no metrics_data present
    4. UNALLOCATED with NO_FLINK_STMT_NAME_TO_OWNER_MAP — if metrics present but no owner map

    See also: core.models.FlinkContextDict for typed context shape.
    """
    # TD-032: Context shape documented in core.models.FlinkContextDict
    stmt_owner_cfu: dict[str, float] = ctx.identities.context.get("stmt_owner_cfu", {})
    if stmt_owner_cfu:
        # allocate_by_usage_ratio already uses USAGE cost type
        return allocate_by_usage_ratio(ctx, stmt_owner_cfu)

    # No CFU data — fall back to even split across merged_active
    merged = list(ctx.identities.merged_active.ids())
    if merged:
        result = allocate_evenly(ctx, merged)
        # Override to USAGE cost type (Flink is consumption-based)
        return AllocationResult(rows=[replace(row, cost_type=CostType.USAGE) for row in result.rows])

    # TD-034/TD-035: Use specific detail codes for Flink terminal fallback paths
    if not ctx.metrics_data:
        detail = AllocationDetail.NO_METRICS_LOCATED
    else:
        detail = AllocationDetail.NO_FLINK_STMT_NAME_TO_OWNER_MAP

    row = make_row(
        ctx,
        identity_id="UNALLOCATED",
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="even_split",
        allocation_detail=detail,
    )
    return AllocationResult(rows=[row])
