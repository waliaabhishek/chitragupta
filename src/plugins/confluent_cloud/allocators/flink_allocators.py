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

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, make_row
from core.models import CostType
from core.models.chargeback import AllocationDetail

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def flink_cfu_allocator(ctx: AllocationContext) -> AllocationResult:
    """Allocate Flink CFU costs proportionally by statement owner usage.

    Reads stmt_owner_cfu from ctx.identities.context["stmt_owner_cfu"]
    (populated by resolve_flink_identity). Falls back to UNALLOCATED with
    specific AllocationDetail codes for observability.

    Fallback chain:
    1. stmt_owner_cfu ratio (USAGE cost type) — primary path
    2. UNALLOCATED with NO_METRICS_LOCATED — if no metrics_data present
    3. UNALLOCATED with NO_FLINK_STMT_NAME_TO_OWNER_MAP — if metrics present but no owner map

    See also: core.models.FlinkContextDict for typed context shape.
    """
    # TD-032: Context shape documented in core.models.FlinkContextDict
    stmt_owner_cfu: dict[str, float] = ctx.identities.context.get("stmt_owner_cfu", {})
    if stmt_owner_cfu:
        # allocate_by_usage_ratio already uses USAGE cost type
        return allocate_by_usage_ratio(ctx, stmt_owner_cfu)

    # TD-034/TD-035: Use specific detail codes for Flink fallback paths
    if not ctx.metrics_data:
        detail = AllocationDetail.NO_METRICS_LOCATED
    else:
        detail = AllocationDetail.NO_FLINK_STMT_NAME_TO_OWNER_MAP

    from core.engine.allocation import AllocationResult

    row = make_row(
        ctx,
        identity_id="UNALLOCATED",
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="even_split",
        allocation_detail=detail,
    )
    return AllocationResult(rows=[row])
