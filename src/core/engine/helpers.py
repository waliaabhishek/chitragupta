from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import ChargebackRow, CostType, Resource

_CENT = Decimal("0.0001")


def _make_row(
    ctx: AllocationContext,
    identity_id: str,
    cost_type: CostType,
    amount: Decimal,
    allocation_method: str,
    allocation_detail: str | None = None,
) -> ChargebackRow:
    """Build a ChargebackRow from AllocationContext fields."""
    bl = ctx.billing_line
    return ChargebackRow(
        ecosystem=bl.ecosystem,
        tenant_id=bl.tenant_id,
        timestamp=bl.timestamp,
        resource_id=bl.resource_id,
        product_category=bl.product_category,
        product_type=bl.product_type,
        identity_id=identity_id,
        cost_type=cost_type,
        amount=amount,
        allocation_method=allocation_method,
        allocation_detail=allocation_detail,
    )


def split_amount_evenly(total: Decimal, count: int) -> list[Decimal]:
    """Split total into count parts, distributing remainder across leading recipients."""
    if count <= 0:
        return []
    base = (total / count).quantize(_CENT, rounding=ROUND_HALF_UP)
    amounts = [base] * count
    diff = total - sum(amounts)
    # Distribute remainder one cent at a time
    step = _CENT if diff > 0 else -_CENT
    idx = 0
    while diff != Decimal(0):
        amounts[idx] += step
        diff -= step
        idx += 1
    return amounts


def allocate_by_usage_ratio(
    ctx: AllocationContext,
    identity_values: dict[str, float],
) -> AllocationResult:
    """Allocate cost proportionally based on per-identity usage values."""
    total_value = sum(identity_values.values())
    if not identity_values or total_value == 0:
        row = _make_row(
            ctx,
            identity_id=ctx.billing_line.resource_id,
            cost_type=CostType.SHARED,
            amount=ctx.split_amount,
            allocation_method="usage_ratio",
            allocation_detail="no usage data; assigned to resource",
        )
        return AllocationResult(rows=[row])

    ids = list(identity_values.keys())
    ratios = [identity_values[i] / total_value for i in ids]
    raw_amounts = [ctx.split_amount * Decimal(str(r)) for r in ratios]
    # Same remainder-distribution algorithm as split_amount_evenly
    quantized = [a.quantize(_CENT, rounding=ROUND_HALF_UP) for a in raw_amounts]
    diff = ctx.split_amount - sum(quantized)
    step = _CENT if diff > 0 else -_CENT
    idx = 0
    while diff != Decimal(0):
        quantized[idx] += step
        diff -= step
        idx += 1

    rows = [
        _make_row(
            ctx,
            identity_id=ident,
            cost_type=CostType.USAGE,
            amount=amt,
            allocation_method="usage_ratio",
            allocation_detail=f"ratio={ratio:.6f}",
        )
        for ident, amt, ratio in zip(ids, quantized, ratios, strict=True)
    ]
    return AllocationResult(rows=rows)


def allocate_evenly(
    ctx: AllocationContext,
    identity_ids: Sequence[str],
) -> AllocationResult:
    """Allocate cost evenly across identities."""
    if not identity_ids:
        row = _make_row(
            ctx,
            identity_id=ctx.billing_line.resource_id,
            cost_type=CostType.SHARED,
            amount=ctx.split_amount,
            allocation_method="even_split",
            allocation_detail="no identities; assigned to resource",
        )
        return AllocationResult(rows=[row])

    amounts = split_amount_evenly(ctx.split_amount, len(identity_ids))
    rows = [
        _make_row(
            ctx,
            identity_id=ident,
            cost_type=CostType.SHARED,
            amount=amt,
            allocation_method="even_split",
        )
        for ident, amt in zip(identity_ids, amounts, strict=True)
    ]
    return AllocationResult(rows=rows)


def allocate_hybrid(
    ctx: AllocationContext,
    usage_ratio: float,
    shared_ratio: float,
    usage_fn: Callable[[AllocationContext], AllocationResult],
    shared_fn: Callable[[AllocationContext], AllocationResult],
) -> AllocationResult:
    """Split cost between usage-based and shared allocations."""
    if abs(usage_ratio + shared_ratio - 1.0) > 1e-9:
        msg = f"usage_ratio ({usage_ratio}) + shared_ratio ({shared_ratio}) must sum to 1.0"
        raise ValueError(msg)

    usage_amount = (ctx.split_amount * Decimal(str(usage_ratio))).quantize(_CENT, rounding=ROUND_HALF_UP)
    shared_amount = ctx.split_amount - usage_amount

    usage_ctx = replace(ctx, split_amount=usage_amount)
    shared_ctx = replace(ctx, split_amount=shared_amount)

    usage_result = usage_fn(usage_ctx)
    shared_result = shared_fn(shared_ctx)

    return AllocationResult(rows=usage_result.rows + shared_result.rows)


def allocate_to_owner(
    ctx: AllocationContext,
    owner_id: str,
) -> AllocationResult:
    """Allocate full cost to a specific owner identity."""
    if not owner_id:
        msg = "owner_id must not be empty"
        raise ValueError(msg)
    row = _make_row(
        ctx,
        identity_id=owner_id,
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="direct_owner",
    )
    return AllocationResult(rows=[row])


def allocate_to_resource(ctx: AllocationContext) -> AllocationResult:
    """Allocate full cost to the resource itself."""
    row = _make_row(
        ctx,
        identity_id=ctx.billing_line.resource_id,
        cost_type=CostType.SHARED,
        amount=ctx.split_amount,
        allocation_method="to_resource",
    )
    return AllocationResult(rows=[row])


def compute_active_fraction(
    resource: Resource,
    billing_start: datetime,
    billing_end: datetime,
) -> Decimal:
    """Compute fraction of billing window the resource was active."""
    total_window = (billing_end - billing_start).total_seconds()
    if total_window == 0:
        return Decimal(1)

    effective_start = resource.created_at if resource.created_at is not None else billing_start
    effective_end = resource.deleted_at if resource.deleted_at is not None else billing_end

    # Resource entirely outside window
    if effective_start >= billing_end or effective_end <= billing_start:
        return Decimal(0)

    # Clamp to window
    active_start = max(effective_start, billing_start)
    active_end = min(effective_end, billing_end)

    active_seconds = Decimal(str((active_end - active_start).total_seconds()))
    total_seconds = Decimal(str(total_window))
    fraction = active_seconds / total_seconds

    # Clamp to [0, 1]
    return max(Decimal(0), min(Decimal(1), fraction))
