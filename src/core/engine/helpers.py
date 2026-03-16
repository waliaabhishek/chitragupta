from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import ChargebackRow, CostType, Resource
from core.models.chargeback import AllocationDetail

logger = logging.getLogger(__name__)

_CENT = Decimal("0.0001")
_ZERO = Decimal(0)


def _distribute_remainder(amounts: list[Decimal], diff: Decimal) -> list[Decimal]:
    """Distribute a rounding remainder across leading entries, one cent at a time.

    Iterates through ``amounts`` in round-robin order, adjusting each entry by
    one cent until ``diff`` reaches zero.  The loop is bounded by
    ``len(amounts) * 2`` iterations — empirically the maximum observed is
    approximately ``len(amounts)`` — so the 2x factor provides a safe margin.

    Raises:
        RuntimeError: If the remainder is not resolved within the safety bound,
            indicating a programming error (e.g. ``diff`` is not a multiple of
            ``_CENT``).
    """
    if diff == _ZERO:
        return amounts
    step = _CENT if diff > 0 else -_CENT
    max_iterations = len(amounts) * 2
    idx = 0
    for _ in range(max_iterations):
        amounts[idx] += step
        diff = diff - step  # no quantize — callers pre-quantize; sub-cent diffs must not falsely converge
        idx = (idx + 1) % len(amounts)
        if diff == _ZERO:
            break
    else:
        msg = f"_distribute_remainder did not converge after {max_iterations} iterations; remaining diff={diff!r}"
        raise RuntimeError(msg)
    return amounts


def make_row(
    ctx: AllocationContext,
    identity_id: str,
    cost_type: CostType,
    amount: Decimal,
    allocation_method: str,
    allocation_detail: str | None = None,
    metadata: dict[str, Any] | None = None,
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
        metadata=metadata or {},
    )


def split_amount_evenly(total: Decimal, count: int) -> list[Decimal]:
    """Split total into count parts, distributing remainder across leading recipients.

    Uses ``_distribute_remainder()`` to assign leftover cents after even
    division.  The remainder loop is bounded by ``count * 2`` iterations and
    raises ``RuntimeError`` if exceeded.
    """
    if count <= 0:
        return []
    total = total.quantize(_CENT, rounding=ROUND_HALF_UP)
    base = (total / count).quantize(_CENT, rounding=ROUND_HALF_UP)
    amounts = [base] * count
    diff = (total - sum(amounts)).quantize(_CENT)
    return _distribute_remainder(amounts, diff)


def allocate_by_usage_ratio(
    ctx: AllocationContext,
    identity_values: dict[str, float],
    allocation_detail: str | None = None,
) -> AllocationResult:
    """Allocate cost proportionally based on per-identity usage values."""
    total_value = sum(identity_values.values())
    if not identity_values or total_value == 0:
        logger.warning(
            "No usable metrics for resource=%s product=%s — falling back to even split",
            ctx.billing_line.resource_id,
            ctx.billing_line.product_type,
        )
        row = make_row(
            ctx,
            identity_id="UNALLOCATED",
            cost_type=CostType.SHARED,
            amount=ctx.split_amount,
            allocation_method="usage_ratio",
            allocation_detail=AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES,
        )
        return AllocationResult(rows=[row])

    ids = list(identity_values.keys())
    ratios = [identity_values[i] / total_value for i in ids]
    split_amount = ctx.split_amount.quantize(_CENT, rounding=ROUND_HALF_UP)
    raw_amounts = [split_amount * Decimal(str(r)) for r in ratios]
    quantized = [a.quantize(_CENT, rounding=ROUND_HALF_UP) for a in raw_amounts]
    diff = (split_amount - sum(quantized)).quantize(_CENT)
    quantized = _distribute_remainder(quantized, diff)

    rows = [
        make_row(
            ctx,
            identity_id=ident,
            cost_type=CostType.USAGE,
            amount=amt,
            allocation_method="usage_ratio",
            allocation_detail=allocation_detail or AllocationDetail.USAGE_RATIO_ALLOCATION,
            metadata={"ratio": ratio},
        )
        for ident, amt, ratio in zip(ids, quantized, ratios, strict=True)
    ]
    return AllocationResult(rows=rows)


def allocate_evenly(
    ctx: AllocationContext,
    identity_ids: Sequence[str],
    allocation_detail: str | None = None,
    cost_type: CostType = CostType.SHARED,
) -> AllocationResult:
    """Allocate cost evenly across identities."""
    if not identity_ids:
        row = make_row(
            ctx,
            identity_id="UNALLOCATED",
            cost_type=cost_type,
            amount=ctx.split_amount,
            allocation_method="even_split",
            allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED,
        )
        return AllocationResult(rows=[row])

    amounts = split_amount_evenly(ctx.split_amount, len(identity_ids))
    rows = [
        make_row(
            ctx,
            identity_id=ident,
            cost_type=cost_type,
            amount=amt,
            allocation_method="even_split",
            allocation_detail=allocation_detail,
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
