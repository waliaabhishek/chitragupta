"""Kafka allocators for CCloud cost distribution.

These allocators handle various Kafka product types:
- KAFKA_NUM_CKU/KAFKA_NUM_CKUS: Hybrid usage/shared split (configurable ratios)
- KAFKA_NETWORK_READ/WRITE: Pure usage-based (bytes in/out)
- KAFKA_BASE/PARTITION/STORAGE: Even split across active identities
"""

from __future__ import annotations

import logging

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.helpers import (
    allocate_by_usage_ratio,
    allocate_hybrid,
    make_row,
    split_amount_evenly,
)
from core.models.chargeback import AllocationDetail, CostType

logger = logging.getLogger(__name__)


def kafka_num_cku_allocator(ctx: AllocationContext) -> AllocationResult:
    """Hybrid allocator: configurable usage/shared ratio.

    Default: 70% usage-based (bytes in/out ratio), 30% shared (even split).
    Configurable via allocator_params:
    - kafka_cku_usage_ratio: float (default 0.70)
    - kafka_cku_shared_ratio: float (default 0.30)
    """
    logger.debug(
        "Allocating kafka_num_cku resource=%s product=%s amount=%s",
        ctx.billing_line.resource_id,
        ctx.billing_line.product_type,
        ctx.split_amount,
    )
    usage_ratio = float(ctx.params.get("kafka_cku_usage_ratio", 0.70))
    shared_ratio = float(ctx.params.get("kafka_cku_shared_ratio", 0.30))

    return allocate_hybrid(
        ctx,
        usage_ratio,
        shared_ratio,
        _kafka_usage_allocation,
        _fallback_no_metrics,
    )


def kafka_network_allocator(ctx: AllocationContext) -> AllocationResult:
    """Pure usage-based allocation by bytes produced/consumed.

    Falls back to even split if no metrics, then to resource.
    """
    return _kafka_usage_allocation(ctx)


def kafka_base_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities.

    Used for KAFKA_BASE, KAFKA_PARTITION, KAFKA_STORAGE.
    """
    return _fallback_no_metrics(ctx)


def _kafka_usage_allocation(ctx: AllocationContext) -> AllocationResult:
    """Allocate network costs by bytes with tiered fallback."""
    if not ctx.metrics_data:
        return _fallback_no_metrics(ctx)

    identity_bytes: dict[str, float] = {}
    has_metric_rows = False
    for key in ("bytes_in", "bytes_out"):
        for row in ctx.metrics_data.get(key, []):
            has_metric_rows = True
            principal = row.labels.get("principal_id")
            if principal and row.value > 0:
                identity_bytes[principal] = identity_bytes.get(principal, 0.0) + row.value

    if identity_bytes:
        return allocate_by_usage_ratio(ctx, identity_bytes)

    if has_metric_rows:
        return _fallback_zero_usage(ctx)

    return _fallback_no_metrics(ctx)


def _fallback_no_metrics(ctx: AllocationContext) -> AllocationResult:
    """Tier 2: Prometheus returned no data for this cluster/timeslice."""
    logger.warning(
        "No metrics for resource=%s — falling back to even split",
        ctx.billing_line.resource_id,
    )
    merged_active = list(ctx.identities.merged_active.ids())
    if merged_active:
        return _even_split_with_detail(ctx, merged_active, AllocationDetail.NO_METRICS_LOCATED)
    all_merged = list(ctx.identities.tenant_period.ids())
    if all_merged:
        return _even_split_with_detail(ctx, all_merged, AllocationDetail.NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED)
    return _to_resource_with_detail(ctx, AllocationDetail.NO_IDENTITIES_LOCATED)


def _fallback_zero_usage(ctx: AllocationContext) -> AllocationResult:
    """Tier 3: Metrics rows exist but all usage values were zero."""
    merged_active = list(ctx.identities.merged_active.ids())
    if merged_active:
        return _even_split_with_detail(
            ctx, merged_active, AllocationDetail.NO_METRICS_PRESENT_MERGED_IDENTITIES_LOCATED
        )
    all_merged = list(ctx.identities.tenant_period.ids())
    if all_merged:
        return _even_split_with_detail(
            ctx, all_merged, AllocationDetail.NO_METRICS_PRESENT_PENALTY_ALLOCATION_FOR_EVERYONE
        )
    return _to_resource_with_detail(ctx, AllocationDetail.NO_IDENTITIES_LOCATED)


def _even_split_with_detail(
    ctx: AllocationContext,
    identities: list[str],
    detail: AllocationDetail,
) -> AllocationResult:
    """Split cost evenly with a custom allocation detail code."""
    amounts = split_amount_evenly(ctx.split_amount, len(identities))
    rows = [
        make_row(
            ctx,
            identity_id=iid,
            cost_type=CostType.SHARED,
            amount=amt,
            allocation_method="even_split",
            allocation_detail=detail,
        )
        for iid, amt in zip(identities, amounts, strict=True)
    ]
    return AllocationResult(rows=rows)


def _to_resource_with_detail(ctx: AllocationContext, detail: AllocationDetail) -> AllocationResult:
    """Assign full cost to the billing resource with a specific detail code."""
    return AllocationResult(
        rows=[
            make_row(
                ctx,
                identity_id=ctx.billing_line.resource_id,
                cost_type=CostType.SHARED,
                amount=ctx.split_amount,
                allocation_method="to_resource",
                allocation_detail=detail,
            )
        ]
    )
