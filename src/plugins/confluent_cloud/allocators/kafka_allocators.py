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
    make_row,
    split_amount_evenly,
)
from core.models import OWNER_IDENTITY_TYPES
from core.models.chargeback import AllocationDetail, CostType
from plugins.confluent_cloud.allocation_models import (
    _CKU_DYNAMIC_MODEL,
    BYTES_IN_MODEL,
    BYTES_OUT_MODEL,
    PARTITION_MODEL,
)

logger = logging.getLogger(__name__)


def kafka_cku_allocator(ctx: AllocationContext) -> AllocationResult:
    """CKU allocator: 70% usage-based / 30% shared by default.

    Uses DynamicCompositionModel — reads kafka_cku_usage_ratio and
    kafka_cku_shared_ratio from ctx.params (defaults: 0.70 / 0.30).
    CompositionModel injects composition_index and composition_ratio metadata.
    Raises ValueError if supplied ratios do not sum to 1.0.
    """
    logger.debug(
        "Allocating kafka_cku resource=%s product=%s amount=%s",
        ctx.billing_line.resource_id,
        ctx.billing_line.product_type,
        ctx.split_amount,
    )
    return _CKU_DYNAMIC_MODEL(ctx)


def kafka_network_allocator(ctx: AllocationContext) -> AllocationResult:
    """Pure usage-based allocation by bytes produced/consumed.

    Falls back to even split if no metrics, then to resource.
    """
    return _kafka_usage_allocation(ctx)


def kafka_base_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities.

    Used for KAFKA_BASE, KAFKA_STORAGE.
    """
    return _fallback_no_metrics(ctx)


def kafka_network_read_allocator(ctx: AllocationContext) -> AllocationResult:
    """KAFKA_NETWORK_READ: bytes_out (response / consume direction)."""
    return BYTES_OUT_MODEL(ctx)


def kafka_network_write_allocator(ctx: AllocationContext) -> AllocationResult:
    """KAFKA_NETWORK_WRITE: bytes_in (request / produce direction)."""
    return BYTES_IN_MODEL(ctx)


def kafka_partition_allocator(ctx: AllocationContext) -> AllocationResult:
    """KAFKA_PARTITION: no metrics — always falls to even-split tiers."""
    return PARTITION_MODEL(ctx)


def _kafka_usage_allocation(ctx: AllocationContext) -> AllocationResult:
    """Allocate network costs by bytes with tiered fallback."""
    if ctx.metrics_fetch_failed:
        return _fallback_fetch_failed(ctx)
    if not ctx.metrics_data:
        return _fallback_no_metrics(ctx)

    api_key_to_owner: dict[str, str] = ctx.identities.context.get("api_key_to_owner", {})
    identity_bytes: dict[str, float] = {}
    has_metric_rows = False
    for key in ("bytes_in", "bytes_out"):
        for row in ctx.metrics_data.get(key, []):
            has_metric_rows = True
            principal = row.labels.get("principal_id")
            if principal and row.value > 0:
                resolved = api_key_to_owner.get(principal, principal)
                identity_bytes[resolved] = identity_bytes.get(resolved, 0.0) + row.value

    if identity_bytes:
        return allocate_by_usage_ratio(ctx, identity_bytes)

    if has_metric_rows:
        return _fallback_zero_usage(ctx)

    return _fallback_no_metrics(ctx)


def _fallback_fetch_failed(ctx: AllocationContext) -> AllocationResult:
    """Metrics infrastructure failure — Prometheus unreachable or errored."""
    logger.warning(
        "Metrics fetch failed for resource=%s — cost unallocated with METRICS_FETCH_FAILED",
        ctx.billing_line.resource_id,
    )
    row = make_row(
        ctx,
        identity_id="UNALLOCATED",
        cost_type=CostType.SHARED,
        amount=ctx.split_amount,
        allocation_method="usage_ratio",
        allocation_detail=AllocationDetail.METRICS_FETCH_FAILED,
    )
    return AllocationResult(rows=[row])


def _fallback_no_metrics(ctx: AllocationContext) -> AllocationResult:
    """Tier 2: Prometheus returned no data for this cluster/timeslice."""
    logger.warning(
        "No metrics for resource=%s — falling back to even split",
        ctx.billing_line.resource_id,
    )
    merged_active = list(ctx.identities.merged_active.ids())
    if merged_active:
        return _even_split_with_detail(ctx, merged_active, AllocationDetail.NO_METRICS_LOCATED)
    all_merged = sorted(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES))
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
    all_merged = sorted(ctx.identities.tenant_period.ids_by_type(*OWNER_IDENTITY_TYPES))
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
