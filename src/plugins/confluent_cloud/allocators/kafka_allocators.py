"""Kafka allocators for CCloud cost distribution.

These allocators handle various Kafka product types:
- KAFKA_NUM_CKU/KAFKA_NUM_CKUS: Hybrid usage/shared split (configurable ratios)
- KAFKA_NETWORK_READ/WRITE: Pure usage-based (bytes in/out)
- KAFKA_BASE/PARTITION/STORAGE: Even split across active identities
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import (
    allocate_by_usage_ratio,
    allocate_evenly,
    allocate_hybrid,
)

if TYPE_CHECKING:
    # AllocationContext/AllocationResult are runtime-available but imported under
    # TYPE_CHECKING for lightweight module loading. Works because
    # `from __future__ import annotations` makes all annotations strings.
    from core.engine.allocation import AllocationContext, AllocationResult


def kafka_num_cku_allocator(ctx: AllocationContext) -> AllocationResult:
    """Hybrid allocator: configurable usage/shared ratio.

    Default: 70% usage-based (bytes in/out ratio), 30% shared (even split).
    Configurable via allocator_params:
    - kafka_cku_usage_ratio: float (default 0.70)
    - kafka_cku_shared_ratio: float (default 0.30)
    """
    usage_ratio = float(ctx.params.get("kafka_cku_usage_ratio", 0.70))
    shared_ratio = float(ctx.params.get("kafka_cku_shared_ratio", 0.30))

    return allocate_hybrid(
        ctx,
        usage_ratio,
        shared_ratio,
        _kafka_usage_allocation,
        _kafka_shared_allocation,
    )


def kafka_network_allocator(ctx: AllocationContext) -> AllocationResult:
    """Pure usage-based allocation by bytes produced/consumed.

    Falls back to even split if no metrics, then to UNALLOCATED.
    """
    return _kafka_usage_allocation(ctx)


def kafka_base_allocator(ctx: AllocationContext) -> AllocationResult:
    """Even split across active identities.

    Used for KAFKA_BASE, KAFKA_PARTITION, KAFKA_STORAGE.
    """
    return _kafka_shared_allocation(ctx)


def _kafka_usage_allocation(ctx: AllocationContext) -> AllocationResult:
    """Allocate by bytes in/out ratio from metrics.

    Falls back to even split when:
    - No metrics_data
    - Metrics present but no non-zero usage values
    """
    if not ctx.metrics_data:
        # No metrics — fall back to even split
        return _kafka_shared_allocation(ctx)

    # Sum bytes per principal from bytes_in and bytes_out
    identity_bytes: dict[str, float] = {}
    for key in ("bytes_in", "bytes_out"):
        for row in ctx.metrics_data.get(key, []):
            principal = row.labels.get("principal_id")
            if principal and row.value > 0:
                identity_bytes[principal] = identity_bytes.get(principal, 0.0) + row.value

    if not identity_bytes:
        # Metrics present but no non-zero usage — fall back to even split
        return _kafka_shared_allocation(ctx)

    return allocate_by_usage_ratio(ctx, identity_bytes)


def _kafka_shared_allocation(ctx: AllocationContext) -> AllocationResult:
    """Even split across merged active identities.

    Fallback chain:
    1. merged_active (resource_active ∪ metrics_derived)
    2. tenant_period (all tenant identities in billing window)
    3. UNALLOCATED (no identities found)
    """
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        # Fall back to tenant_period
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)
