"""Per-product-type cost allocators for self-managed Kafka.

Allocation strategy by product type:
- COMPUTE/STORAGE: even split across active identities (infrastructure costs — everyone benefits equally)
- NETWORK_*: usage ratio based on bytes per principal (usage-driven costs)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult
    from core.models import MetricRow


def self_kafka_compute_allocator(ctx: AllocationContext) -> AllocationResult:
    """COMPUTE: even split across all active identities (infrastructure cost)."""
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)


def self_kafka_storage_allocator(ctx: AllocationContext) -> AllocationResult:
    """STORAGE: even split across all active identities (infrastructure cost)."""
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)


def self_kafka_network_allocator(ctx: AllocationContext) -> AllocationResult:
    """NETWORK: usage ratio based on bytes per principal.

    Falls back to even split when:
    - No metrics_data provided
    - Metrics present but no non-zero usage values per principal
    """
    if not ctx.metrics_data:
        return _even_split_fallback(ctx)

    identity_bytes = _sum_bytes_per_principal(ctx.metrics_data)
    if not identity_bytes:
        return _even_split_fallback(ctx)

    return allocate_by_usage_ratio(ctx, identity_bytes)


def _even_split_fallback(ctx: AllocationContext) -> AllocationResult:
    """Even split across merged_active identities, falling back to tenant_period."""
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)


def _sum_bytes_per_principal(
    metrics_data: dict[str, list[MetricRow]],
) -> dict[str, float]:
    """Sum bytes_in and bytes_out per principal from metrics data."""
    identity_bytes: dict[str, float] = {}
    for key in ("bytes_in_per_principal", "bytes_out_per_principal"):
        for row in metrics_data.get(key, []):
            principal = row.labels.get("principal")
            if principal and row.value > 0:
                identity_bytes[principal] = identity_bytes.get(principal, 0.0) + row.value
    return identity_bytes
