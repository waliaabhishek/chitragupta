"""Per-product-type cost allocators for self-managed Kafka.

Allocation strategy by product type:
- COMPUTE/STORAGE: even split across active identities (infrastructure costs — everyone benefits equally)
- NETWORK_INGRESS: usage ratio based on bytes_in per principal
- NETWORK_EGRESS: usage ratio based on bytes_out per principal
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


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


def self_kafka_network_ingress_allocator(ctx: AllocationContext) -> AllocationResult:
    """NETWORK_INGRESS: usage ratio based on bytes_in per principal.

    Falls back to even split when:
    - ctx.metrics_data is None or empty
    - No non-zero per-principal values found for bytes_in_per_principal
    """
    return _network_allocator(ctx, "bytes_in_per_principal")


def self_kafka_network_egress_allocator(ctx: AllocationContext) -> AllocationResult:
    """NETWORK_EGRESS: usage ratio based on bytes_out per principal.

    Falls back to even split when:
    - ctx.metrics_data is None or empty
    - No non-zero per-principal values found for bytes_out_per_principal
    """
    return _network_allocator(ctx, "bytes_out_per_principal")


def _network_allocator(ctx: AllocationContext, metric_key: str) -> AllocationResult:
    """Allocate network cost by per-principal bytes for the given direction.

    Falls back to even split when:
    - ctx.metrics_data is None or empty
    - No non-zero per-principal values found for the given metric_key
    """
    if not ctx.metrics_data:
        return _even_split_fallback(ctx)

    identity_bytes: dict[str, float] = {}
    for row in ctx.metrics_data.get(metric_key, []):
        principal = row.labels.get("principal")
        if principal and row.value > 0:
            identity_bytes[principal] = identity_bytes.get(principal, 0.0) + row.value

    if not identity_bytes:
        return _even_split_fallback(ctx)

    return allocate_by_usage_ratio(ctx, identity_bytes)


def _even_split_fallback(ctx: AllocationContext) -> AllocationResult:
    """Even split across merged_active identities, falling back to tenant_period."""
    identity_ids = list(ctx.identities.merged_active.ids())
    if not identity_ids:
        identity_ids = list(ctx.identities.tenant_period.ids())
    return allocate_evenly(ctx, identity_ids)
