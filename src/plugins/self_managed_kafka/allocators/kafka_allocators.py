"""Per-product-type cost allocators for self-managed Kafka."""

from __future__ import annotations

from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly_with_fallback

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult


def self_kafka_network_ingress_allocator(ctx: AllocationContext) -> AllocationResult:
    """NETWORK_INGRESS: usage ratio based on bytes_in per principal."""
    return _network_allocator(ctx, "bytes_in_per_principal")


def self_kafka_network_egress_allocator(ctx: AllocationContext) -> AllocationResult:
    """NETWORK_EGRESS: usage ratio based on bytes_out per principal."""
    return _network_allocator(ctx, "bytes_out_per_principal")


def _network_allocator(ctx: AllocationContext, metric_key: str) -> AllocationResult:
    """Allocate network cost by per-principal bytes for the given direction."""
    if not ctx.metrics_data:
        return allocate_evenly_with_fallback(ctx)

    identity_bytes: dict[str, float] = {}
    for row in ctx.metrics_data.get(metric_key, []):
        principal = row.labels.get("principal")
        if principal and row.value > 0:
            identity_bytes[principal] = identity_bytes.get(principal, 0.0) + row.value

    if not identity_bytes:
        return allocate_evenly_with_fallback(ctx)

    return allocate_by_usage_ratio(ctx, identity_bytes)
