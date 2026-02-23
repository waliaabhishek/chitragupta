"""Custom allocator for integration test override scenario."""

from __future__ import annotations

from core.engine.allocation import AllocationContext, AllocationResult
from core.models.chargeback import ChargebackRow, CostType


def my_allocator(ctx: AllocationContext) -> AllocationResult:
    """Custom allocator that assigns everything to resource owner with 'custom_override' method."""
    row = ChargebackRow(
        ecosystem=ctx.billing_line.ecosystem,
        tenant_id=ctx.billing_line.tenant_id,
        timestamp=ctx.billing_line.timestamp,
        resource_id=ctx.billing_line.resource_id,
        product_category=ctx.billing_line.product_category,
        product_type=ctx.billing_line.product_type,
        identity_id="custom-identity",
        cost_type=CostType.USAGE,
        amount=ctx.split_amount,
        allocation_method="custom_override",
    )
    return AllocationResult(rows=[row])
