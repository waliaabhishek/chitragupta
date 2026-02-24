"""Tests for default and cluster-linking allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext
from core.models import BillingLineItem, CostType, IdentityResolution, IdentitySet


def _make_ctx(product_type: str = "TABLEFLOW_STORAGE", amount: Decimal = Decimal("50")) -> AllocationContext:
    """Build a minimal AllocationContext for default allocator tests."""
    return AllocationContext(
        timeslice=datetime(2026, 2, 1, tzinfo=UTC),
        billing_line=BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="tableflow-1",
            product_category="TABLEFLOW",
            product_type=product_type,
            quantity=Decimal("1"),
            unit_price=amount,
            total_cost=amount,
        ),
        identities=IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        ),
        split_amount=amount,
    )


class TestDefaultAllocator:
    """Tests for default_allocator."""

    def test_allocates_to_unallocated(self) -> None:
        """Default allocator sends full amount to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.default_allocators import default_allocator

        ctx = _make_ctx()
        result = default_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("50")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_preserves_billing_line_fields(self) -> None:
        """Chargeback row carries billing line fields."""
        from plugins.confluent_cloud.allocators.default_allocators import default_allocator

        ctx = _make_ctx(product_type="TABLEFLOW_DATA_PROCESSED", amount=Decimal("75"))
        result = default_allocator(ctx)

        row = result.rows[0]
        assert row.product_type == "TABLEFLOW_DATA_PROCESSED"
        assert row.tenant_id == "org-123"
        assert row.amount == Decimal("75")


class TestClusterLinkingAllocator:
    """Tests for cluster_linking_allocator."""

    def test_allocates_to_unallocated(self) -> None:
        """Cluster-linking allocator sends full amount to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.default_allocators import cluster_linking_allocator

        ctx = _make_ctx(product_type="CLUSTER_LINKING_READ", amount=Decimal("30"))
        result = cluster_linking_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("30")

    def test_cost_type_is_usage(self) -> None:
        """Cluster-linking costs are USAGE (direct resource usage)."""
        from plugins.confluent_cloud.allocators.default_allocators import cluster_linking_allocator

        ctx = _make_ctx(product_type="CLUSTER_LINKING_PER_LINK")
        result = cluster_linking_allocator(ctx)

        assert result.rows[0].cost_type == CostType.USAGE
