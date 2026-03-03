"""Tests for default and cluster-linking allocators.

GAP-02: Both allocators should attribute cost to the billing resource
(resource_id), not to the synthetic UNALLOCATED sentinel.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext
from core.models import BillingLineItem, CostType, IdentityResolution, IdentitySet
from core.models.chargeback import AllocationDetail
from plugins.confluent_cloud.allocators.default_allocators import (
    cluster_linking_allocator,
    default_allocator,
)
from plugins.confluent_cloud.constants import CLUSTER_LINKING_COST


def _make_ctx(
    product_type: str = "TABLEFLOW_STORAGE",
    resource_id: str = "tableflow-res-1",
    amount: Decimal = Decimal("50"),
) -> AllocationContext:
    """Build a minimal AllocationContext for default allocator tests."""
    return AllocationContext(
        timeslice=datetime(2026, 2, 1, tzinfo=UTC),
        billing_line=BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id=resource_id,
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
    """Tests for default_allocator — GAP-02 expected behavior."""

    def test_identity_is_resource_id_not_unallocated(self) -> None:
        """Default allocator attributes cost to billing resource, not UNALLOCATED."""
        ctx = _make_ctx(resource_id="tableflow-res-abc")
        result = default_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "tableflow-res-abc"
        assert result.rows[0].identity_id != "UNALLOCATED"

    def test_identity_matches_billing_line_resource_id(self) -> None:
        """identity_id == ctx.billing_line.resource_id exactly."""
        ctx = _make_ctx(resource_id="some-unique-resource-id-999")
        result = default_allocator(ctx)

        assert result.rows[0].identity_id == ctx.billing_line.resource_id

    def test_allocation_detail_is_using_default_allocator(self) -> None:
        """Default allocator sets AllocationDetail.USING_DEFAULT_ALLOCATOR."""
        ctx = _make_ctx()
        result = default_allocator(ctx)

        assert result.rows[0].allocation_detail == AllocationDetail.USING_DEFAULT_ALLOCATOR

    def test_cost_type_is_shared(self) -> None:
        """Default allocator uses SHARED cost type."""
        ctx = _make_ctx()
        result = default_allocator(ctx)

        assert result.rows[0].cost_type == CostType.SHARED

    def test_single_row_full_amount(self) -> None:
        """Default allocator produces exactly one row with the full split amount."""
        ctx = _make_ctx(amount=Decimal("75"))
        result = default_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].amount == Decimal("75")

    def test_preserves_billing_line_fields(self) -> None:
        """Chargeback row carries billing line metadata fields."""
        ctx = _make_ctx(product_type="TABLEFLOW_DATA_PROCESSED", amount=Decimal("75"))
        result = default_allocator(ctx)

        row = result.rows[0]
        assert row.product_type == "TABLEFLOW_DATA_PROCESSED"
        assert row.tenant_id == "org-123"
        assert row.amount == Decimal("75")

    def test_tableflow_num_topics_uses_resource_id(self) -> None:
        """TABLEFLOW_NUM_TOPICS product type also attributes to resource_id."""
        ctx = _make_ctx(product_type="TABLEFLOW_NUM_TOPICS", resource_id="tf-topics-res")
        result = default_allocator(ctx)

        assert result.rows[0].identity_id == "tf-topics-res"
        assert result.rows[0].allocation_detail == AllocationDetail.USING_DEFAULT_ALLOCATOR


class TestClusterLinkingAllocator:
    """Tests for cluster_linking_allocator — GAP-02 expected behavior."""

    def test_identity_is_resource_id_not_unallocated(self) -> None:
        """Cluster-linking allocator attributes to resource, not UNALLOCATED."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_READ", resource_id="lkc-cluster-42")
        result = cluster_linking_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "lkc-cluster-42"
        assert result.rows[0].identity_id != "UNALLOCATED"

    def test_identity_matches_billing_line_resource_id(self) -> None:
        """identity_id == ctx.billing_line.resource_id exactly."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_PER_LINK", resource_id="lkc-unique-777")
        result = cluster_linking_allocator(ctx)

        assert result.rows[0].identity_id == ctx.billing_line.resource_id

    def test_cost_type_is_usage(self) -> None:
        """Cluster-linking costs are USAGE (direct resource usage)."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_PER_LINK")
        result = cluster_linking_allocator(ctx)

        assert result.rows[0].cost_type == CostType.USAGE

    def test_allocation_detail_is_cluster_linking_cost(self) -> None:
        """Cluster-linking allocator sets CLUSTER_LINKING_COST (plugin constant)."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_READ", amount=Decimal("30"))
        result = cluster_linking_allocator(ctx)

        assert result.rows[0].allocation_detail == CLUSTER_LINKING_COST

    def test_single_row_full_amount(self) -> None:
        """Cluster-linking allocator produces exactly one row with the full split amount."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_READ", amount=Decimal("30"))
        result = cluster_linking_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].amount == Decimal("30")

    def test_cluster_linking_write_uses_resource_id(self) -> None:
        """CLUSTER_LINKING_WRITE variant also attributes to resource_id."""
        ctx = _make_ctx(product_type="CLUSTER_LINKING_WRITE", resource_id="lkc-writer-9")
        result = cluster_linking_allocator(ctx)

        assert result.rows[0].identity_id == "lkc-writer-9"
        assert result.rows[0].cost_type == CostType.USAGE
        assert result.rows[0].allocation_detail == CLUSTER_LINKING_COST
