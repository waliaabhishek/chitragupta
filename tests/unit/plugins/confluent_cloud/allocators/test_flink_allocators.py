"""Tests for Flink allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    CostType,
    Identity,
    IdentityResolution,
    IdentitySet,
)
from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator


@pytest.fixture
def flink_billing_line() -> BillingLineItem:
    """Standard Flink billing line for tests."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lfcp-pool-1",
        product_category="FLINK",
        product_type="FLINK_NUM_CFU",
        quantity=Decimal("10"),
        unit_price=Decimal("10"),
        total_cost=Decimal("100"),
    )


def make_identity_set(*identity_ids: str) -> IdentitySet:
    """Create an IdentitySet with the given identity IDs."""
    iset = IdentitySet()
    for identity_id in identity_ids:
        iset.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=identity_id,
                identity_type="service_account",
            )
        )
    return iset


def _make_resolution(
    resource_active: IdentitySet | None = None,
    metrics_derived: IdentitySet | None = None,
    tenant_period: IdentitySet | None = None,
    stmt_owner_cfu: dict[str, float] | None = None,
) -> IdentityResolution:
    """Build IdentityResolution with optional stmt_owner_cfu context."""
    context: dict[str, object] = {}
    if stmt_owner_cfu:
        context["stmt_owner_cfu"] = stmt_owner_cfu
    return IdentityResolution(
        resource_active=resource_active or IdentitySet(),
        metrics_derived=metrics_derived or IdentitySet(),
        tenant_period=tenant_period or IdentitySet(),
        context=context,
    )


class TestFlinkCfuAllocatorUsageRatio:
    """Tests for usage-ratio allocation path."""

    def test_two_owners_proportional_split(self, flink_billing_line: BillingLineItem) -> None:
        """Two owners split cost by CFU ratio."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            stmt_owner_cfu={"sa-1": 75.0, "sa-2": 25.0},
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        row_by_id = {r.identity_id: r for r in result.rows}
        assert row_by_id["sa-1"].amount == Decimal("75.0000")
        assert row_by_id["sa-2"].amount == Decimal("25.0000")
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.allocation_method == "usage_ratio" for r in result.rows)

    def test_single_owner_gets_full_amount(self, flink_billing_line: BillingLineItem) -> None:
        """Single owner gets 100% of cost."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1"),
            stmt_owner_cfu={"sa-1": 50.0},
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE

    def test_three_owners_ratio_split(self, flink_billing_line: BillingLineItem) -> None:
        """Three owners split cost by ratio, remainder distributed correctly."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2", "sa-3"),
            stmt_owner_cfu={"sa-1": 10.0, "sa-2": 10.0, "sa-3": 10.0},
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 3
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")


class TestFlinkCfuAllocatorFallback:
    """Tests for fallback allocation paths."""

    def test_no_stmt_cfu_falls_back_to_merged_active(self, flink_billing_line: BillingLineItem) -> None:
        """Without stmt_owner_cfu, falls back to even split across merged_active."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.amount == Decimal("50") for r in result.rows)
        assert all(r.cost_type == CostType.USAGE for r in result.rows)

    def test_no_active_falls_back_to_tenant_period(self, flink_billing_line: BillingLineItem) -> None:
        """Without active identities, falls back to tenant_period."""
        resolution = _make_resolution(
            tenant_period=make_identity_set("sa-tenant"),
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.USAGE

    def test_no_identities_unallocated(self, flink_billing_line: BillingLineItem) -> None:
        """Without any identities, allocates to UNALLOCATED."""
        resolution = _make_resolution()
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE

    def test_empty_stmt_cfu_falls_back(self, flink_billing_line: BillingLineItem) -> None:
        """Empty stmt_owner_cfu dict triggers fallback path."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1"),
            stmt_owner_cfu={},
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"
        assert result.rows[0].cost_type == CostType.USAGE
