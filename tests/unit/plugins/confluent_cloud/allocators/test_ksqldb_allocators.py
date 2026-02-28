"""Tests for ksqlDB allocators."""

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
from plugins.confluent_cloud.allocators.ksqldb_allocators import ksqldb_csu_allocator


@pytest.fixture
def ksqldb_billing_line() -> BillingLineItem:
    """Standard ksqlDB billing line for tests."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="ksqldb-cluster-xyz",
        product_category="KSQL",
        product_type="KSQL_CSU",
        quantity=Decimal("4"),
        unit_price=Decimal("25"),
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


class TestKsqldbCsuAllocator:
    """Tests for ksqldb_csu_allocator (even split, USAGE cost type)."""

    def test_even_split_two_identities(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Even split across two identities with USAGE cost type."""
        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.amount == Decimal("50") for r in result.rows)
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)

    def test_no_identities_unallocated(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to resource_id with SHARED cost type."""
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == ksqldb_billing_line.resource_id
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_fallback_to_tenant_period(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period with SHARED cost type when merged_active is empty."""
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant"),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.SHARED

    def test_three_way_split_with_remainder(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Three identities splitting $10 handles remainder correctly."""
        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2", "sa-3"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("10"),  # 10 / 3 = 3.3333...
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        assert len(result.rows) == 3
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("10")
        # All amounts should be close to 3.33
        for row in result.rows:
            assert Decimal("3.33") <= row.amount <= Decimal("3.34")
            assert row.cost_type == CostType.USAGE

    def test_uses_merged_active_over_tenant_period(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Merged active identities take precedence over tenant_period."""
        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-active"),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant-1", "sa-tenant-2"),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        # Should use merged_active (1 identity), not tenant_period (2 identities)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-active"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE

    def test_single_identity_full_amount(self, ksqldb_billing_line: BillingLineItem) -> None:
        """Single identity gets full amount with USAGE cost type."""
        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-only"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-only"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE
        assert result.rows[0].allocation_method == "even_split"
