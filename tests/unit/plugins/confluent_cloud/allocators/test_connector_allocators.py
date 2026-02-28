"""Tests for Connect allocators."""

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


@pytest.fixture
def connect_billing_line() -> BillingLineItem:
    """Standard Connect billing line for tests."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="connector-xyz",
        product_category="CONNECT",
        product_type="CONNECT_CAPACITY",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )


def make_identity_set_typed(*entries: tuple[str, str]) -> IdentitySet:
    """Create an IdentitySet from (identity_id, identity_type) pairs."""
    iset = IdentitySet()
    for identity_id, identity_type in entries:
        iset.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=identity_id,
                identity_type=identity_type,
            )
        )
    return iset


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


class TestConnectCapacityAllocator:
    """Tests for connect_capacity_allocator (even split, SHARED cost type)."""

    def test_even_split_two_identities(self, connect_billing_line: BillingLineItem) -> None:
        """Even split across two identities with SHARED cost type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_capacity_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.amount == Decimal("50") for r in result.rows)
        assert all(r.cost_type == CostType.SHARED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)

    def test_no_identities_unallocated(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_capacity_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_fallback_to_tenant_period(self, connect_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period when merged_active is empty."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant"),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_capacity_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.SHARED


class TestConnectTasksAllocator:
    """Tests for connect_tasks_allocator (even split, USAGE cost type)."""

    def test_even_split_two_identities(self, connect_billing_line: BillingLineItem) -> None:
        """Even split across two identities with USAGE cost type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.amount == Decimal("50") for r in result.rows)
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)

    def test_no_identities_unallocated(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED with USAGE cost type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE

    def test_fallback_to_tenant_period(self, connect_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period when merged_active is empty."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant"),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.USAGE

    def test_three_way_split_with_remainder(self, connect_billing_line: BillingLineItem) -> None:
        """Three identities splitting $10 handles remainder correctly."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2", "sa-3"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("10"),  # 10 / 3 = 3.3333...
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        assert len(result.rows) == 3
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("10")
        # All amounts should be close to 3.33
        for row in result.rows:
            assert Decimal("3.33") <= row.amount <= Decimal("3.34")
            assert row.cost_type == CostType.USAGE


class TestConnectThroughputAllocator:
    """Tests for connect_throughput_allocator (delegates to tasks allocator)."""

    def test_delegates_to_tasks_allocator(self, connect_billing_line: BillingLineItem) -> None:
        """Throughput allocator delegates to tasks allocator."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
            connect_throughput_allocator,
        )

        resolution = IdentityResolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        throughput_result = connect_throughput_allocator(ctx)
        tasks_result = connect_tasks_allocator(ctx)

        # Results should be identical
        assert len(throughput_result.rows) == len(tasks_result.rows)
        assert sum(r.amount for r in throughput_result.rows) == sum(r.amount for r in tasks_result.rows)
        assert all(r.cost_type == CostType.USAGE for r in throughput_result.rows)

    def test_no_identities_unallocated(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED with USAGE cost type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_throughput_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_throughput_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].cost_type == CostType.USAGE


class TestConnectCapacityAllocatorTenantPeriodTypeFiltering:
    """GAP-10: tenant_period fallback must exclude api_key and system types."""

    def test_fallback_tenant_period_excludes_api_keys(self, connect_billing_line: BillingLineItem) -> None:
        """When merged_active is empty, tenant_period fallback excludes API keys."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set_typed(
                ("sa-1", "service_account"),
                ("pool-1", "identity_pool"),
                ("key-1", "api_key"),
                ("key-2", "api_key"),
            ),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_capacity_allocator(ctx)

        row_ids = {r.identity_id for r in result.rows}
        assert row_ids == {"sa-1", "pool-1"}
        assert len(result.rows) == 2

    def test_fallback_tenant_period_excludes_system_unallocated(self, connect_billing_line: BillingLineItem) -> None:
        """When merged_active is empty, tenant_period fallback excludes system type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set_typed(
                ("sa-1", "service_account"),
                ("UNALLOCATED", "system"),
            ),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_capacity_allocator(ctx)

        row_ids = {r.identity_id for r in result.rows}
        assert "UNALLOCATED" not in row_ids
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"


class TestConnectTasksAllocatorTenantPeriodTypeFiltering:
    """GAP-10: tenant_period fallback in tasks allocator must exclude api_key and system."""

    def test_fallback_tenant_period_excludes_api_keys(self, connect_billing_line: BillingLineItem) -> None:
        """When merged_active is empty, tenant_period fallback excludes API keys."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set_typed(
                ("sa-1", "service_account"),
                ("pool-1", "identity_pool"),
                ("key-1", "api_key"),
                ("key-2", "api_key"),
            ),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        row_ids = {r.identity_id for r in result.rows}
        assert row_ids == {"sa-1", "pool-1"}
        assert len(result.rows) == 2
        assert all(r.cost_type == CostType.USAGE for r in result.rows)

    def test_fallback_tenant_period_excludes_system_unallocated(self, connect_billing_line: BillingLineItem) -> None:
        """When merged_active is empty, tenant_period fallback excludes system type."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set_typed(
                ("sa-1", "service_account"),
                ("UNALLOCATED", "system"),
            ),
        )
        ctx = AllocationContext(
            timeslice=connect_billing_line.timestamp,
            billing_line=connect_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = connect_tasks_allocator(ctx)

        row_ids = {r.identity_id for r in result.rows}
        assert "UNALLOCATED" not in row_ids
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"
        assert result.rows[0].cost_type == CostType.USAGE
