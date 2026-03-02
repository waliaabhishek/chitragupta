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

    def test_no_identities_resource_local(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, falls back to resource-local (identity_id == resource_id)."""
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
        assert result.rows[0].identity_id == connect_billing_line.resource_id
        assert result.rows[0].amount == Decimal("100")


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

    def test_no_identities_resource_local(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, falls back to resource-local (identity_id == resource_id)."""
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
        assert result.rows[0].identity_id == connect_billing_line.resource_id
        assert result.rows[0].amount == Decimal("100")

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

    def test_no_identities_resource_local(self, connect_billing_line: BillingLineItem) -> None:
        """Without identities, falls back to resource-local (identity_id == resource_id)."""
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
        assert result.rows[0].identity_id == connect_billing_line.resource_id
        assert result.rows[0].amount == Decimal("100")


class TestConnectorResourceLocalFallback:
    """GAP-24: Empty merged_active must fall back to resource-local, not tenant-period."""

    def test_capacity_empty_merged_active_identity_id_is_resource_id(
        self, connect_billing_line: BillingLineItem
    ) -> None:
        """Capacity: empty merged_active with tenant_period identities → single row with identity_id == resource_id."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant-1", "sa-tenant-2"),
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
        assert result.rows[0].identity_id == connect_billing_line.resource_id
        assert result.rows[0].identity_id == "connector-xyz"
        assert result.rows[0].amount == Decimal("100")

    def test_tasks_empty_merged_active_identity_id_is_resource_id(self, connect_billing_line: BillingLineItem) -> None:
        """Tasks: empty merged_active with tenant_period identities → single row with identity_id == resource_id."""
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_tasks_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant-1", "sa-tenant-2"),
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
        assert result.rows[0].identity_id == connect_billing_line.resource_id
        assert result.rows[0].identity_id == "connector-xyz"
        assert result.rows[0].amount == Decimal("100")

    def test_capacity_active_identities_even_split(self, connect_billing_line: BillingLineItem) -> None:
        """Capacity: active identities present → even split (unchanged behavior)."""
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
        assert {r.identity_id for r in result.rows} == {"sa-1", "sa-2"}
        assert all(r.amount == Decimal("50") for r in result.rows)

    def test_tasks_active_identities_even_split(self, connect_billing_line: BillingLineItem) -> None:
        """Tasks: active identities present → even split (unchanged behavior)."""
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
        assert {r.identity_id for r in result.rows} == {"sa-1", "sa-2"}
        assert all(r.amount == Decimal("50") for r in result.rows)

    def test_capacity_fallback_row_allocation_method_to_resource(self, connect_billing_line: BillingLineItem) -> None:
        """Capacity: fallback row must have allocation_method == 'to_resource'."""
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
        assert result.rows[0].allocation_method == "to_resource"

    def test_tasks_fallback_row_allocation_method_to_resource(self, connect_billing_line: BillingLineItem) -> None:
        """Tasks: fallback row must have allocation_method == 'to_resource'."""
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
        assert result.rows[0].allocation_method == "to_resource"

    def test_parity_legacy_resource_local_fallback(self, connect_billing_line: BillingLineItem) -> None:
        """Parity: connector with no active identities produces same structural result as legacy.

        Legacy behavior: single row with principal=connector_id (resource-local assignment).
        New behavior: single row with identity_id=resource_id, allocation_method='to_resource'.
        Both represent "charge this connector's cost back to the connector itself."
        """
        from plugins.confluent_cloud.allocators.connector_allocators import (
            connect_capacity_allocator,
        )

        # Simulate a billing date where the connector has no active identities
        # but has tenant_period identities (the scenario where legacy and new diverge)
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=make_identity_set("sa-tenant-1", "sa-tenant-2"),
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

        # Legacy produced: 1 row, principal == connector_id, full amount
        # New produces the same structural pattern via allocate_to_resource
        legacy_expected_principal = connect_billing_line.resource_id  # "connector-xyz"
        assert len(result.rows) == 1, "parity: legacy emitted exactly one row"
        assert result.rows[0].identity_id == legacy_expected_principal, (
            "parity: identity_id must equal resource_id, matching legacy principal=connector_id"
        )
        assert result.rows[0].amount == Decimal("100"), "parity: full amount assigned to resource row"
        assert result.rows[0].allocation_method == "to_resource", (
            "parity: allocation_method signals resource-local, not tenant-wide"
        )
