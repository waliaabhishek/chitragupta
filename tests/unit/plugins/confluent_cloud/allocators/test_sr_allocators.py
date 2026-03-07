"""Tests for Schema Registry allocator."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    CoreIdentity,
    IdentityResolution,
    IdentitySet,
)
from core.models.billing import CoreBillingLineItem


@pytest.fixture
def sr_billing_line() -> BillingLineItem:
    """Standard Schema Registry billing line for tests."""
    return CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lsrc-xyz",
        product_category="SCHEMA_REGISTRY",
        product_type="SCHEMA_REGISTRY",
        quantity=Decimal("1"),
        unit_price=Decimal("50"),
        total_cost=Decimal("50"),
    )


class TestSchemaRegistryAllocator:
    """Tests for schema_registry_allocator (even split)."""

    def test_even_split_two_identities(self, sr_billing_line: BillingLineItem) -> None:
        """Even split across two identities."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        iset = IdentitySet()
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-1",
                identity_type="service_account",
            )
        )
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-2",
                identity_type="service_account",
            )
        )

        resolution = IdentityResolution(
            resource_active=iset,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("50"),
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("50")
        assert all(r.amount == Decimal("25") for r in result.rows)

    def test_no_identities_unallocated(self, sr_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("50"),
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("50")

    def test_single_identity_full_amount(self, sr_billing_line: BillingLineItem) -> None:
        """Single identity gets full amount."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        iset = IdentitySet()
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-solo",
                identity_type="service_account",
            )
        )

        resolution = IdentityResolution(
            resource_active=iset,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("50"),
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-solo"
        assert result.rows[0].amount == Decimal("50")

    def test_uses_tenant_period_fallback(self, sr_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period when merged_active is empty."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-tenant",
                identity_type="service_account",
            )
        )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tenant_period,
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("50"),
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"

    def test_tenant_period_splits_evenly_across_real_identities(self, sr_billing_line: BillingLineItem) -> None:
        """GAP-23: tenant_period fallback splits evenly across all real identities."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        tp = IdentitySet()
        for sa_id in ("sa-1", "sa-2"):
            tp.add(
                CoreIdentity(
                    ecosystem="confluent_cloud",
                    tenant_id="org-123",
                    identity_id=sa_id,
                    identity_type="service_account",
                )
            )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tp,
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("50"),
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert recipient_ids == {"sa-1", "sa-2"}
        assert len(result.rows) == 2

    def test_schema_registry_allocator_is_allocate_evenly_with_fallback(self) -> None:
        """TASK-024: schema_registry_allocator must be allocate_evenly_with_fallback after fix."""
        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.confluent_cloud.allocators.sr_allocators import schema_registry_allocator

        assert schema_registry_allocator is allocate_evenly_with_fallback

    def test_three_way_split_with_remainder(self, sr_billing_line: BillingLineItem) -> None:
        """Three identities splitting $10 handles remainder correctly."""
        from plugins.confluent_cloud.allocators.sr_allocators import (
            schema_registry_allocator,
        )

        iset = IdentitySet()
        for i in range(1, 4):
            iset.add(
                CoreIdentity(
                    ecosystem="confluent_cloud",
                    tenant_id="org-123",
                    identity_id=f"sa-{i}",
                    identity_type="service_account",
                )
            )

        resolution = IdentityResolution(
            resource_active=iset,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=sr_billing_line.timestamp,
            billing_line=sr_billing_line,
            identities=resolution,
            split_amount=Decimal("10"),  # 10 / 3 = 3.3333...
            metrics_data=None,
            params={},
        )

        result = schema_registry_allocator(ctx)

        assert len(result.rows) == 3
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("10")
        # All amounts should be close to 3.33
        for row in result.rows:
            assert Decimal("3.33") <= row.amount <= Decimal("3.34")
