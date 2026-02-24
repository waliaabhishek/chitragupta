"""Tests for org-wide allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext
from core.models import BillingLineItem, CostType, Identity, IdentityResolution, IdentitySet


def _make_identity_set(*ids: str) -> IdentitySet:
    """Build an IdentitySet from identity IDs."""
    s = IdentitySet()
    for iid in ids:
        s.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=iid,
                identity_type="service_account",
            )
        )
    return s


def _make_ctx(
    tenant_period_ids: tuple[str, ...] = (),
    amount: Decimal = Decimal("100"),
) -> AllocationContext:
    """Build a minimal AllocationContext for org-wide tests."""
    return AllocationContext(
        timeslice=datetime(2026, 2, 1, tzinfo=UTC),
        billing_line=BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="org-123",
            product_category="PLATFORM",
            product_type="AUDIT_LOG_READ",
            quantity=Decimal("1"),
            unit_price=amount,
            total_cost=amount,
        ),
        identities=IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=_make_identity_set(*tenant_period_ids),
        ),
        split_amount=amount,
    )


class TestOrgWideAllocator:
    """Tests for org_wide_allocator."""

    def test_even_split_three_identities(self) -> None:
        """$100 split across 3 identities -> ~$33.3333 each."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        ctx = _make_ctx(tenant_period_ids=("sa-1", "sa-2", "sa-3"))
        result = org_wide_allocator(ctx)

        assert len(result.rows) == 3
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")
        assert all(r.cost_type == CostType.SHARED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)

    def test_single_identity_gets_all(self) -> None:
        """Single identity gets full amount."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        ctx = _make_ctx(tenant_period_ids=("sa-1",))
        result = org_wide_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_no_identities_unallocated(self) -> None:
        """No identities -> UNALLOCATED."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        ctx = _make_ctx(tenant_period_ids=())
        result = org_wide_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_uses_tenant_period_not_merged_active(self) -> None:
        """Verifies org_wide_allocator uses tenant_period scope."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        # tenant_period has sa-1, sa-2; resource_active has sa-3 only
        ctx = AllocationContext(
            timeslice=datetime(2026, 2, 1, tzinfo=UTC),
            billing_line=BillingLineItem(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                resource_id="org-123",
                product_category="PLATFORM",
                product_type="SUPPORT",
                quantity=Decimal("1"),
                unit_price=Decimal("60"),
                total_cost=Decimal("60"),
            ),
            identities=IdentityResolution(
                resource_active=_make_identity_set("sa-3"),
                metrics_derived=IdentitySet(),
                tenant_period=_make_identity_set("sa-1", "sa-2"),
            ),
            split_amount=Decimal("60"),
        )
        result = org_wide_allocator(ctx)

        assert len(result.rows) == 2
        row_ids = {r.identity_id for r in result.rows}
        assert row_ids == {"sa-1", "sa-2"}
        assert "sa-3" not in row_ids

    def test_deterministic_order(self) -> None:
        """Identity IDs are sorted for deterministic allocation."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        ctx = _make_ctx(tenant_period_ids=("sa-c", "sa-a", "sa-b"), amount=Decimal("99"))
        result = org_wide_allocator(ctx)

        assert [r.identity_id for r in result.rows] == ["sa-a", "sa-b", "sa-c"]
