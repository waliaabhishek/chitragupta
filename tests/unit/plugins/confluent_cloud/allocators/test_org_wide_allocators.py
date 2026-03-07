"""Tests for org-wide allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext
from core.models import (
    CoreBillingLineItem,
    CoreIdentity,
    CostType,
    IdentityResolution,
    IdentitySet,
)


def _make_identity_set_typed(*entries: tuple[str, str]) -> IdentitySet:
    """Build an IdentitySet from (identity_id, identity_type) pairs."""
    s = IdentitySet()
    for iid, itype in entries:
        s.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=iid,
                identity_type=itype,
            )
        )
    return s


def _make_identity_set(*ids: str) -> IdentitySet:
    """Build an IdentitySet from identity IDs."""
    s = IdentitySet()
    for iid in ids:
        s.add(
            CoreIdentity(
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
        billing_line=CoreBillingLineItem(
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
            billing_line=CoreBillingLineItem(
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


def _make_ctx_typed(
    entries: list[tuple[str, str]],
    amount: Decimal = Decimal("100"),
) -> AllocationContext:
    """Build an AllocationContext with mixed identity types."""
    return AllocationContext(
        timeslice=datetime(2026, 2, 1, tzinfo=UTC),
        billing_line=CoreBillingLineItem(
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
            tenant_period=_make_identity_set_typed(*entries),
        ),
        split_amount=amount,
    )


class TestOrgWideAllocatorTypeFiltering:
    """GAP-10: org_wide_allocator must exclude api_key and system types."""

    def test_excludes_api_keys_splits_across_sa_and_pools_only(self) -> None:
        """10 SAs + 50 API keys + 3 pools -> 13 rows, not 63."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        sa_entries = [(f"sa-{i}", "service_account") for i in range(10)]
        api_key_entries = [(f"key-{i}", "api_key") for i in range(50)]
        pool_entries = [(f"pool-{i}", "identity_pool") for i in range(3)]
        ctx = _make_ctx_typed(sa_entries + api_key_entries + pool_entries, amount=Decimal("130"))

        result = org_wide_allocator(ctx)

        assert len(result.rows) == 13
        row_ids = {r.identity_id for r in result.rows}
        assert not any(rid.startswith("key-") for rid in row_ids)

    def test_excludes_system_unallocated_identity(self) -> None:
        """UNALLOCATED (identity_type=system) is excluded from split."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        entries = [
            ("sa-1", "service_account"),
            ("sa-2", "service_account"),
            ("UNALLOCATED", "system"),
        ]
        ctx = _make_ctx_typed(entries, amount=Decimal("60"))

        result = org_wide_allocator(ctx)

        assert len(result.rows) == 2
        row_ids = {r.identity_id for r in result.rows}
        assert "UNALLOCATED" not in row_ids

    def test_includes_identity_pool_type(self) -> None:
        """identity_pool type is included in the even split."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        entries = [
            ("sa-1", "service_account"),
            ("pool-1", "identity_pool"),
        ]
        ctx = _make_ctx_typed(entries, amount=Decimal("100"))

        result = org_wide_allocator(ctx)

        assert len(result.rows) == 2
        row_ids = {r.identity_id for r in result.rows}
        assert "pool-1" in row_ids

    def test_only_sa_user_pool_included_others_excluded(self) -> None:
        """service_account, user, identity_pool included; api_key and system excluded."""
        from plugins.confluent_cloud.allocators.org_wide_allocators import org_wide_allocator

        entries = [
            ("sa-1", "service_account"),
            ("u-1", "user"),
            ("pool-1", "identity_pool"),
            ("key-1", "api_key"),
            ("sys-1", "system"),
        ]
        ctx = _make_ctx_typed(entries, amount=Decimal("300"))

        result = org_wide_allocator(ctx)

        row_ids = {r.identity_id for r in result.rows}
        assert row_ids == {"sa-1", "u-1", "pool-1"}
