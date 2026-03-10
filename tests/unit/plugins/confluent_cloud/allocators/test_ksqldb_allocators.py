"""Tests for ksqlDB allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import (
    BillingLineItem,
    CoreIdentity,
    CostType,
    IdentityResolution,
    IdentitySet,
)
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail
from plugins.confluent_cloud.allocation_models import KSQLDB_MODEL
from plugins.confluent_cloud.allocators.ksqldb_allocators import ksqldb_csu_allocator


@pytest.fixture
def ksqldb_billing_line() -> BillingLineItem:
    """Standard ksqlDB billing line for tests."""
    return CoreBillingLineItem(
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
            CoreIdentity(
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

    def test_tenant_period_splits_evenly_across_real_identities(self, ksqldb_billing_line: BillingLineItem) -> None:
        """GAP-23: tenant_period fallback splits evenly across all real identities."""
        tp = IdentitySet()
        for sa_id in ("sa-1", "sa-2", "sa-3"):
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
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=resolution,
            split_amount=Decimal("90"),
            metrics_data=None,
            params={},
        )

        result = ksqldb_csu_allocator(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert recipient_ids == {"sa-1", "sa-2", "sa-3"}
        assert len(result.rows) == 3

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

    def test_fallback_tenant_period_filters_to_owner_identity_types(self, ksqldb_billing_line: BillingLineItem) -> None:
        """GAP-059: tenant_period fallback must exclude api_key and system identities."""
        tp = IdentitySet()
        tp.add(
            CoreIdentity(
                ecosystem="confluent_cloud", tenant_id="org-123", identity_id="user-owner", identity_type="user"
            )
        )
        tp.add(
            CoreIdentity(
                ecosystem="confluent_cloud", tenant_id="org-123", identity_id="UNALLOCATED", identity_type="system"
            )
        )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tp,
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

        assert {r.identity_id for r in result.rows} == {"user-owner"}
        assert len(result.rows) == 1
        assert result.rows[0].cost_type == CostType.SHARED

    def test_fallback_terminal_when_tenant_period_has_only_non_owners(
        self, ksqldb_billing_line: BillingLineItem
    ) -> None:
        """GAP-059: when tenant_period has only api_key/system, fall through to allocate_to_resource."""
        tp = IdentitySet()
        tp.add(
            CoreIdentity(
                ecosystem="confluent_cloud", tenant_id="org-123", identity_id="api-key-1", identity_type="api_key"
            )
        )
        tp.add(
            CoreIdentity(
                ecosystem="confluent_cloud", tenant_id="org-123", identity_id="UNALLOCATED", identity_type="system"
            )
        )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tp,
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
        assert result.rows[0].cost_type == CostType.SHARED


# --- KSQLDB_MODEL (ChainModel-based) tests ---


@pytest.fixture
def ksqldb_chain_billing_line() -> BillingLineItem:
    """Standard ksqlDB billing line for KSQLDB_MODEL tests."""
    return CoreBillingLineItem(
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


def _make_identity(identity_id: str) -> CoreIdentity:
    return CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id=identity_id,
        identity_type="service_account",
    )


def _make_resolution(
    resource_active: list[str] | None = None,
    metrics_derived: list[str] | None = None,
    tenant_period: list[str] | None = None,
) -> IdentityResolution:
    def _iset(ids: list[str] | None) -> IdentitySet:
        iset = IdentitySet()
        for i in ids or []:
            iset.add(_make_identity(i))
        return iset

    return IdentityResolution(
        resource_active=_iset(resource_active),
        metrics_derived=_iset(metrics_derived),
        tenant_period=_iset(tenant_period),
    )


def _make_ctx(
    billing_line: BillingLineItem,
    resolution: IdentityResolution,
    split_amount: Decimal,
) -> AllocationContext:
    return AllocationContext(
        timeslice=billing_line.timestamp,
        billing_line=billing_line,
        identities=resolution,
        split_amount=split_amount,
        metrics_data=None,
        params={},
    )


class TestKsqldbModelAlias:
    """KSQLDB_MODEL identity check — ksqldb_csu_allocator must be KSQLDB_MODEL."""

    def test_ksqldb_csu_allocator_is_ksqldb_model(self) -> None:
        """ksqldb_csu_allocator is KSQLDB_MODEL — not a re-implementation."""
        assert ksqldb_csu_allocator is KSQLDB_MODEL


class TestKsqldbModelTier0:
    """Tier 0: merged_active identities → USAGE cost type, chain_tier=0, detail=None."""

    def test_ksqldb_model_tier0_resource_active_two_identities(
        self, ksqldb_chain_billing_line: BillingLineItem
    ) -> None:
        """resource_active with sa-1, sa-2 → USAGE rows, chain_tier=0, no detail."""
        resolution = _make_resolution(resource_active=["sa-1", "sa-2"])
        ctx = _make_ctx(ksqldb_chain_billing_line, resolution, Decimal("100"))

        result = KSQLDB_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        assert all(row.cost_type == CostType.USAGE for row in result.rows)
        assert all(row.allocation_detail is None for row in result.rows)
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)
        assert sum(row.amount for row in result.rows) == Decimal("100")

    def test_ksqldb_model_tier0_metrics_derived_merges_into_merged_active(
        self, ksqldb_chain_billing_line: BillingLineItem
    ) -> None:
        """metrics_derived merges with resource_active into merged_active for Tier 0."""
        resolution = _make_resolution(resource_active=["sa-1"], metrics_derived=["sa-2"])
        ctx = _make_ctx(ksqldb_chain_billing_line, resolution, Decimal("100"))

        result = KSQLDB_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        identity_ids = {row.identity_id for row in result.rows}
        assert "sa-1" in identity_ids
        assert "sa-2" in identity_ids
        assert all(row.cost_type == CostType.USAGE for row in result.rows)
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)


class TestKsqldbModelTier1:
    """Tier 1: merged_active empty, tenant_period has identities → SHARED, chain_tier=1."""

    def test_ksqldb_model_tier1_tenant_period_shared_with_detail(
        self, ksqldb_chain_billing_line: BillingLineItem
    ) -> None:
        """No merged_active; tenant_period sa-tenant → SHARED with NO_ACTIVE_IDENTITIES_LOCATED."""
        resolution = _make_resolution(tenant_period=["sa-tenant"])
        ctx = _make_ctx(ksqldb_chain_billing_line, resolution, Decimal("100"))

        result = KSQLDB_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 1


class TestKsqldbModelTier2:
    """Tier 2: all identity sets empty → identity_id=resource_id, SHARED, chain_tier=2."""

    def test_ksqldb_model_tier2_terminal_to_resource_id(self, ksqldb_chain_billing_line: BillingLineItem) -> None:
        """All identity sets empty → terminal assigns to resource_id with NO_IDENTITIES_LOCATED."""
        resolution = _make_resolution()
        ctx = _make_ctx(ksqldb_chain_billing_line, resolution, Decimal("100"))

        result = KSQLDB_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "ksqldb-cluster-xyz"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].identity_id != "UNALLOCATED"
