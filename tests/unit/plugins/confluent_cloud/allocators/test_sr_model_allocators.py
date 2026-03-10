"""Tests for Schema Registry SR_MODEL allocator (ChainModel-based)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import (
    BillingLineItem,
    CoreIdentity,
    IdentityResolution,
    IdentitySet,
)
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail, CostType
from plugins.confluent_cloud.allocators.sr_allocators import SR_MODEL, schema_registry_allocator


@pytest.fixture
def base_billing_line() -> BillingLineItem:
    """Standard SR billing line for tests."""
    return CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lsrc-xyz",
        product_category="SCHEMA_REGISTRY",
        product_type="SCHEMA_REGISTRY_USAGE",
        quantity=Decimal("1"),
        unit_price=Decimal("50"),
        total_cost=Decimal("50"),
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


class TestSRModelTier0ActiveIdentities:
    """Tier 0: merged_active identities get USAGE cost type."""

    def test_sr_model_tier0_resource_active_only_two_identities(self, base_billing_line: BillingLineItem) -> None:
        """resource_active with sa-1, sa-2; no metrics_derived or tenant_period."""
        resolution = _make_resolution(resource_active=["sa-1", "sa-2"])
        ctx = _make_ctx(base_billing_line, resolution, Decimal("50"))

        result = SR_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        assert all(row.cost_type == CostType.USAGE for row in result.rows)
        assert sum(row.amount for row in result.rows) == Decimal("50")
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)

    def test_sr_model_tier0_metrics_derived_merges_into_merged_active(self, base_billing_line: BillingLineItem) -> None:
        """metrics_derived sa-2 merges with resource_active sa-1 into merged_active."""
        resolution = _make_resolution(
            resource_active=["sa-1"],
            metrics_derived=["sa-2"],
        )
        ctx = _make_ctx(base_billing_line, resolution, Decimal("50"))

        result = SR_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        identity_ids = {row.identity_id for row in result.rows}
        assert "sa-1" in identity_ids
        assert "sa-2" in identity_ids
        assert all(row.cost_type == CostType.USAGE for row in result.rows)
        assert all(row.metadata["chain_tier"] == 0 for row in result.rows)


class TestSRModelTier1TenantPeriodFallback:
    """Tier 1: tenant_period fallback when no merged_active identities."""

    def test_sr_model_tier1_tenant_period_shared_with_detail(self, base_billing_line: BillingLineItem) -> None:
        """No resource_active or metrics_derived; tenant_period has sa-tenant."""
        resolution = _make_resolution(tenant_period=["sa-tenant"])
        ctx = _make_ctx(base_billing_line, resolution, Decimal("50"))

        result = SR_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 1


class TestSRModelTier2Terminal:
    """Tier 2: terminal fallback to resource_id when all identity sets empty."""

    def test_sr_model_tier2_terminal_to_resource_id_not_unallocated(self, base_billing_line: BillingLineItem) -> None:
        """All identity sets empty — terminal assigns to resource_id, not UNALLOCATED."""
        resolution = _make_resolution()
        ctx = _make_ctx(base_billing_line, resolution, Decimal("50"))

        result = SR_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "lsrc-xyz"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].identity_id != "UNALLOCATED"


class TestSRModelRemainderDistribution:
    """Remainder distribution across uneven splits."""

    def test_sr_model_remainder_distributed_correctly(self, base_billing_line: BillingLineItem) -> None:
        """3 active identities, split_amount=10 — total stays exact, no penny lost."""
        resolution = _make_resolution(resource_active=["sa-1", "sa-2", "sa-3"])
        ctx = _make_ctx(base_billing_line, resolution, Decimal("10"))

        result = SR_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 3
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("10")
        amounts = {row.amount for row in result.rows}
        assert amounts <= {Decimal("3.3333"), Decimal("3.3334")}


class TestSRModelAlias:
    """SR_MODEL identity check — schema_registry_allocator must be SR_MODEL."""

    def test_schema_registry_allocator_is_sr_model(self) -> None:
        """schema_registry_allocator is SR_MODEL — not a re-implementation."""
        assert schema_registry_allocator is SR_MODEL
