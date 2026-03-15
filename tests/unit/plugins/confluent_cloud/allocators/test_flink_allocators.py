"""Tests for Flink allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.allocation_models import ChainModel
from core.models import (
    BillingLineItem,
    CoreIdentity,
    CostType,
    IdentityResolution,
    IdentitySet,
)
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail
from plugins.confluent_cloud.allocators.flink_allocators import flink_cfu_allocator


@pytest.fixture
def flink_billing_line() -> BillingLineItem:
    """Standard Flink billing line for tests."""
    return CoreBillingLineItem(
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
            CoreIdentity(
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
    """Tests for Tier 3 terminal fallback — all identity sets empty, no CFU data."""

    def test_no_metrics_data_terminal_to_resource_id(self, flink_billing_line: BillingLineItem) -> None:
        """No metrics_data, no identities → Tier 3 terminal: resource_id, SHARED, NO_IDENTITIES_LOCATED."""
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
        assert result.rows[0].identity_id == "lfcp-pool-1"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 3

    def test_metrics_present_no_owner_map_terminal_to_resource_id(self, flink_billing_line: BillingLineItem) -> None:
        """metrics_data present, no stmt_owner_cfu, no identities → Tier 3 terminal: resource_id, SHARED."""
        resolution = _make_resolution()
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data={"confluent_flink_num_cfu": []},
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "lfcp-pool-1"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 3

    def test_empty_stmt_cfu_no_identities_terminal_to_resource_id(self, flink_billing_line: BillingLineItem) -> None:
        """Empty stmt_owner_cfu, no metrics_data, no identities → Tier 3 terminal: resource_id, SHARED."""
        resolution = _make_resolution(stmt_owner_cfu={})
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
        assert result.rows[0].identity_id == "lfcp-pool-1"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 3


class TestFlinkCfuAllocatorZeroCfuFallback:
    """Tests for zero-CFU even-split fallback (sub-gap B fix).

    When metrics are present but stmt_owner_cfu is empty (all CFU values zero),
    the allocator should even-split across merged_active instead of going UNALLOCATED.
    """

    def test_zero_cfu_two_identities_even_split_usage(self, flink_billing_line: BillingLineItem) -> None:
        """Metrics present but zero CFU → even split across 2 identities, USAGE cost type."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data={"confluent_flink_num_cfu": []},
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-1", "sa-2"}
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert "UNALLOCATED" not in identity_ids

    def test_zero_cfu_three_identities_even_split_usage(self, flink_billing_line: BillingLineItem) -> None:
        """Metrics present but zero CFU → even split across 3 identities, totals match."""
        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2", "sa-3"),
        )
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=resolution,
            split_amount=Decimal("99"),
            metrics_data={"confluent_flink_num_cfu": []},
            params={},
        )

        result = flink_cfu_allocator(ctx)

        assert len(result.rows) == 3
        assert sum(r.amount for r in result.rows) == Decimal("99")
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-1", "sa-2", "sa-3"}
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert "UNALLOCATED" not in identity_ids


# ---------------------------------------------------------------------------
# FLINK_MODEL (ChainModel-based) tests — task-070
# These tests assert the NEW composable model structure and WILL FAIL until
# FLINK_MODEL is added to allocation_models.py and flink_cfu_allocator is
# updated to be an alias for it.
# ---------------------------------------------------------------------------


def _make_ctx_with_resolution(
    billing_line: BillingLineItem,
    resolution: IdentityResolution,
    split_amount: Decimal,
    metrics_data: dict | None = None,
) -> AllocationContext:
    return AllocationContext(
        timeslice=billing_line.timestamp,
        billing_line=billing_line,
        identities=resolution,
        split_amount=split_amount,
        metrics_data=metrics_data,
        params={},
    )


class TestFlinkModelExported:
    """FLINK_MODEL is exported from allocation_models as a 4-tier ChainModel."""

    def test_flink_model_is_chain_model_instance(self) -> None:
        """FLINK_MODEL must be a ChainModel instance."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        assert isinstance(FLINK_MODEL, ChainModel)

    def test_flink_model_has_four_tiers(self) -> None:
        """FLINK_MODEL must have exactly 4 models in the chain."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        assert len(FLINK_MODEL.models) == 4


class TestFlinkModelTier0UsageRatio:
    """Tier 0: UsageRatioModel — stmt_owner_cfu produces proportional USAGE rows."""

    def test_tier0_proportional_75_25_split(self, flink_billing_line: BillingLineItem) -> None:
        """sa-A gets 75%, sa-B gets 25% based on CFU 3.0/1.0 ratio."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(stmt_owner_cfu={"sa-A": 3.0, "sa-B": 1.0})
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        row_by_id = {r.identity_id: r for r in result.rows}
        assert row_by_id["sa-A"].amount == Decimal("75.0000")
        assert row_by_id["sa-B"].amount == Decimal("25.0000")
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)


class TestFlinkModelTier1EvenSplitActive:
    """Tier 1: EvenSplitModel(merged_active) — empty stmt_owner_cfu, USAGE + NO_USAGE_FOR_ACTIVE_IDENTITIES."""

    def test_tier1_even_split_two_merged_active_identities(self, flink_billing_line: BillingLineItem) -> None:
        """Empty CFU, two merged_active identities → equal USAGE rows, chain_tier=1."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            stmt_owner_cfu={},
        )
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-1", "sa-2"}
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES for r in result.rows)
        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)

    def test_tier1_chain_tier_is_one(self, flink_billing_line: BillingLineItem) -> None:
        """Empty stmt_owner_cfu + merged_active identities → chain_tier=1."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(
            resource_active=make_identity_set("sa-X"),
            stmt_owner_cfu={},
        )
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("50"))

        result = FLINK_MODEL(ctx)

        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)


class TestFlinkModelTier2TenantPeriod:
    """Tier 2: EvenSplitModel(tenant_period) — empty merged_active, SHARED + NO_ACTIVE_IDENTITIES_LOCATED."""

    def test_tier2_even_split_two_tenant_period_owners(self, flink_billing_line: BillingLineItem) -> None:
        """Empty CFU + empty merged_active, two tenant_period identities → SHARED, chain_tier=2."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(
            tenant_period=make_identity_set("sa-tp-1", "sa-tp-2"),
            stmt_owner_cfu={},
        )
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-tp-1", "sa-tp-2"}
        assert all(r.cost_type == CostType.SHARED for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.allocation_detail == AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED for r in result.rows)
        assert all(r.metadata["chain_tier"] == 2 for r in result.rows)

    def test_tier2_chain_tier_is_two(self, flink_billing_line: BillingLineItem) -> None:
        """Empty CFU + empty merged_active + tenant_period identities → chain_tier=2."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(
            tenant_period=make_identity_set("sa-tp"),
            stmt_owner_cfu={},
        )
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("50"))

        result = FLINK_MODEL(ctx)

        assert all(r.metadata["chain_tier"] == 2 for r in result.rows)


class TestFlinkModelTier3Terminal:
    """Tier 3: TerminalModel — all identity sets empty, identity_id=resource_id, SHARED + NO_IDENTITIES_LOCATED."""

    def test_tier3_terminal_to_resource_id_not_unallocated(self, flink_billing_line: BillingLineItem) -> None:
        """All identity sources empty → terminal assigns to resource_id, NOT UNALLOCATED."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(stmt_owner_cfu={})
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == flink_billing_line.resource_id
        assert result.rows[0].identity_id == "lfcp-pool-1"
        assert result.rows[0].identity_id != "UNALLOCATED"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 3

    def test_tier3_chain_tier_is_three(self, flink_billing_line: BillingLineItem) -> None:
        """All identity sets empty → chain_tier=3 in terminal row."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution()
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("50"))

        result = FLINK_MODEL(ctx)

        assert result.rows[0].metadata["chain_tier"] == 3

    def test_tier3_cost_type_is_shared_not_usage(self, flink_billing_line: BillingLineItem) -> None:
        """Terminal tier must use SHARED cost type (not USAGE like old implementation)."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution()
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert result.rows[0].cost_type == CostType.SHARED


class TestFlinkModelRounding:
    """Penny test: Tier 0 row amounts must sum exactly to split_amount."""

    def test_tier0_one_third_split_penny_exact(self, flink_billing_line: BillingLineItem) -> None:
        """1.0/3.0 CFU split — sum of amounts must equal split_amount exactly."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(stmt_owner_cfu={"sa-A": 1.0, "sa-B": 3.0})
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("100"))

        result = FLINK_MODEL(ctx)

        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_tier0_three_way_uneven_cfu_penny_exact(self, flink_billing_line: BillingLineItem) -> None:
        """Three owners with 1/6/3 CFU → 10% / 60% / 30%, sum must equal split_amount."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        resolution = _make_resolution(stmt_owner_cfu={"sa-A": 1.0, "sa-B": 6.0, "sa-C": 3.0})
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("33.33"))

        result = FLINK_MODEL(ctx)

        assert sum(r.amount for r in result.rows) == Decimal("33.33")


class TestFlinkCfuAllocatorIsFlinkModel:
    """flink_cfu_allocator must be an alias for FLINK_MODEL (not a standalone function)."""

    def test_flink_cfu_allocator_is_flink_model(self) -> None:
        """flink_cfu_allocator is FLINK_MODEL — not a re-implementation."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL

        assert flink_cfu_allocator is FLINK_MODEL


class TestFlinkModelIntegration:
    """Integration: FlinkHandler → get_allocator → FLINK_MODEL → allocation rows."""

    def test_handler_allocator_produces_chain_tier_zero_for_cfu_data(self, flink_billing_line: BillingLineItem) -> None:
        """FlinkHandler.get_allocator returns FLINK_MODEL; with stmt_owner_cfu → chain_tier=0, USAGE."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        allocator = handler.get_allocator("FLINK_NUM_CFU")

        resolution = _make_resolution(
            resource_active=make_identity_set("sa-A", "sa-B"),
            stmt_owner_cfu={"sa-A": 2.0, "sa-B": 1.0},
        )
        ctx = _make_ctx_with_resolution(flink_billing_line, resolution, Decimal("90"))

        result = allocator(ctx)

        assert isinstance(result, AllocationResult)
        assert sum(r.amount for r in result.rows) == Decimal("90")
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)

    def test_handler_cfus_allocator_is_same_flink_model(self, flink_billing_line: BillingLineItem) -> None:
        """FLINK_NUM_CFUS alternate spelling also returns FLINK_MODEL."""
        from plugins.confluent_cloud.allocation_models import FLINK_MODEL
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        allocator_cfu = handler.get_allocator("FLINK_NUM_CFU")
        allocator_cfus = handler.get_allocator("FLINK_NUM_CFUS")

        assert allocator_cfu is FLINK_MODEL
        assert allocator_cfus is FLINK_MODEL
