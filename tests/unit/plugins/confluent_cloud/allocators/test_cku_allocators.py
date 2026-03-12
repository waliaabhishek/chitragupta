"""Tests for CKU composition model and allocator.

Tests for CKU_USAGE_CHAIN, CKU_SHARED_CHAIN, _CKU_DYNAMIC_MODEL, and
kafka_cku_allocator — written before implementation (TDD red phase).
"""

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
    MetricRow,
)
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cku_billing_line() -> BillingLineItem:
    """Standard CKU billing line."""
    return CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NUM_CKU",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )


def make_identity_set(*identity_ids: str, identity_type: str = "service_account") -> IdentitySet:
    """Create an IdentitySet with the given identity IDs."""
    iset = IdentitySet()
    for identity_id in identity_ids:
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=identity_id,
                identity_type=identity_type,
            )
        )
    return iset


def make_resolution(
    resource_active: IdentitySet | None = None,
    metrics_derived: IdentitySet | None = None,
    tenant_period: IdentitySet | None = None,
    api_key_to_owner: dict[str, str] | None = None,
) -> IdentityResolution:
    """Build IdentityResolution with optional context."""
    context: dict[str, object] = {}
    if api_key_to_owner:
        context["api_key_to_owner"] = api_key_to_owner
    return IdentityResolution(
        resource_active=resource_active or IdentitySet(),
        metrics_derived=metrics_derived or IdentitySet(),
        tenant_period=tenant_period or IdentitySet(),
        context=context,
    )


def make_ctx(
    billing_line: BillingLineItem,
    resolution: IdentityResolution,
    split_amount: Decimal,
    metrics_data: dict | None = None,
    params: dict | None = None,
) -> AllocationContext:
    """Build AllocationContext."""
    return AllocationContext(
        timeslice=billing_line.timestamp,
        billing_line=billing_line,
        identities=resolution,
        split_amount=split_amount,
        metrics_data=metrics_data,
        params=params or {},
    )


def make_metric_row(value: float, principal_id: str, metric_key: str = "bytes_in") -> MetricRow:
    """Create a MetricRow with principal_id label."""
    return MetricRow(datetime(2026, 2, 1, tzinfo=UTC), metric_key, value, {"principal_id": principal_id})


# ---------------------------------------------------------------------------
# Test 1: Static 70/30 split math
# ---------------------------------------------------------------------------


class TestCkuStaticSplitMath:
    """Static 70/30 default ratio produces correct amount splits."""

    def test_static_70_30_usage_rows_sum_to_70(self, cku_billing_line: BillingLineItem) -> None:
        """Usage component rows sum to 70.00 with default params."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=make_identity_set("sa-1", "sa-2"),
        )
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        usage_rows = [r for r in result.rows if r.metadata.get("composition_index") == 0]
        shared_rows = [r for r in result.rows if r.metadata.get("composition_index") == 1]
        assert sum(r.amount for r in usage_rows) == Decimal("70.00")
        assert sum(r.amount for r in shared_rows) == Decimal("30.00")

    def test_static_70_30_composition_index_and_ratio_on_usage_rows(self, cku_billing_line: BillingLineItem) -> None:
        """All usage rows have composition_index=0, composition_ratio=0.70."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        usage_rows = [r for r in result.rows if r.metadata.get("composition_index") == 0]
        assert all(r.metadata["composition_ratio"] == pytest.approx(0.70) for r in usage_rows)

    def test_static_70_30_composition_index_and_ratio_on_shared_rows(self, cku_billing_line: BillingLineItem) -> None:
        """All shared rows have composition_index=1, composition_ratio=0.30."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        shared_rows = [r for r in result.rows if r.metadata.get("composition_index") == 1]
        assert all(r.metadata["composition_ratio"] == pytest.approx(0.30) for r in shared_rows)


# ---------------------------------------------------------------------------
# Test 2: Dynamic ratio override
# ---------------------------------------------------------------------------


class TestCkuDynamicRatioOverride:
    """ctx.params can override usage/shared ratios."""

    def test_60_40_override_usage_rows_sum_to_60(self, cku_billing_line: BillingLineItem) -> None:
        """Usage rows sum to 60.00 when kafka_cku_usage_ratio=0.60."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        params = {"kafka_cku_usage_ratio": "0.60", "kafka_cku_shared_ratio": "0.40"}
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data, params)

        result = kafka_cku_allocator(ctx)

        usage_rows = [r for r in result.rows if r.metadata.get("composition_index") == 0]
        shared_rows = [r for r in result.rows if r.metadata.get("composition_index") == 1]
        assert sum(r.amount for r in usage_rows) == Decimal("60.00")
        assert sum(r.amount for r in shared_rows) == Decimal("40.00")


# ---------------------------------------------------------------------------
# Test 3: Invalid ratio raises ValueError
# ---------------------------------------------------------------------------


class TestCkuInvalidRatioRaisesValueError:
    """Ratios that don't sum to 1.0 raise ValueError."""

    def test_sum_1_10_raises_value_error(self, cku_billing_line: BillingLineItem) -> None:
        """usage_ratio=0.80 + shared_ratio=0.30 = 1.10 → ValueError."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1"))
        params = {"kafka_cku_usage_ratio": "0.80", "kafka_cku_shared_ratio": "0.30"}
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), params=params)

        with pytest.raises(ValueError):
            kafka_cku_allocator(ctx)


# ---------------------------------------------------------------------------
# Test 4: CKU_USAGE_CHAIN Tier 0 — proportional split by bytes
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier0:
    """CKU_USAGE_CHAIN Tier 0 — usage ratio by combined bytes_in + bytes_out."""

    def test_tier0_300_700_bytes_proportional_split(self, cku_billing_line: BillingLineItem) -> None:
        """sa-1 gets 30%, sa-2 gets 70% of usage slice from 300/700 byte split."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = CKU_USAGE_CHAIN(ctx)

        assert isinstance(result, AllocationResult)
        row_by_id = {r.identity_id: r for r in result.rows}
        assert row_by_id["sa-1"].amount == pytest.approx(Decimal("30.0000"), abs=Decimal("0.01"))
        assert row_by_id["sa-2"].amount == pytest.approx(Decimal("70.0000"), abs=Decimal("0.01"))
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result.rows)

    def test_tier0_cost_type_is_usage(self, cku_billing_line: BillingLineItem) -> None:
        """Tier 0 rows have CostType.USAGE."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1"))
        metrics_data = {"bytes_in": [make_metric_row(500, "sa-1")]}
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = CKU_USAGE_CHAIN(ctx)

        assert all(r.cost_type == CostType.USAGE for r in result.rows)


# ---------------------------------------------------------------------------
# Test 5: CKU_USAGE_CHAIN Tier 0 — combined bytes same principal
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier0CombinedBytes:
    """CKU_USAGE_CHAIN Tier 0 — same principal appears in both bytes_in and bytes_out."""

    def test_combined_bytes_same_principal_50_50_split(self, cku_billing_line: BillingLineItem) -> None:
        """bytes_in sa-1=200, bytes_out sa-1=300, bytes_out sa-2=500 → sa-1=50%, sa-2=50%."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(200, "sa-1")],
            "bytes_out": [
                make_metric_row(300, "sa-1"),
                make_metric_row(500, "sa-2"),
            ],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = CKU_USAGE_CHAIN(ctx)

        row_by_id = {r.identity_id: r for r in result.rows}
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        # sa-1 has 500/1000 = 50%, sa-2 has 500/1000 = 50%
        assert row_by_id["sa-1"].amount == pytest.approx(Decimal("50.0000"), abs=Decimal("0.01"))
        assert row_by_id["sa-2"].amount == pytest.approx(Decimal("50.0000"), abs=Decimal("0.01"))


# ---------------------------------------------------------------------------
# Test 6: CKU_USAGE_CHAIN Tier 0 — API key resolution
# ---------------------------------------------------------------------------


class TestCkuUsageChainApiKeyResolution:
    """CKU_USAGE_CHAIN resolves API keys to owning service accounts."""

    def test_api_key_resolved_to_sa_owner(self, cku_billing_line: BillingLineItem) -> None:
        """principal_id=ak-1 with api_key_to_owner={"ak-1": "sa-10"} → identity_id=sa-10."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(
            resource_active=make_identity_set("sa-10"),
            api_key_to_owner={"ak-1": "sa-10"},
        )
        metrics_data = {"bytes_in": [make_metric_row(500, "ak-1")]}
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = CKU_USAGE_CHAIN(ctx)

        identity_ids = {r.identity_id for r in result.rows}
        assert "sa-10" in identity_ids
        assert "ak-1" not in identity_ids


# ---------------------------------------------------------------------------
# Test 7: CKU_USAGE_CHAIN Tier 1 — no metrics fallback
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier1NoMetrics:
    """CKU_USAGE_CHAIN Tier 1 — no metrics, even split over merged_active."""

    def test_no_metrics_even_split_over_merged_active(self, cku_billing_line: BillingLineItem) -> None:
        """metrics_data={}, merged_active=[sa-1, sa-2] → even split, chain_tier=1."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data={})

        result = CKU_USAGE_CHAIN(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-1", "sa-2"}
        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES for r in result.rows)
        assert all(r.cost_type == CostType.SHARED for r in result.rows)


# ---------------------------------------------------------------------------
# Test 7b: CKU_USAGE_CHAIN Tier 1 — zero usage falls through to merged_active
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier1ZeroUsage:
    """CKU_USAGE_CHAIN Tier 1 — metrics present but all values zero."""

    def test_zero_usage_fallback_to_merged_active(self, cku_billing_line: BillingLineItem) -> None:
        """bytes_in=0 for sa-1 → Tier 1 even split over merged_active=[sa-2]."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-2"))
        metrics_data = {"bytes_in": [make_metric_row(0, "sa-1")]}
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = CKU_USAGE_CHAIN(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-2"
        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES for r in result.rows)
        assert all(r.cost_type == CostType.SHARED for r in result.rows)


# ---------------------------------------------------------------------------
# Test 8: CKU_USAGE_CHAIN Tier 2 — no metrics, no active identities
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier2:
    """CKU_USAGE_CHAIN Tier 2 — no metrics, no merged_active, tenant_period fallback."""

    def test_tenant_period_fallback(self, cku_billing_line: BillingLineItem) -> None:
        """Empty merged_active, tenant_period=[sa-3] → Tier 2 even split."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution(
            tenant_period=make_identity_set("sa-3"),
        )
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data={})

        result = CKU_USAGE_CHAIN(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-3"
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].allocation_detail == AllocationDetail.NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 9: CKU_USAGE_CHAIN Tier 3 — no identities at all
# ---------------------------------------------------------------------------


class TestCkuUsageChainTier3:
    """CKU_USAGE_CHAIN Tier 3 — terminal fallback to resource_id."""

    def test_terminal_to_resource_id(self, cku_billing_line: BillingLineItem) -> None:
        """No metrics, no identities → identity_id=resource_id, chain_tier=3."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        resolution = make_resolution()
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data={})

        result = CKU_USAGE_CHAIN(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "lkc-abc"
        assert result.rows[0].metadata["chain_tier"] == 3
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 10: CKU_SHARED_CHAIN Tier 0 — even split over merged_active
# ---------------------------------------------------------------------------


class TestCkuSharedChainTier0:
    """CKU_SHARED_CHAIN Tier 0 — even split over merged_active, CostType.USAGE."""

    def test_even_split_two_active_identities(self, cku_billing_line: BillingLineItem) -> None:
        """merged_active=[sa-1, sa-2] → even split, chain_tier=0, CostType.USAGE."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"))

        result = CKU_SHARED_CHAIN(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"sa-1", "sa-2"}
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert all(r.allocation_detail is None for r in result.rows)

    def test_tier0_no_allocation_detail(self, cku_billing_line: BillingLineItem) -> None:
        """Tier 0 CKU_SHARED_CHAIN rows have no allocation_detail."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        resolution = make_resolution(resource_active=make_identity_set("sa-1"))
        ctx = make_ctx(cku_billing_line, resolution, Decimal("50.00"))

        result = CKU_SHARED_CHAIN(ctx)

        assert all(r.allocation_detail is None for r in result.rows)


# ---------------------------------------------------------------------------
# Test 11: CKU_SHARED_CHAIN Tier 1 — no merged_active, tenant_period fallback
# ---------------------------------------------------------------------------


class TestCkuSharedChainTier1:
    """CKU_SHARED_CHAIN Tier 1 — empty merged_active, tenant_period owners."""

    def test_tenant_period_fallback_chain_tier_1(self, cku_billing_line: BillingLineItem) -> None:
        """Empty merged_active, tenant_period=[sa-3] → chain_tier=1, NO_ACTIVE_IDENTITIES_LOCATED."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        resolution = make_resolution(tenant_period=make_identity_set("sa-3"))
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"))

        result = CKU_SHARED_CHAIN(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-3"
        assert result.rows[0].metadata["chain_tier"] == 1
        assert result.rows[0].allocation_detail == AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 12: CKU_SHARED_CHAIN Tier 2 — no identities
# ---------------------------------------------------------------------------


class TestCkuSharedChainTier2:
    """CKU_SHARED_CHAIN Tier 2 — terminal fallback to resource_id."""

    def test_terminal_to_resource_id(self, cku_billing_line: BillingLineItem) -> None:
        """No identities → identity_id=resource_id, chain_tier=2, NO_IDENTITIES_LOCATED."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        resolution = make_resolution()
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"))

        result = CKU_SHARED_CHAIN(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "lkc-abc"
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 13: Combined result structure — total rows and total amount
# ---------------------------------------------------------------------------


class TestCkuCombinedResultStructure:
    """kafka_cku_allocator produces usage+shared rows summing to split_amount."""

    def test_total_rows_equals_usage_plus_shared(self, cku_billing_line: BillingLineItem) -> None:
        """Total rows = usage component rows + shared component rows."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        usage_rows = [r for r in result.rows if r.metadata.get("composition_index") == 0]
        shared_rows = [r for r in result.rows if r.metadata.get("composition_index") == 1]
        assert len(result.rows) == len(usage_rows) + len(shared_rows)

    def test_all_rows_sum_to_split_amount(self, cku_billing_line: BillingLineItem) -> None:
        """Sum of all row amounts equals split_amount=100.00."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        assert sum(r.amount for r in result.rows) == Decimal("100.00")

    def test_composition_index_present_in_all_rows(self, cku_billing_line: BillingLineItem) -> None:
        """Every row has composition_index in metadata."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        assert all("composition_index" in r.metadata for r in result.rows)
        assert all("composition_ratio" in r.metadata for r in result.rows)


# ---------------------------------------------------------------------------
# Test 14: Integration — end-to-end CKU allocation
# ---------------------------------------------------------------------------


class TestCkuIntegration:
    """Integration test: kafka_cku_allocator via KafkaHandler produces correct rows."""

    def test_end_to_end_via_handler_produces_composition_metadata(self, cku_billing_line: BillingLineItem) -> None:
        """KafkaHandler.get_allocator('KAFKA_NUM_CKU') returns kafka_cku_allocator with composition metadata."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        allocator = handler.get_allocator("KAFKA_NUM_CKU")

        resolution = make_resolution(resource_active=make_identity_set("sa-1", "sa-2"))
        metrics_data = {
            "bytes_in": [make_metric_row(300, "sa-1")],
            "bytes_out": [make_metric_row(700, "sa-2")],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = allocator(ctx)

        assert isinstance(result, AllocationResult)
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        assert all("composition_index" in r.metadata for r in result.rows)
        assert all("composition_ratio" in r.metadata for r in result.rows)

    def test_kafka_num_ckus_uses_same_allocator(self, cku_billing_line: BillingLineItem) -> None:
        """KAFKA_NUM_CKUS alternate spelling also maps to kafka_cku_allocator."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        allocator_cku = handler.get_allocator("KAFKA_NUM_CKU")
        allocator_ckus = handler.get_allocator("KAFKA_NUM_CKUS")

        assert allocator_cku is kafka_cku_allocator
        assert allocator_ckus is kafka_cku_allocator

    def test_end_to_end_correct_70_30_distribution(self, cku_billing_line: BillingLineItem) -> None:
        """Realistic context: 70% usage / 30% shared, composition_index present in all rows."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_cku_allocator

        resolution = make_resolution(
            resource_active=make_identity_set("sa-1", "sa-2"),
            metrics_derived=make_identity_set("sa-1", "sa-2"),
        )
        metrics_data = {
            "bytes_in": [
                make_metric_row(400, "sa-1"),
                make_metric_row(100, "sa-2"),
            ],
            "bytes_out": [
                make_metric_row(300, "sa-1"),
                make_metric_row(200, "sa-2"),
            ],
        }
        ctx = make_ctx(cku_billing_line, resolution, Decimal("100.00"), metrics_data)

        result = kafka_cku_allocator(ctx)

        assert isinstance(result, AllocationResult)
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        usage_rows = [r for r in result.rows if r.metadata.get("composition_index") == 0]
        shared_rows = [r for r in result.rows if r.metadata.get("composition_index") == 1]
        assert sum(r.amount for r in usage_rows) == Decimal("70.00")
        assert sum(r.amount for r in shared_rows) == Decimal("30.00")


# ---------------------------------------------------------------------------
# Exported constants structure
# ---------------------------------------------------------------------------


class TestCkuModelExports:
    """CKU_USAGE_CHAIN, CKU_SHARED_CHAIN are exported from allocation_models."""

    def test_cku_usage_chain_is_chain_model(self) -> None:
        """CKU_USAGE_CHAIN must be a ChainModel instance."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        assert isinstance(CKU_USAGE_CHAIN, ChainModel)

    def test_cku_shared_chain_is_chain_model(self) -> None:
        """CKU_SHARED_CHAIN must be a ChainModel instance."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        assert isinstance(CKU_SHARED_CHAIN, ChainModel)

    def test_cku_usage_chain_has_four_tiers(self) -> None:
        """CKU_USAGE_CHAIN must have exactly 4 models in the chain."""
        from plugins.confluent_cloud.allocation_models import CKU_USAGE_CHAIN

        assert len(CKU_USAGE_CHAIN.models) == 4

    def test_cku_shared_chain_has_three_tiers(self) -> None:
        """CKU_SHARED_CHAIN must have exactly 3 models in the chain."""
        from plugins.confluent_cloud.allocation_models import CKU_SHARED_CHAIN

        assert len(CKU_SHARED_CHAIN.models) == 3
