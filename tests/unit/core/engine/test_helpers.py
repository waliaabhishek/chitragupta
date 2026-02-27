from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.helpers import (
    allocate_by_usage_ratio,
    allocate_evenly,
    allocate_hybrid,
    allocate_to_owner,
    allocate_to_resource,
    compute_active_fraction,
    make_row,
    split_amount_evenly,
)
from core.models import CostType, Resource, ResourceStatus
from core.models.chargeback import AllocationDetail

from .conftest import make_ctx

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
_DAY = timedelta(days=1)


# --- split_amount_evenly ---


class TestSplitAmountEvenly:
    def test_count_zero(self) -> None:
        assert split_amount_evenly(Decimal("10.00"), 0) == []

    def test_count_one(self) -> None:
        assert split_amount_evenly(Decimal("10.00"), 1) == [Decimal("10.00")]

    def test_even_division(self) -> None:
        result = split_amount_evenly(Decimal("10.00"), 2)
        assert result == [Decimal("5.0000"), Decimal("5.0000")]

    def test_uneven_division_sum_preserved(self) -> None:
        result = split_amount_evenly(Decimal("10.00"), 3)
        assert sum(result) == Decimal("10.00")
        assert len(result) == 3

    def test_total_zero(self) -> None:
        result = split_amount_evenly(Decimal("0"), 3)
        assert result == [Decimal("0.0000")] * 3
        assert sum(result) == Decimal(0)

    def test_negative_total(self) -> None:
        result = split_amount_evenly(Decimal("-9.00"), 3)
        assert sum(result) == Decimal("-9.00")
        assert len(result) == 3

    def test_large_count_sum_preserved(self) -> None:
        result = split_amount_evenly(Decimal("10.00"), 100)
        assert sum(result) == Decimal("10.00")
        assert len(result) == 100


# --- allocate_by_usage_ratio ---


class TestAllocateByUsageRatio:
    def test_two_identities(self) -> None:
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 60.0, "u-2": 40.0})
        assert len(result.rows) == 2
        amounts = {r.identity_id: r.amount for r in result.rows}
        assert amounts["u-1"] == Decimal("6.0000")
        assert amounts["u-2"] == Decimal("4.0000")

    def test_single_identity_gets_all(self) -> None:
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 100.0})
        assert len(result.rows) == 1
        assert result.rows[0].amount == Decimal("10.0000")

    def test_empty_dict_fallback_to_unallocated(self) -> None:
        ctx = make_ctx(split_amount=Decimal("5.00"))
        result = allocate_by_usage_ratio(ctx, {})
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_USAGE_FOR_ACTIVE_IDENTITIES

    def test_all_zero_values_fallback_to_unallocated(self) -> None:
        ctx = make_ctx()
        result = allocate_by_usage_ratio(ctx, {"u-1": 0.0, "u-2": 0.0})
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].cost_type == CostType.SHARED

    def test_sum_preservation(self) -> None:
        ctx = make_ctx(split_amount=Decimal("100.00"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 33.0, "u-2": 33.0, "u-3": 34.0})
        assert sum(r.amount for r in result.rows) == Decimal("100.00")

    def test_remainder_distribution(self) -> None:
        """Equal ratios with non-evenly-divisible amount forces remainder loop."""
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 1.0, "u-2": 1.0, "u-3": 1.0})
        assert len(result.rows) == 3
        assert sum(r.amount for r in result.rows) == Decimal("10.00")
        # Each gets ~3.3333; remainder distributed to leading recipients
        amounts = [r.amount for r in result.rows]
        assert all(a > Decimal(0) for a in amounts)

    def test_cost_type_is_usage(self) -> None:
        ctx = make_ctx()
        result = allocate_by_usage_ratio(ctx, {"u-1": 1.0})
        assert all(r.cost_type == CostType.USAGE for r in result.rows)

    def test_allocation_method(self) -> None:
        ctx = make_ctx()
        result = allocate_by_usage_ratio(ctx, {"u-1": 1.0})
        assert all(r.allocation_method == "usage_ratio" for r in result.rows)

    def test_allocation_detail_on_success(self) -> None:
        ctx = make_ctx()
        result = allocate_by_usage_ratio(ctx, {"u-1": 1.0})
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result.rows)


# --- allocate_evenly ---


class TestAllocateEvenly:
    def test_two_identities(self) -> None:
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_evenly(ctx, ["u-1", "u-2"])
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_three_identities_uneven(self) -> None:
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_evenly(ctx, ["u-1", "u-2", "u-3"])
        assert sum(r.amount for r in result.rows) == Decimal("10.00")
        assert len(result.rows) == 3

    def test_empty_list_fallback_to_unallocated(self) -> None:
        ctx = make_ctx(split_amount=Decimal("5.00"))
        result = allocate_evenly(ctx, [])
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].cost_type == CostType.SHARED
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED

    def test_single_identity_full_amount(self) -> None:
        ctx = make_ctx(split_amount=Decimal("7.50"))
        result = allocate_evenly(ctx, ["u-1"])
        assert result.rows[0].amount == Decimal("7.50")

    def test_cost_type_is_shared(self) -> None:
        ctx = make_ctx()
        result = allocate_evenly(ctx, ["u-1", "u-2"])
        assert all(r.cost_type == CostType.SHARED for r in result.rows)

    def test_allocation_method(self) -> None:
        ctx = make_ctx()
        result = allocate_evenly(ctx, ["u-1"])
        assert result.rows[0].allocation_method == "even_split"

    def test_allocation_detail_on_success(self) -> None:
        ctx = make_ctx()
        result = allocate_evenly(ctx, ["u-1", "u-2"])
        assert all(r.allocation_detail == AllocationDetail.EVEN_SPLIT_ALLOCATION for r in result.rows)


# --- allocate_hybrid ---


def _passthrough_usage(ctx: AllocationContext) -> AllocationResult:
    return AllocationResult(rows=[make_row(ctx, "u-1", CostType.USAGE, ctx.split_amount, "test")])


def _passthrough_shared(ctx: AllocationContext) -> AllocationResult:
    return AllocationResult(rows=[make_row(ctx, "u-1", CostType.SHARED, ctx.split_amount, "test")])


def _empty_allocator(ctx: AllocationContext) -> AllocationResult:
    return AllocationResult()


class TestAllocateHybrid:
    def test_70_30_split(self) -> None:
        ctx = make_ctx(split_amount=Decimal("100.00"))
        result = allocate_hybrid(
            ctx,
            usage_ratio=0.7,
            shared_ratio=0.3,
            usage_fn=_passthrough_usage,
            shared_fn=_passthrough_shared,
        )
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100.00")
        cost_types = {r.cost_type for r in result.rows}
        assert CostType.USAGE in cost_types
        assert CostType.SHARED in cost_types

    def test_50_50_split(self) -> None:
        ctx = make_ctx(split_amount=Decimal("10.00"))
        result = allocate_hybrid(
            ctx,
            usage_ratio=0.5,
            shared_ratio=0.5,
            usage_fn=_passthrough_usage,
            shared_fn=_passthrough_shared,
        )
        amounts = [r.amount for r in result.rows]
        assert amounts[0] == Decimal("5.0000")
        assert amounts[1] == Decimal("5.0000")

    def test_ratio_validation(self) -> None:
        ctx = make_ctx()
        with pytest.raises(ValueError, match="must sum to 1.0"):
            allocate_hybrid(
                ctx,
                usage_ratio=0.6,
                shared_ratio=0.5,
                usage_fn=_empty_allocator,
                shared_fn=_empty_allocator,
            )

    def test_sub_allocator_empty_rows(self) -> None:
        ctx = make_ctx()
        result = allocate_hybrid(
            ctx,
            usage_ratio=0.7,
            shared_ratio=0.3,
            usage_fn=_empty_allocator,
            shared_fn=_empty_allocator,
        )
        assert result.rows == []


# --- allocate_to_owner ---


class TestAllocateToOwner:
    def test_valid_owner(self) -> None:
        ctx = make_ctx(split_amount=Decimal("25.00"))
        result = allocate_to_owner(ctx, "u-owner")
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "u-owner"
        assert result.rows[0].cost_type == CostType.USAGE
        assert result.rows[0].amount == Decimal("25.00")
        assert result.rows[0].allocation_method == "direct_owner"

    def test_empty_string_raises(self) -> None:
        ctx = make_ctx()
        with pytest.raises(ValueError, match="must not be empty"):
            allocate_to_owner(ctx, "")


# --- allocate_to_resource ---


class TestAllocateToResource:
    def test_produces_single_row(self) -> None:
        ctx = make_ctx(split_amount=Decimal("15.00"))
        result = allocate_to_resource(ctx)
        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.identity_id == "lkc-abc123"
        assert row.cost_type == CostType.SHARED
        assert row.allocation_method == "to_resource"
        assert row.amount == Decimal("15.00")


# --- compute_active_fraction ---


class TestComputeActiveFraction:
    def test_fully_active(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW + _DAY)
        assert frac == Decimal(1)

    def test_created_mid_window(self) -> None:
        start = _NOW
        end = _NOW + _DAY
        mid = _NOW + _DAY / 2
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=mid,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, start, end)
        assert frac == Decimal("0.5")

    def test_deleted_mid_window(self) -> None:
        start = _NOW
        end = _NOW + _DAY
        mid = _NOW + _DAY / 2
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=start,
            deleted_at=mid,
            status=ResourceStatus.DELETED,
        )
        frac = compute_active_fraction(r, start, end)
        assert frac == Decimal("0.5")

    def test_created_after_window(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW + 2 * _DAY,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW + _DAY)
        assert frac == Decimal(0)

    def test_deleted_before_window(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW - 5 * _DAY,
            deleted_at=_NOW - _DAY,
            status=ResourceStatus.DELETED,
        )
        frac = compute_active_fraction(r, _NOW, _NOW + _DAY)
        assert frac == Decimal(0)

    def test_created_at_none(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=None,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW + _DAY)
        assert frac == Decimal(1)

    def test_deleted_at_none(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW,
            deleted_at=None,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW + _DAY)
        assert frac == Decimal(1)

    def test_zero_length_window(self) -> None:
        r = Resource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW)
        assert frac == Decimal(1)
