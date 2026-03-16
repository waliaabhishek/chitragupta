from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.helpers import (
    _distribute_remainder,
    allocate_by_usage_ratio,
    allocate_evenly,
    allocate_hybrid,
    compute_active_fraction,
    make_row,
    split_amount_evenly,
)
from core.models import CoreResource, CostType, ResourceStatus
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

    def test_non_quantized_input_no_crash(self) -> None:
        # Decimal("1.00005") has 5 decimal places — remainder index must not overflow
        result = split_amount_evenly(Decimal("1.00005"), 2)
        assert len(result) == 2

    def test_non_quantized_input_sum_quantized(self) -> None:
        # Sum must equal the 4-decimal quantized value of the input
        result = split_amount_evenly(Decimal("1.00005"), 2)
        assert sum(result) == Decimal("1.0001")

    def test_extreme_precision_no_crash(self) -> None:
        # 9 decimal places — remainder could exceed count without modulo guard
        result = split_amount_evenly(Decimal("1.000000001"), 2)
        assert len(result) == 2

    def test_large_remainder_wraps(self) -> None:
        # remainder > count*CENT: modulo wraparound must still distribute correctly
        result = split_amount_evenly(Decimal("0.00009"), 2)
        assert len(result) == 2
        assert sum(result) == Decimal("0.0001")


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

    def test_non_quantized_split_amount_no_crash(self) -> None:
        # split_amount with >4 decimal places must not raise IndexError
        ctx = make_ctx(split_amount=Decimal("1.00005"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 60.0, "u-2": 40.0})
        assert len(result.rows) == 2

    def test_non_quantized_split_amount_sum_preserved(self) -> None:
        # Sum of allocated rows must equal quantized(split_amount)
        ctx = make_ctx(split_amount=Decimal("1.00005"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 60.0, "u-2": 40.0})
        assert sum(r.amount for r in result.rows) == Decimal("1.0001")


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
        assert all(r.allocation_detail is None for r in result.rows)


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


# --- compute_active_fraction ---


class TestComputeActiveFraction:
    def test_fully_active(self) -> None:
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
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
        r = CoreResource(
            ecosystem="c",
            tenant_id="t",
            resource_id="r",
            resource_type="x",
            created_at=_NOW,
            status=ResourceStatus.ACTIVE,
        )
        frac = compute_active_fraction(r, _NOW, _NOW)
        assert frac == Decimal(1)


# --- _distribute_remainder ---


class TestDistributeRemainder:
    def test_safety_guard_non_convergent_diff_raises(self) -> None:
        """diff=0.00005 is not a multiple of _CENT (0.0001); loop cannot converge → RuntimeError."""
        with pytest.raises(RuntimeError, match="did not converge"):
            _distribute_remainder([Decimal("1.00")], Decimal("0.00005"))


# --- new split_amount_evenly cases ---


class TestSplitAmountEvenlyExtended:
    def test_uneven_first_recipient_gets_extra_cent(self) -> None:
        """10.00 / 3 = 3.3333 each; extra 0.0001 goes to first recipient."""
        result = split_amount_evenly(Decimal("10.00"), 3)
        assert len(result) == 3
        assert sum(result) == Decimal("10.00")
        # First recipient gets the extra cent
        assert result[0] > result[1]
        assert result[0] > result[2]

    def test_large_count_near_boundary(self) -> None:
        """0.0001 / 999 = 0.0000 each with diff 0.0001; only first recipient gets 0.0001."""
        result = split_amount_evenly(Decimal("0.0001"), 999)
        assert len(result) == 999
        assert sum(result) == Decimal("0.0001")
        assert result[0] == Decimal("0.0001")
        assert all(r == Decimal("0.0000") for r in result[1:])

    def test_zero_diff_no_remainder_loop_needed(self) -> None:
        """3.00 / 3 = 1.0000 each; diff is zero, distribute returns immediately."""
        result = split_amount_evenly(Decimal("3.00"), 3)
        assert result == [Decimal("1.0000"), Decimal("1.0000"), Decimal("1.0000")]
        assert sum(result) == Decimal("3.00")


# --- allocate_by_usage_ratio sum preservation (uneven ratios) ---


class TestAllocateByUsageRatioSumPreservationUneven:
    def test_three_identities_uneven_ratios_sum_preserved(self) -> None:
        """Uneven ratios (e.g. 1:2:7) must still sum exactly to split_amount."""
        ctx = make_ctx(split_amount=Decimal("1.00"))
        result = allocate_by_usage_ratio(ctx, {"u-1": 10.0, "u-2": 20.0, "u-3": 70.0})
        assert sum(r.amount for r in result.rows) == Decimal("1.00")
