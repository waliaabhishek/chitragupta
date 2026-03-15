from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.orchestrator import _aggregate_rows
from core.models.chargeback import AllocationDetail, ChargebackRow, CostType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)


def _row(
    *,
    product_type: str = "kafka",
    amount: str = "10.00",
    timestamp: datetime = _BASE_TS,
    identity_id: str = "u-1",
    allocation_method: str | None = "even_split",
    allocation_detail: str | None = None,
    resource_id: str | None = "lkc-abc",
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="confluent",
        tenant_id="t-001",
        timestamp=timestamp,
        resource_id=resource_id,
        product_category="kafka",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=Decimal(amount),
        allocation_method=allocation_method,
        allocation_detail=allocation_detail,
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


class TestAggregateRowsSingleRow:
    def test_single_row_amount_preserved(self) -> None:
        row = _row(amount="7.50")
        result = _aggregate_rows([row], "daily")
        assert len(result) == 1
        assert result[0].amount == Decimal("7.50")

    def test_single_row_allocation_detail_is_none(self) -> None:
        row = _row(amount="7.50")
        result = _aggregate_rows([row], "daily")
        assert result[0].allocation_detail is None


class TestAggregateRowsSameKey:
    def test_three_same_key_rows_sum(self) -> None:
        rows = [
            _row(amount="10.00"),
            _row(amount="20.00"),
            _row(amount="30.00"),
        ]
        result = _aggregate_rows(rows, "daily")
        assert len(result) == 1
        assert result[0].amount == Decimal("60.00")

    def test_allocation_method_from_first_row(self) -> None:
        rows = [
            _row(amount="10.00", allocation_method="first_method"),
            _row(amount="20.00", allocation_method="second_method"),
            _row(amount="30.00", allocation_method="third_method"),
        ]
        result = _aggregate_rows(rows, "daily")
        assert result[0].allocation_method == "first_method"


class TestAggregateRowsDistinctKeys:
    def test_distinct_product_types_produce_separate_rows(self) -> None:
        rows = [
            _row(product_type="kafka", amount="10.00"),
            _row(product_type="connect", amount="20.00"),
            _row(product_type="schema_registry", amount="30.00"),
        ]
        result = _aggregate_rows(rows, "daily")
        assert len(result) == 3

    def test_distinct_keys_amounts_not_mixed(self) -> None:
        rows = [
            _row(product_type="kafka", amount="10.00"),
            _row(product_type="connect", amount="20.00"),
        ]
        result = _aggregate_rows(rows, "daily")
        amounts = {r.product_type: r.amount for r in result}
        assert amounts["kafka"] == Decimal("10.00")
        assert amounts["connect"] == Decimal("20.00")


class TestAggregateRowsDailyTruncation:
    def test_same_date_different_hours_collapse_to_one_row(self) -> None:
        ts_morning = datetime(2024, 6, 15, 8, 0, 0, tzinfo=UTC)
        ts_noon = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        ts_evening = datetime(2024, 6, 15, 22, 59, 59, tzinfo=UTC)
        rows = [_row(timestamp=ts_morning), _row(timestamp=ts_noon), _row(timestamp=ts_evening)]
        result = _aggregate_rows(rows, "daily")
        assert len(result) == 1

    def test_daily_timestamp_is_midnight_utc(self) -> None:
        ts = datetime(2024, 6, 15, 14, 35, 0, tzinfo=UTC)
        result = _aggregate_rows([_row(timestamp=ts)], "daily")
        assert result[0].timestamp == datetime(2024, 6, 15, 0, 0, 0, tzinfo=UTC)


class TestAggregateRowsMonthlyTruncation:
    def test_same_month_different_days_collapse_to_one_row(self) -> None:
        ts_day5 = datetime(2024, 6, 5, 0, 0, 0, tzinfo=UTC)
        ts_day20 = datetime(2024, 6, 20, 0, 0, 0, tzinfo=UTC)
        rows = [_row(timestamp=ts_day5), _row(timestamp=ts_day20)]
        result = _aggregate_rows(rows, "monthly")
        assert len(result) == 1

    def test_monthly_timestamp_is_first_of_month_midnight_utc(self) -> None:
        ts = datetime(2024, 6, 20, 14, 35, 0, tzinfo=UTC)
        result = _aggregate_rows([_row(timestamp=ts)], "monthly")
        assert result[0].timestamp == datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)


class TestAggregateRowsEmptyInput:
    def test_empty_input_returns_empty_list(self) -> None:
        result = _aggregate_rows([], "daily")
        assert result == []

    def test_empty_input_monthly_returns_empty_list(self) -> None:
        result = _aggregate_rows([], "monthly")
        assert result == []


class TestAggregateRowsAllocationDetailDropped:
    def test_non_none_allocation_detail_becomes_none(self) -> None:
        rows = [
            _row(allocation_detail=AllocationDetail.EVEN_SPLIT_ALLOCATION),
            _row(allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION),
        ]
        result = _aggregate_rows(rows, "daily")
        assert all(r.allocation_detail is None for r in result)

    def test_string_allocation_detail_becomes_none(self) -> None:
        row = _row(allocation_detail="some_custom_detail")
        result = _aggregate_rows([row], "daily")
        assert result[0].allocation_detail is None


class TestAggregateBucketDataclass:
    """Structural test: implementation must use a _Bucket dataclass."""

    def test_bucket_dataclass_exists_in_orchestrator_module(self) -> None:
        import dataclasses

        import core.engine.orchestrator as mod

        assert hasattr(mod, "_Bucket"), "_Bucket dataclass not found in orchestrator module"
        assert dataclasses.is_dataclass(mod._Bucket), "_Bucket is not a dataclass"

    def test_bucket_has_total_and_template_fields(self) -> None:
        import dataclasses

        import core.engine.orchestrator as mod

        field_names = {f.name for f in dataclasses.fields(mod._Bucket)}
        assert "total" in field_names
        assert "template" in field_names
