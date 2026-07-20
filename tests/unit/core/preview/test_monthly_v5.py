from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal, getcontext, localcontext
from importlib import import_module
from typing import Any

import pytest


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _monthly() -> Any:
    return import_module("core.preview.monthly")


def _lineage(*, source_id: str = "cost-1", day: int = 1, ordinal: int = 0) -> Any:
    mapping = _mapping()
    return mapping.PreviewLineageMember(
        source_cost_id=source_id,
        calculation_id=f"calculation-{day}",
        origin_timestamp=datetime(2026, 7, day, tzinfo=UTC),
        origin_environment_id="env-1",
        origin_resource_id="lkc-1",
        origin_product_type="KAFKA_STORAGE",
        origin_product_category="KAFKA",
        portion_ordinal=ordinal,
    )


def _row(
    *,
    day: int = 1,
    billed: str = "8",
    contracted: str = "10",
    effective: str = "8",
    list_cost: str = "10",
    pricing_cost: str = "8",
    pricing_quantity: str | None = "5",
    consumed_quantity: str | None = "5",
    discount: str = "2",
    lineage: tuple[Any, ...] | None = None,
    **dimensions: object,
) -> Any:
    mapping = _mapping()
    target = {column: None for column in mapping.FOCUS_1_4_FULL_COLUMNS}
    custom = {column: None for column in mapping.CUSTOM_EVIDENCE_COLUMNS}
    target.update(
        {
            "BilledCost": Decimal(billed),
            "ContractedCost": Decimal(contracted),
            "EffectiveCost": Decimal(effective),
            "ListCost": Decimal(list_cost),
            "PricingCurrencyEffectiveCost": Decimal(pricing_cost),
            "PricingQuantity": None if pricing_quantity is None else Decimal(pricing_quantity),
            "ConsumedQuantity": None if consumed_quantity is None else Decimal(consumed_quantity),
            "ListUnitPrice": Decimal("2"),
            "PricingCurrencyListUnitPrice": Decimal("2"),
            "PricingUnit": "GB",
            "ConsumedUnit": "GB",
            "ChargePeriodStart": datetime(2026, 7, day, tzinfo=UTC),
            "ChargePeriodEnd": datetime(2026, 7, day + 1, tzinfo=UTC),
            "BillingPeriodStart": datetime(2026, 7, 1, tzinfo=UTC),
            "BillingPeriodEnd": datetime(2026, 8, 1, tzinfo=UTC),
        }
    )
    custom.update(
        {
            "x_ConfluentDiscountAmount": Decimal(discount),
            "x_ChitraguptaSourceCostId": f"cost-{day}",
            "x_ChitraguptaAllocationRatio": Decimal("0.5"),
            "x_ChitraguptaAllocationMethodVersion": "v1",
        }
    )
    for name, value in dimensions.items():
        (target if name in target else custom)[name] = value
    financials = mapping.PreviewFinancialProjection(
        billed_cost=Decimal(billed),
        contracted_cost=Decimal(contracted),
        effective_cost=Decimal(effective),
        list_cost=Decimal(list_cost),
        list_unit_price=Decimal("2"),
        pricing_currency_effective_cost=Decimal(pricing_cost),
        pricing_currency_list_unit_price=Decimal("2"),
        pricing_quantity=None if pricing_quantity is None else Decimal(pricing_quantity),
        pricing_unit="GB",
        consumed_quantity=None if consumed_quantity is None else Decimal(consumed_quantity),
        consumed_unit="GB",
    )
    return mapping.PreviewFullRow(
        target_values=tuple(target[column] for column in mapping.FOCUS_1_4_FULL_COLUMNS),
        custom_values=tuple(custom[column] for column in mapping.CUSTOM_EVIDENCE_COLUMNS),
        financials=financials,
        lineage_members=lineage or (_lineage(source_id=f"cost-{day}", day=day),),
    )


def _values(row: Any) -> dict[str, object]:
    mapping = _mapping()
    return dict(
        zip(
            (*mapping.FOCUS_1_4_FULL_COLUMNS, *mapping.CUSTOM_EVIDENCE_COLUMNS),
            (*row.target_values, *row.custom_values),
            strict=True,
        )
    )


def _context_state() -> tuple[object, ...]:
    context = getcontext()
    return (
        context.prec,
        context.rounding,
        context.Emin,
        context.Emax,
        context.capitals,
        context.clamp,
        tuple(context.flags.items()),
        tuple(context.traps.items()),
    )


def test_monthly_aggregation_sums_exact_cost_quantity_and_signed_discount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)
    before = _context_state()
    with localcontext() as ambient:
        ambient.prec = 5
        rows = monthly.aggregate_monthly_full_rows(
            rows=(
                _row(day=1, billed="8.123456789", discount="2.25"),
                _row(
                    day=2,
                    billed="-1.123456789",
                    contracted="-2",
                    effective="-1",
                    list_cost="-2",
                    pricing_cost="-1",
                    discount="-3.5",
                ),
            ),
            month_start=datetime(2026, 7, 1, tzinfo=UTC),
            month_end=datetime(2026, 8, 1, tzinfo=UTC),
        )

    assert _context_state() == before
    assert len(rows) == 1
    values = _values(rows[0])
    assert values["BilledCost"] == Decimal("7.000000000")
    assert values["ContractedCost"] == Decimal("8")
    assert values["EffectiveCost"] == Decimal("7")
    assert values["ListCost"] == Decimal("8")
    assert values["PricingCurrencyEffectiveCost"] == Decimal("7")
    assert values["PricingQuantity"] == Decimal("10")
    assert values["ConsumedQuantity"] == Decimal("10")
    assert values["x_ConfluentDiscountAmount"] == Decimal("-1.25")
    assert rows[0].financials.billed_cost == values["BilledCost"]
    assert rows[0].financials.pricing_quantity == values["PricingQuantity"]
    assert values["ChargePeriodStart"] == datetime(2026, 7, 1, tzinfo=UTC)
    assert values["ChargePeriodEnd"] == datetime(2026, 8, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("column", "first", "second"),
    [
        ("AllocatedResourceId", "sa-1", "sa-2"),
        ("AllocatedMethodId", "even_split", "direct"),
        ("AllocatedMethodDetails", '{"target_kind":"identity"}', '{"target_kind":"resource"}'),
        ("x_ChitraguptaAllocationRatio", Decimal("0.5"), Decimal("0.25")),
        ("x_ChitraguptaAllocationMethodVersion", "v1", "v2"),
        ("PricingUnit", "GB", "TB"),
        ("ListUnitPrice", Decimal("2"), Decimal("3")),
        ("Tags", '{"team":"a"}', '{"team":"b"}'),
        ("ChargeCategory", "Usage", "Credit"),
        ("ResourceId", "lkc-1", "lkc-2"),
        ("SkuId", "sku-a", "sku-b"),
        ("x_ConfluentTierDimensions", '{"tier":"a"}', '{"tier":"b"}'),
    ],
)
def test_monthly_aggregation_preserves_every_nonadditive_dimension(
    monkeypatch: pytest.MonkeyPatch,
    column: str,
    first: object,
    second: object,
) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)

    rows = monthly.aggregate_monthly_full_rows(
        rows=(_row(day=1, **{column: first}), _row(day=2, **{column: second})),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )

    assert len(rows) == 2
    assert {_values(row)[column] for row in rows} == {first, second}


@pytest.mark.parametrize("quantity_column", ["PricingQuantity", "ConsumedQuantity"])
def test_monthly_aggregation_rejects_mixed_null_quantity(
    monkeypatch: pytest.MonkeyPatch,
    quantity_column: str,
) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)
    first = _row(day=1)
    second = _row(
        day=2,
        pricing_quantity=None if quantity_column == "PricingQuantity" else "5",
        consumed_quantity=None if quantity_column == "ConsumedQuantity" else "5",
    )

    with pytest.raises(monthly.PreviewMonthlyAggregationError, match="quantity|Quantity"):
        monthly.aggregate_monthly_full_rows(
            rows=(first, second),
            month_start=datetime(2026, 7, 1, tzinfo=UTC),
            month_end=datetime(2026, 8, 1, tzinfo=UTC),
        )


def test_monthly_aggregation_failure_is_a_typed_mapping_diagnostic() -> None:
    monthly = _monthly()
    mapping = _mapping()

    with pytest.raises(monthly.PreviewMonthlyAggregationError) as exc_info:
        monthly.aggregate_monthly_full_rows(
            rows=(_row(day=1, pricing_quantity=None), _row(day=2, pricing_quantity="5")),
            month_start=datetime(2026, 7, 1, tzinfo=UTC),
            month_end=datetime(2026, 8, 1, tzinfo=UTC),
        )

    assert isinstance(exc_info.value, mapping.PreviewMappingError)


def test_monthly_composite_identity_is_order_independent_and_contains_complete_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)
    first = _row(day=1, lineage=(_lineage(source_id="cost-b", day=2),))
    second = _row(day=2, lineage=(_lineage(source_id="cost-a", day=1),))

    forward = monthly.aggregate_monthly_full_rows(
        rows=(first, second),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )[0]
    reverse = monthly.aggregate_monthly_full_rows(
        rows=(second, first),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )[0]

    source_id = _values(forward)["x_ChitraguptaSourceCostId"]
    assert source_id == _values(reverse)["x_ChitraguptaSourceCostId"]
    assert isinstance(source_id, str)
    assert tuple(member.source_cost_id for member in forward.lineage_members) == ("cost-a", "cost-b")
    payload = {
        "schema_version": "v1",
        "members": [
            {
                "source_cost_id": member.source_cost_id,
                "calculation_id": member.calculation_id,
                "origin_timestamp": _mapping().preview_utc_text(member.origin_timestamp),
                "origin_environment_id": member.origin_environment_id,
                "origin_resource_id": member.origin_resource_id,
                "origin_product_type": member.origin_product_type,
                "origin_product_category": member.origin_product_category,
                "portion_ordinal": member.portion_ordinal,
            }
            for member in forward.lineage_members
        ],
    }
    digest = hashlib.sha256(_mapping().preview_canonical_json(payload).encode()).hexdigest()
    assert source_id == f"chitragupta:confluent-cloud:source-cost-set:v1:{digest}"


def test_single_lineage_member_retains_native_source_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)

    aggregate = monthly.aggregate_monthly_full_rows(
        rows=(_row(day=1),),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )[0]

    assert _values(aggregate)["x_ChitraguptaSourceCostId"] == "cost-1"


def test_monthly_rows_are_sorted_by_canonical_full_values_not_input_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monthly = _monthly()
    monkeypatch.setattr(monthly, "validate_preview_row", lambda **_kwargs: None)

    rows = monthly.aggregate_monthly_full_rows(
        rows=(
            _row(day=2, AllocatedResourceId="sa-b"),
            _row(day=1, AllocatedResourceId="sa-a"),
        ),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )

    assert [_values(row)["AllocatedResourceId"] for row in rows] == ["sa-a", "sa-b"]


def test_monthly_aggregation_calls_common_row_validator_after_rebuilding_financials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monthly = _monthly()
    validated: list[Any] = []

    def validate(**kwargs: object) -> None:
        validated.append(kwargs["row"])

    monkeypatch.setattr(monthly, "validate_preview_row", validate)
    result = monthly.aggregate_monthly_full_rows(
        rows=(_row(day=1), _row(day=2)),
        month_start=datetime(2026, 7, 1, tzinfo=UTC),
        month_end=datetime(2026, 8, 1, tzinfo=UTC),
    )

    assert validated == list(result)


def test_monthly_module_has_no_allocation_builder_or_ratio_division() -> None:
    source = __import__("inspect").getsource(_monthly())

    assert "build_allocation_lineage_capture" not in source
    assert "AllocationContext" not in source
    assert "allocation_ratio =" not in source
