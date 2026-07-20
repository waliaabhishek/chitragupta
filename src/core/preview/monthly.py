from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from decimal import Decimal, localcontext

from core.preview.mapping import (
    CUSTOM_EVIDENCE_COLUMNS,
    CUSTOM_EVIDENCE_RULES,
    FOCUS_1_4_COLUMN_RULES,
    FOCUS_1_4_FULL_COLUMNS,
    PREVIEW_DECIMAL_CONTEXT,
    PreviewCell,
    PreviewFinancialProjection,
    PreviewFullRow,
    PreviewLineageMember,
    PreviewMappingError,
    preview_canonical_json,
    preview_serialize_cell,
    preview_utc_text,
    validate_preview_row,
)
from core.preview.models import canonical_next_month_boundary

logger = logging.getLogger(__name__)


class PreviewMonthlyAggregationError(PreviewMappingError):
    """Persisted Full rows cannot be aggregated under the Monthly contract."""


MONTHLY_SUM_COLUMNS = (
    "BilledCost",
    "ContractedCost",
    "EffectiveCost",
    "ListCost",
    "PricingCurrencyEffectiveCost",
    "PricingQuantity",
    "ConsumedQuantity",
    "x_ConfluentDiscountAmount",
)
MONTHLY_REPLACED_COLUMNS = (
    "ChargePeriodStart",
    "ChargePeriodEnd",
    "x_ChitraguptaSourceCostId",
)
_ALL_COLUMNS = (*FOCUS_1_4_FULL_COLUMNS, *CUSTOM_EVIDENCE_COLUMNS)
_GROUP_COLUMNS = tuple(
    column for column in _ALL_COLUMNS if column not in {*MONTHLY_SUM_COLUMNS, *MONTHLY_REPLACED_COLUMNS}
)


def _lineage_key(member: PreviewLineageMember) -> tuple[object, ...]:
    return (
        member.origin_timestamp.astimezone(UTC),
        member.origin_environment_id,
        member.origin_resource_id,
        member.origin_product_type,
        member.origin_product_category,
        member.calculation_id,
        member.portion_ordinal,
        member.source_cost_id,
    )


def _source_identity(members: tuple[PreviewLineageMember, ...]) -> str:
    if len(members) == 1:
        return members[0].source_cost_id
    payload = {
        "schema_version": "v1",
        "members": [
            {
                "source_cost_id": member.source_cost_id,
                "calculation_id": member.calculation_id,
                "origin_timestamp": preview_utc_text(member.origin_timestamp),
                "origin_environment_id": member.origin_environment_id,
                "origin_resource_id": member.origin_resource_id,
                "origin_product_type": member.origin_product_type,
                "origin_product_category": member.origin_product_category,
                "portion_ordinal": member.portion_ordinal,
            }
            for member in members
        ],
    }
    digest = hashlib.sha256(preview_canonical_json(payload).encode()).hexdigest()
    return f"chitragupta:confluent-cloud:source-cost-set:v1:{digest}"


def _sum_quantity(values: list[PreviewCell], column: str) -> Decimal | None:
    present = [value for value in values if value is not None]
    if present and len(present) != len(values):
        raise PreviewMonthlyAggregationError(f"{column} cannot mix null and non-null quantity values")
    if not present:
        return None
    if not all(isinstance(value, Decimal) for value in present):
        raise PreviewMonthlyAggregationError(f"{column} must contain Decimal quantity values")
    return sum((value for value in present if isinstance(value, Decimal)), Decimal(0))


def _decimal_cell(values: dict[str, PreviewCell], column: str) -> Decimal:
    value = values[column]
    if not isinstance(value, Decimal):
        raise PreviewMonthlyAggregationError(f"{column} must contain a Decimal value")
    return value


def _optional_decimal_cell(values: dict[str, PreviewCell], column: str) -> Decimal | None:
    value = values[column]
    if value is not None and not isinstance(value, Decimal):
        raise PreviewMonthlyAggregationError(f"{column} must contain a Decimal or null value")
    return value


def _optional_text_cell(values: dict[str, PreviewCell], column: str) -> str | None:
    value = values[column]
    if value is not None and not isinstance(value, str):
        raise PreviewMonthlyAggregationError(f"{column} must contain text or null")
    return value


def aggregate_monthly_full_rows(
    *,
    rows: Iterable[PreviewFullRow],
    month_start: datetime,
    month_end: datetime,
) -> tuple[PreviewFullRow, ...]:
    if (
        month_start.tzinfo is None
        or month_start.utcoffset() is None
        or month_end.tzinfo is None
        or month_end.utcoffset() is None
    ):
        raise PreviewMonthlyAggregationError("monthly bounds must be timezone-aware")
    month_start = month_start.astimezone(UTC)
    month_end = month_end.astimezone(UTC)
    try:
        expected_month_end = canonical_next_month_boundary(month_start.date())
    except ValueError as exc:
        raise PreviewMonthlyAggregationError("monthly bounds must cover one exact UTC calendar month") from exc
    if (
        month_start.time() != datetime.min.time()
        or month_end.time() != datetime.min.time()
        or month_end.date() != expected_month_end
    ):
        raise PreviewMonthlyAggregationError("monthly bounds must cover one exact UTC calendar month")

    grouped: dict[tuple[PreviewCell, ...], list[PreviewFullRow]] = defaultdict(list)
    for row in rows:
        if len(row.target_values) != len(FOCUS_1_4_FULL_COLUMNS) or len(row.custom_values) != len(
            CUSTOM_EVIDENCE_COLUMNS
        ):
            raise PreviewMonthlyAggregationError("monthly row has an invalid Full-column count")
        values = dict(zip(_ALL_COLUMNS, (*row.target_values, *row.custom_values), strict=True))
        grouped[tuple(values[column] for column in _GROUP_COLUMNS)].append(row)

    result: list[PreviewFullRow] = []
    for group_rows in grouped.values():
        values_by_row = [
            dict(zip(_ALL_COLUMNS, (*row.target_values, *row.custom_values), strict=True)) for row in group_rows
        ]
        output = dict(values_by_row[0])
        with localcontext(PREVIEW_DECIMAL_CONTEXT):
            for column in (
                "BilledCost",
                "ContractedCost",
                "EffectiveCost",
                "ListCost",
                "PricingCurrencyEffectiveCost",
                "x_ConfluentDiscountAmount",
            ):
                cells = [values[column] for values in values_by_row]
                if not all(isinstance(value, Decimal) for value in cells):
                    raise PreviewMonthlyAggregationError(f"{column} must contain Decimal values")
                output[column] = sum(
                    (value for value in cells if isinstance(value, Decimal)),
                    Decimal(0),
                )
            output["PricingQuantity"] = _sum_quantity(
                [values["PricingQuantity"] for values in values_by_row], "PricingQuantity"
            )
            output["ConsumedQuantity"] = _sum_quantity(
                [values["ConsumedQuantity"] for values in values_by_row], "ConsumedQuantity"
            )
        output["BillingPeriodStart"] = month_start
        output["BillingPeriodEnd"] = month_end
        output["ChargePeriodStart"] = month_start
        output["ChargePeriodEnd"] = month_end
        members = tuple(sorted((member for row in group_rows for member in row.lineage_members), key=_lineage_key))
        if not members:
            raise PreviewMonthlyAggregationError("monthly row requires allocation lineage members")
        output["x_ChitraguptaSourceCostId"] = _source_identity(members)
        financials = PreviewFinancialProjection(
            billed_cost=_decimal_cell(output, "BilledCost"),
            contracted_cost=_decimal_cell(output, "ContractedCost"),
            effective_cost=_decimal_cell(output, "EffectiveCost"),
            list_cost=_decimal_cell(output, "ListCost"),
            list_unit_price=_optional_decimal_cell(output, "ListUnitPrice"),
            pricing_currency_effective_cost=_decimal_cell(output, "PricingCurrencyEffectiveCost"),
            pricing_currency_list_unit_price=_optional_decimal_cell(output, "PricingCurrencyListUnitPrice"),
            pricing_quantity=_optional_decimal_cell(output, "PricingQuantity"),
            pricing_unit=_optional_text_cell(output, "PricingUnit"),
            consumed_quantity=_optional_decimal_cell(output, "ConsumedQuantity"),
            consumed_unit=_optional_text_cell(output, "ConsumedUnit"),
        )
        aggregate = PreviewFullRow(
            target_values=tuple(output[column] for column in FOCUS_1_4_FULL_COLUMNS),
            custom_values=tuple(output[column] for column in CUSTOM_EVIDENCE_COLUMNS),
            financials=financials,
            lineage_members=members,
        )
        validate_preview_row(
            row=aggregate,
            target_rules=FOCUS_1_4_COLUMN_RULES,
            custom_rules=CUSTOM_EVIDENCE_RULES,
        )
        result.append(aggregate)
    result.sort(
        key=lambda row: tuple(preview_serialize_cell(value) for value in (*row.target_values, *row.custom_values))
    )
    return tuple(result)
