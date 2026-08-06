from __future__ import annotations

import hashlib
import json
import logging
import pickle
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING

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

if TYPE_CHECKING:
    from core.preview.spooling import PreviewGenerationWorkspace
from core.preview.spooling import SQLITE_BATCH_SIZE

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
            contracted_unit_price=_optional_decimal_cell(output, "ContractedUnitPrice"),
            effective_cost=_decimal_cell(output, "EffectiveCost"),
            list_cost=_decimal_cell(output, "ListCost"),
            list_unit_price=_optional_decimal_cell(output, "ListUnitPrice"),
            pricing_currency_contracted_unit_price=_optional_decimal_cell(output, "PricingCurrencyContractedUnitPrice"),
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


def _validated_month_bounds(month_start: datetime, month_end: datetime) -> tuple[datetime, datetime]:
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
    return month_start, month_end


def _lineage_identity_from_cursor(
    rows: Iterable[tuple[object, ...]],
    *,
    member_count: int,
) -> str:
    iterator = iter(rows)
    if member_count == 1:
        first = next(iterator)
        return str(first[0])
    digest = hashlib.sha256()
    digest.update(b'{"members":[')
    for index, row in enumerate(iterator):
        (
            source_cost_id,
            calculation_id,
            origin_timestamp,
            origin_environment_id,
            origin_resource_id,
            origin_product_type,
            origin_product_category,
            portion_ordinal,
        ) = row
        if index:
            digest.update(b",")
        member = {
            "source_cost_id": source_cost_id,
            "calculation_id": calculation_id,
            "origin_timestamp": origin_timestamp,
            "origin_environment_id": origin_environment_id,
            "origin_resource_id": origin_resource_id,
            "origin_product_type": origin_product_type,
            "origin_product_category": origin_product_category,
            "portion_ordinal": portion_ordinal,
        }
        digest.update(preview_canonical_json(member).encode())
    digest.update(b'],"schema_version":"v1"}')
    return f"chitragupta:confluent-cloud:source-cost-set:v1:{digest.hexdigest()}"


def aggregate_monthly_full_rows_bounded(
    *,
    rows: Iterable[PreviewFullRow],
    month_start: datetime,
    month_end: datetime,
    workspace: PreviewGenerationWorkspace,
) -> Iterator[PreviewFullRow]:
    """Externally group monthly rows while retaining only one group in memory."""

    month_start, month_end = _validated_month_bounds(month_start, month_end)
    database_path = workspace.root / "monthly.sqlite"
    with workspace.sqlite_connection(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE monthly_rows (
                group_key TEXT NOT NULL,
                encounter_ordinal INTEGER NOT NULL,
                row_payload BLOB NOT NULL,
                PRIMARY KEY (group_key, encounter_ordinal)
            );
            CREATE TABLE monthly_lineage (
                group_key TEXT NOT NULL,
                origin_timestamp TEXT NOT NULL,
                origin_environment_id TEXT NOT NULL,
                origin_resource_id TEXT NOT NULL,
                origin_product_type TEXT NOT NULL,
                origin_product_category TEXT NOT NULL,
                calculation_id TEXT NOT NULL,
                portion_ordinal INTEGER NOT NULL,
                source_cost_id TEXT NOT NULL
            );
            CREATE INDEX ix_monthly_lineage_order ON monthly_lineage (
                group_key,
                origin_timestamp,
                origin_environment_id,
                origin_resource_id,
                origin_product_type,
                origin_product_category,
                calculation_id,
                portion_ordinal,
                source_cost_id
            );
            """
        )
        pending_rows = 0
        for ordinal, row in enumerate(rows):
            if len(row.target_values) != len(FOCUS_1_4_FULL_COLUMNS) or len(row.custom_values) != len(
                CUSTOM_EVIDENCE_COLUMNS
            ):
                raise PreviewMonthlyAggregationError("monthly row has an invalid Full-column count")
            values = dict(zip(_ALL_COLUMNS, (*row.target_values, *row.custom_values), strict=True))
            group_key = json.dumps(
                [preview_serialize_cell(values[column]) for column in _GROUP_COLUMNS],
                separators=(",", ":"),
            )
            row_payload = pickle.dumps(row, protocol=pickle.HIGHEST_PROTOCOL)
            workspace.preflight_write(len(row_payload) + len(group_key.encode()))
            connection.execute(
                "INSERT INTO monthly_rows (group_key, encounter_ordinal, row_payload) VALUES (?, ?, ?)",
                (group_key, ordinal, row_payload),
            )
            connection.executemany(
                """
                INSERT INTO monthly_lineage (
                    group_key, origin_timestamp, origin_environment_id, origin_resource_id,
                    origin_product_type, origin_product_category, calculation_id,
                    portion_ordinal, source_cost_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        group_key,
                        preview_utc_text(member.origin_timestamp),
                        member.origin_environment_id,
                        member.origin_resource_id,
                        member.origin_product_type,
                        member.origin_product_category,
                        member.calculation_id,
                        member.portion_ordinal,
                        member.source_cost_id,
                    )
                    for member in row.lineage_members
                ),
            )
            pending_rows += 1
            if pending_rows == SQLITE_BATCH_SIZE:
                connection.commit()
                workspace.enforce_limit()
                pending_rows = 0
        if pending_rows:
            connection.commit()
            workspace.enforce_limit()

        group_keys = connection.execute("SELECT DISTINCT group_key FROM monthly_rows ORDER BY group_key")
        for (group_key_value,) in group_keys:
            group_key = str(group_key_value)
            group_rows = connection.execute(
                """
                SELECT row_payload
                FROM monthly_rows
                WHERE group_key = ?
                ORDER BY encounter_ordinal
                """,
                (group_key,),
            )
            first_row: PreviewFullRow | None = None
            output: dict[str, PreviewCell] | None = None
            sums = {
                column: Decimal(0)
                for column in MONTHLY_SUM_COLUMNS
                if column not in {"PricingQuantity", "ConsumedQuantity"}
            }
            quantity_sums = {"PricingQuantity": Decimal(0), "ConsumedQuantity": Decimal(0)}
            quantity_presence = {"PricingQuantity": False, "ConsumedQuantity": False}
            quantity_null = {"PricingQuantity": False, "ConsumedQuantity": False}
            with localcontext(PREVIEW_DECIMAL_CONTEXT):
                for (payload,) in group_rows:
                    row = pickle.loads(bytes(payload))
                    if not isinstance(row, PreviewFullRow):
                        raise PreviewMonthlyAggregationError("monthly spool contains an invalid row")
                    if first_row is None:
                        first_row = row
                        output = dict(zip(_ALL_COLUMNS, (*row.target_values, *row.custom_values), strict=True))
                    values = dict(zip(_ALL_COLUMNS, (*row.target_values, *row.custom_values), strict=True))
                    for column in sums:
                        value = values[column]
                        if not isinstance(value, Decimal):
                            raise PreviewMonthlyAggregationError(f"{column} must contain Decimal values")
                        sums[column] += value
                    for column in quantity_sums:
                        value = values[column]
                        if value is None:
                            quantity_null[column] = True
                        elif isinstance(value, Decimal):
                            quantity_presence[column] = True
                            quantity_sums[column] += value
                        else:
                            raise PreviewMonthlyAggregationError(f"{column} must contain Decimal quantity values")
            assert first_row is not None
            assert output is not None
            for column, value in sums.items():
                output[column] = value
            for column in quantity_sums:
                if quantity_presence[column] and quantity_null[column]:
                    raise PreviewMonthlyAggregationError(f"{column} cannot mix null and non-null quantity values")
                output[column] = quantity_sums[column] if quantity_presence[column] else None
            output["BillingPeriodStart"] = month_start
            output["BillingPeriodEnd"] = month_end
            output["ChargePeriodStart"] = month_start
            output["ChargePeriodEnd"] = month_end
            count_row = connection.execute(
                "SELECT COUNT(*) FROM monthly_lineage WHERE group_key = ?",
                (group_key,),
            ).fetchone()
            assert count_row is not None
            member_count = int(count_row[0])
            if member_count == 0:
                raise PreviewMonthlyAggregationError("monthly row requires allocation lineage members")
            lineage = connection.execute(
                """
                SELECT source_cost_id, calculation_id, origin_timestamp, origin_environment_id,
                       origin_resource_id, origin_product_type, origin_product_category, portion_ordinal
                FROM monthly_lineage
                WHERE group_key = ?
                ORDER BY origin_timestamp, origin_environment_id, origin_resource_id,
                         origin_product_type, origin_product_category, calculation_id,
                         portion_ordinal, source_cost_id
                """,
                (group_key,),
            )
            output["x_ChitraguptaSourceCostId"] = _lineage_identity_from_cursor(lineage, member_count=member_count)
            financials = PreviewFinancialProjection(
                billed_cost=_decimal_cell(output, "BilledCost"),
                contracted_cost=_decimal_cell(output, "ContractedCost"),
                contracted_unit_price=_optional_decimal_cell(output, "ContractedUnitPrice"),
                effective_cost=_decimal_cell(output, "EffectiveCost"),
                list_cost=_decimal_cell(output, "ListCost"),
                list_unit_price=_optional_decimal_cell(output, "ListUnitPrice"),
                pricing_currency_contracted_unit_price=_optional_decimal_cell(
                    output, "PricingCurrencyContractedUnitPrice"
                ),
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
                lineage_members=(),
            )
            validate_preview_row(
                row=aggregate,
                target_rules=FOCUS_1_4_COLUMN_RULES,
                custom_rules=CUSTOM_EVIDENCE_RULES,
            )
            yield aggregate
    database_path.unlink(missing_ok=True)
    workspace.enforce_limit()
