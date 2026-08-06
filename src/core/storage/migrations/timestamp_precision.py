from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Connection, inspect, text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimestampPlan:
    table: str
    timestamp_columns: tuple[str, ...]
    natural_key: tuple[str, ...] = ()
    json_column: str | None = None
    json_kind: str | None = None


PLANS = (
    TimestampPlan(
        "billing",
        ("timestamp",),
        ("ecosystem", "tenant_id", "timestamp", "resource_id", "product_type", "product_category"),
    ),
    TimestampPlan(
        "ccloud_billing",
        ("timestamp",),
        (
            "ecosystem",
            "tenant_id",
            "timestamp",
            "env_id",
            "resource_id",
            "product_type",
            "product_category",
        ),
    ),
    TimestampPlan("chargeback_facts", ("timestamp",), ("timestamp", "dimension_id")),
    TimestampPlan("topic_attribution_facts", ("timestamp",), ("timestamp", "dimension_id")),
    TimestampPlan(
        "ccloud_cost_source_records",
        (
            "source_period_start",
            "source_period_end",
            "collection_window_start",
            "collection_window_end",
            "evidence_scope_start",
            "evidence_scope_end",
            "allocation_timestamp",
            "retention_timestamp",
            "billing_timestamp",
        ),
        ("ecosystem", "tenant_id", "source_record_id", "evidence_scope_start", "evidence_scope_end"),
    ),
    TimestampPlan(
        "ccloud_source_capture_readiness",
        ("window_start", "window_end", "captured_at"),
        ("ecosystem", "tenant_id", "window_start", "window_end"),
    ),
    TimestampPlan(
        "ccloud_source_capture_readiness_history",
        ("window_start", "window_end", "captured_at"),
        ("ecosystem", "tenant_id", "attempt_sequence", "window_start", "window_end"),
    ),
    TimestampPlan(
        "ccloud_allocation_lineage_portions",
        ("origin_timestamp",),
        (
            "ecosystem",
            "tenant_id",
            "tracking_date",
            "calculation_id",
            "origin_timestamp",
            "origin_env_id",
            "origin_resource_id",
            "origin_product_type",
            "origin_product_category",
            "portion_ordinal",
        ),
    ),
    TimestampPlan("ccloud_source_evidence_attempts", ("refresh_start", "refresh_end", "started_at", "completed_at")),
    TimestampPlan("pipeline_state", ("calculation_completed_at",)),
    TimestampPlan("ccloud_allocation_lineage_runs", ("calculation_completed_at",)),
    TimestampPlan(
        "preview_requests",
        (
            "created_at",
            "started_at",
            "completed_at",
            "expires_at",
            "lease_expires_at",
            "calculation_timestamp",
            "source_through",
        ),
        json_column="calculation_coverage_json",
        json_kind="coverage",
    ),
    TimestampPlan(
        "preview_revisions",
        ("published_at", "retention_pending_at"),
        json_column="source_snapshot_json",
        json_kind="snapshot",
    ),
    TimestampPlan("ccloud_focus_preview_repairs", ("created_at", "started_at", "completed_at")),
    TimestampPlan(
        "ccloud_focus_preview_repair_dates",
        ("started_at", "completed_at", "calculation_completed_at"),
    ),
    TimestampPlan("ccloud_organization_authority_attempts", ("started_at", "completed_at")),
)

OPTIONAL_PREVIEW_EVIDENCE_TABLES = frozenset(
    {
        "ccloud_cost_source_records",
        "ccloud_source_evidence_attempts",
        "ccloud_source_capture_readiness",
        "ccloud_source_capture_readiness_history",
        "ccloud_allocation_lineage_runs",
        "ccloud_allocation_lineage_portions",
        "ccloud_focus_preview_repairs",
        "ccloud_focus_preview_repair_dates",
        "ccloud_organization_authority_attempts",
    }
)


@dataclass(frozen=True)
class JsonUpdate:
    table: str
    physical_id: Any
    column: str
    value: str


@dataclass(frozen=True)
class PreflightResult:
    plans: tuple[TimestampPlan, ...]
    json_updates: tuple[JsonUpdate, ...]
    lineage_parents: tuple[tuple[Any, Any, Any, Any], ...]


def canonicalize_persisted_timestamps(connection: Connection) -> None:
    """Preflight every present plan, then canonicalize all rows."""
    result = _preflight(connection)
    json_updates_by_table: dict[str, list[JsonUpdate]] = {}
    for update in result.json_updates:
        json_updates_by_table.setdefault(update.table, []).append(update)
    for plan in result.plans:
        if plan.natural_key:
            _delete_identical_duplicates(connection, plan)
        for update in json_updates_by_table.get(plan.table, ()):
            _update_physical_row(
                connection,
                update.table,
                update.physical_id,
                {update.column: update.value},
            )
        _canonicalize_scalar_columns(connection, plan)
    _recompute_lineage_counts(connection, set(result.lineage_parents))


def downgrade_sqlite_timestamp_format(connection: Connection) -> None:
    """Restore revision-028's SQLite scalar DateTime representation."""
    if connection.dialect.name != "sqlite":
        return
    result = _preflight(connection)
    for plan in result.plans:
        if plan.natural_key:
            _delete_identical_duplicates(connection, plan)
        assignments = ", ".join(
            f"\"{column}\" = strftime('%Y-%m-%d %H:%M:%S', \"{column}\") || '.000000'"
            for column in plan.timestamp_columns
        )
        nullable_guard = " OR ".join(f'"{column}" IS NOT NULL' for column in plan.timestamp_columns)
        connection.execute(
            text(f'UPDATE "{plan.table}" SET {assignments} WHERE {nullable_guard}')  # noqa: S608
        )
    _recompute_lineage_counts(connection, set(result.lineage_parents))


def _preflight(connection: Connection) -> PreflightResult:
    inspector = inspect(connection)
    existing = set(inspector.get_table_names())
    plans: list[TimestampPlan] = []
    json_updates: list[JsonUpdate] = []

    for plan in PLANS:
        if plan.table not in existing:
            continue
        columns = tuple(column["name"] for column in inspector.get_columns(plan.table))
        if not _plan_columns_are_compatible(plan, columns):
            if plan.table in OPTIONAL_PREVIEW_EVIDENCE_TABLES:
                continue
            _raise_missing_plan_columns(plan, columns)
        _preflight_scalar_values(connection, plan)
        if plan.natural_key:
            _preflight_collisions(connection, plan, columns)
        if plan.json_column is not None:
            json_updates.extend(_preflight_json(connection, plan))
        plans.append(plan)

    lineage_parents = _preflight_lineage_parents(connection, plans)
    return PreflightResult(
        plans=tuple(plans),
        json_updates=tuple(json_updates),
        lineage_parents=tuple(lineage_parents),
    )


def _plan_columns_are_compatible(plan: TimestampPlan, columns: tuple[str, ...]) -> bool:
    expected = set(plan.timestamp_columns) | set(plan.natural_key)
    if plan.json_column is not None:
        expected.add(plan.json_column)
    return expected <= set(columns)


def _raise_missing_plan_columns(plan: TimestampPlan, columns: tuple[str, ...]) -> None:
    expected = set(plan.timestamp_columns) | set(plan.natural_key)
    if plan.json_column is not None:
        expected.add(plan.json_column)
    missing = sorted(expected - set(columns))
    joined = ", ".join(missing)
    raise RuntimeError(f"timestamp canonicalization cannot inspect {plan.table}; missing columns: {joined}")


def _preflight_scalar_values(connection: Connection, plan: TimestampPlan) -> None:
    if connection.dialect.name == "postgresql":
        return
    if connection.dialect.name != "sqlite":
        raise RuntimeError(f"timestamp canonicalization does not support dialect {connection.dialect.name!r}")
    for column in plan.timestamp_columns:
        invalid = connection.execute(
            text(
                f'SELECT rowid FROM "{plan.table}" '  # noqa: S608
                f'WHERE "{column}" IS NOT NULL AND {_canonical_expression(connection, column)} IS NULL LIMIT 1'
            )
        ).scalar_one_or_none()
        if invalid is not None:
            raise RuntimeError(
                f"invalid timestamp in {plan.table}.{column} at physical row {invalid}; "
                "repair the value and retry migration 029"
            )


def _preflight_collisions(
    connection: Connection,
    plan: TimestampPlan,
    columns: tuple[str, ...],
) -> None:
    canonical_columns = ", ".join(
        f"{_canonical_expression(connection, column) if column in plan.timestamp_columns else _q(column)} "
        f"AS {_q(column)}"
        for column in columns
    )
    group_columns = ", ".join(
        _canonical_expression(connection, column) if column in plan.timestamp_columns else _q(column)
        for column in columns
    )
    key_columns = ", ".join(_q(column) for column in plan.natural_key)
    conflict = (
        connection.execute(
            text(
                f"""
            SELECT {key_columns}, SUM(__row_count) AS duplicate_count
            FROM (
                SELECT {canonical_columns}, COUNT(*) AS __row_count
                FROM {_q(plan.table)}
                GROUP BY {group_columns}
            ) AS canonical_payloads
            GROUP BY {key_columns}
            HAVING SUM(__row_count) > 1 AND COUNT(*) > 1
            LIMIT 1
            """  # noqa: S608
            )
        )
        .mappings()
        .one_or_none()
    )
    if conflict is None:
        return
    key = tuple(conflict[column] for column in plan.natural_key)
    raise RuntimeError(_conflict_message(plan, key, int(conflict["duplicate_count"])))


def _preflight_json(connection: Connection, plan: TimestampPlan) -> list[JsonUpdate]:
    assert plan.json_column is not None
    locator = "rowid" if connection.dialect.name == "sqlite" else "ctid::text"
    rows = connection.execute(
        text(
            f"SELECT {locator} AS __physical_id, {_q(plan.json_column)} "  # noqa: S608
            f"FROM {_q(plan.table)} WHERE {_q(plan.json_column)} IS NOT NULL"
        )
    ).mappings()
    updates: list[JsonUpdate] = []
    for row in rows:
        canonical = _canonical_json(
            row[plan.json_column],
            table=plan.table,
            column=plan.json_column,
            kind=plan.json_kind,
            physical_id=row["__physical_id"],
        )
        if canonical != row[plan.json_column]:
            updates.append(
                JsonUpdate(
                    table=plan.table,
                    physical_id=row["__physical_id"],
                    column=plan.json_column,
                    value=canonical,
                )
            )
    return updates


def _canonical_json(
    raw: Any,
    *,
    table: str,
    column: str,
    kind: str | None,
    physical_id: Any,
) -> str:
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
        if kind == "coverage":
            _canonicalize_coverage(value, path=column)
        elif kind == "snapshot":
            if not isinstance(value, dict):
                raise ValueError("expected an object")
            _canonicalize_json_leaf(value, "calculation_timestamp", f"{column}.calculation_timestamp")
            _canonicalize_json_leaf(value, "source_through", f"{column}.source_through")
            coverage = value.get("calculation_coverage")
            if coverage is not None:
                _canonicalize_coverage(coverage, path=f"{column}.calculation_coverage")
        else:
            raise ValueError("unknown persisted JSON timestamp plan")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"invalid timestamp JSON in {table}.{column} at physical row {physical_id}; "
            "repair the value and retry migration 029"
        ) from exc
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonicalize_coverage(value: Any, *, path: str) -> None:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list")
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{path}[{index}] must be an object")
        _canonicalize_json_leaf(
            item,
            "calculation_completed_at",
            f"{path}[{index}].calculation_completed_at",
        )


def _canonicalize_json_leaf(value: dict[str, Any], key: str, path: str) -> None:
    if key not in value or value[key] is None:
        return
    raw = value[key]
    if not isinstance(raw, str):
        raise ValueError(f"{path} must be a string")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{path} must be timezone-aware")
    value[key] = parsed.astimezone(UTC).replace(microsecond=0).isoformat(timespec="seconds")


def _preflight_lineage_parents(
    connection: Connection,
    plans: list[TimestampPlan],
) -> set[tuple[Any, Any, Any, Any]]:
    present = {plan.table for plan in plans}
    if "ccloud_allocation_lineage_portions" not in present:
        return set()
    timestamp = _canonical_expression(connection, "origin_timestamp")
    rows = connection.execute(
        text(
            f"""
            SELECT ecosystem, tenant_id, tracking_date, calculation_id
            FROM ccloud_allocation_lineage_portions
            GROUP BY ecosystem, tenant_id, tracking_date, calculation_id,
                     {timestamp}, origin_env_id, origin_resource_id,
                     origin_product_type, origin_product_category, portion_ordinal
            HAVING COUNT(*) > 1
            """  # noqa: S608
        )
    ).all()
    affected = {tuple(row) for row in rows}
    if not affected:
        return set()
    if "ccloud_allocation_lineage_runs" not in present:
        raise RuntimeError(
            "unsafe timestamp canonicalization in ccloud_allocation_lineage_portions: "
            "ccloud_allocation_lineage_runs is missing"
        )
    for ecosystem, tenant_id, tracking_date, calculation_id in affected:
        count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM ccloud_allocation_lineage_runs
                WHERE ecosystem = :ecosystem
                  AND tenant_id = :tenant_id
                  AND tracking_date = :tracking_date
                  AND calculation_id = :calculation_id
                  AND capture_status = 'complete'
                """
            ),
            {
                "ecosystem": ecosystem,
                "tenant_id": tenant_id,
                "tracking_date": tracking_date,
                "calculation_id": calculation_id,
            },
        ).scalar_one()
        if count != 1:
            raise RuntimeError(
                "unsafe timestamp canonicalization in ccloud_allocation_lineage_portions: "
                f"parent ecosystem={ecosystem}, tenant_id={tenant_id}, tracking_date={tracking_date}, "
                f"calculation_id={calculation_id} is missing, mismatched, or not complete"
            )
    return affected


def _delete_identical_duplicates(connection: Connection, plan: TimestampPlan) -> None:
    partition = ", ".join(
        _canonical_expression(connection, column) if column in plan.timestamp_columns else _q(column)
        for column in plan.natural_key
    )
    if connection.dialect.name == "sqlite":
        physical = "rowid"
        delete_physical = "rowid"
    else:
        physical = "ctid"
        delete_physical = "ctid"
    connection.execute(
        text(
            f"""
            DELETE FROM {_q(plan.table)}
            WHERE {delete_physical} IN (
                SELECT __physical_id
                FROM (
                    SELECT {physical} AS __physical_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY {partition}
                               ORDER BY {physical}
                           ) AS __duplicate_ordinal
                    FROM {_q(plan.table)}
                ) AS canonical_duplicates
                WHERE __duplicate_ordinal > 1
            )
            """  # noqa: S608
        )
    )


def _canonicalize_scalar_columns(connection: Connection, plan: TimestampPlan) -> None:
    assignments = ", ".join(
        f"{_q(column)} = {_canonical_expression(connection, column)}" for column in plan.timestamp_columns
    )
    guard = " OR ".join(
        f"{_q(column)} IS NOT NULL AND {_q(column)} <> {_canonical_expression(connection, column)}"
        for column in plan.timestamp_columns
    )
    connection.execute(
        text(f"UPDATE {_q(plan.table)} SET {assignments} WHERE {guard}")  # noqa: S608
    )


def _canonical_expression(connection: Connection, column: str) -> str:
    if connection.dialect.name == "sqlite":
        return f"strftime('%Y-%m-%d %H:%M:%S', {_q(column)})"
    if connection.dialect.name == "postgresql":
        return f"date_trunc('second', {_q(column)})"
    raise RuntimeError(f"timestamp canonicalization does not support dialect {connection.dialect.name!r}")


def _conflict_message(plan: TimestampPlan, key: tuple[Any, ...], count: int) -> str:
    rendered = ", ".join(f"{column}={value}" for column, value in zip(plan.natural_key, key, strict=True))
    return (
        f"timestamp canonicalization conflict in {plan.table}: canonical natural key {rendered}; "
        f"{count} rows have different payloads; resolve the duplicate rows and retry migration 029"
    )


def _update_physical_row(
    connection: Connection,
    table: str,
    physical_id: Any,
    updates: dict[str, Any],
) -> None:
    assignments = ", ".join(f'"{column}" = :value_{index}' for index, column in enumerate(updates))
    parameters = {f"value_{index}": value for index, value in enumerate(updates.values())}
    parameters["physical_id"] = physical_id
    clause = "rowid = :physical_id" if connection.dialect.name == "sqlite" else "ctid = CAST(:physical_id AS tid)"
    connection.execute(
        text(f'UPDATE "{table}" SET {assignments} WHERE {clause}'),  # noqa: S608
        parameters,
    )


def _recompute_lineage_counts(
    connection: Connection,
    parents: set[tuple[Any, Any, Any, Any]],
) -> None:
    for ecosystem, tenant_id, tracking_date, calculation_id in parents:
        parameters = {
            "ecosystem": ecosystem,
            "tenant_id": tenant_id,
            "tracking_date": tracking_date,
            "calculation_id": calculation_id,
        }
        count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM ccloud_allocation_lineage_portions
                WHERE ecosystem = :ecosystem
                  AND tenant_id = :tenant_id
                  AND tracking_date = :tracking_date
                  AND calculation_id = :calculation_id
                """
            ),
            parameters,
        ).scalar_one()
        connection.execute(
            text(
                """
                UPDATE ccloud_allocation_lineage_runs
                SET portion_count = :portion_count
                WHERE ecosystem = :ecosystem
                  AND tenant_id = :tenant_id
                  AND tracking_date = :tracking_date
                  AND calculation_id = :calculation_id
                  AND capture_status = 'complete'
                """
            ),
            parameters | {"portion_count": count},
        )


def _q(identifier: str) -> str:
    return f'"{identifier}"'
