from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy import Connection, inspect

from core.preview.storage_availability import PreviewEvidenceSchemaError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn

_SOURCE_TABLE = "ccloud_cost_source_records"
_V21_COLUMNS = (
    "billing_timestamp",
    "billing_env_id",
    "billing_resource_id",
    "billing_product_type",
    "billing_product_category",
)
_V26_COLUMNS = ("capture_id",)


def _source_v18() -> sa.Table:
    metadata = sa.MetaData()
    return sa.Table(
        _SOURCE_TABLE,
        metadata,
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("source_record_id", sa.String(), nullable=False),
        sa.Column("identity_scheme", sa.String(), nullable=False),
        sa.Column("provider_cost_id", sa.String(), nullable=True),
        sa.Column("source_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("collection_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("collection_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_scope_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_scope_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("allocation_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("retention_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("granularity", sa.String(), nullable=True),
        sa.Column("product", sa.String(), nullable=True),
        sa.Column("line_type", sa.String(), nullable=True),
        sa.Column("amount", sa.String(), nullable=True),
        sa.Column("original_amount", sa.String(), nullable=True),
        sa.Column("discount_amount", sa.String(), nullable=True),
        sa.Column("price", sa.String(), nullable=True),
        sa.Column("quantity", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("network_access_type", sa.String(), nullable=True),
        sa.Column("resource_id", sa.String(), nullable=True),
        sa.Column("resource_name", sa.String(), nullable=True),
        sa.Column("environment_id", sa.String(), nullable=True),
        sa.Column("tier_dimensions_json", sa.String(), nullable=False),
        sa.Column("malformed", sa.Boolean(), nullable=False),
        sa.Column("diagnostics_json", sa.String(), nullable=False),
        sa.Column("raw_payload_json", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "source_record_id",
            "evidence_scope_start",
            "evidence_scope_end",
        ),
        sa.Index(
            "ix_ccloud_cost_source_allocation",
            "ecosystem",
            "tenant_id",
            "allocation_timestamp",
        ),
        sa.Index(
            "ix_ccloud_cost_source_retention",
            "ecosystem",
            "tenant_id",
            "retention_timestamp",
        ),
        sa.Index(
            "ix_ccloud_cost_source_undated_scope",
            "ecosystem",
            "tenant_id",
            "source_period_start",
            "evidence_scope_start",
            "evidence_scope_end",
        ),
    )


def _source_for_revision(target_revision: str) -> sa.Table:
    table = _source_v18()
    if target_revision in {"021", "026", "027"}:
        table.append_column(sa.Column("billing_timestamp", sa.DateTime(timezone=True), nullable=True))
        table.append_column(sa.Column("billing_env_id", sa.String(), nullable=True))
        table.append_column(sa.Column("billing_resource_id", sa.String(), nullable=True))
        table.append_column(sa.Column("billing_product_type", sa.String(), nullable=True))
        table.append_column(sa.Column("billing_product_category", sa.String(), nullable=True))
    if target_revision in {"026", "027"}:
        table.append_column(sa.Column("capture_id", sa.String(), nullable=True))
    return table


def _sqlmodel_table(model: type[object]) -> sa.Table:
    table = getattr(model, "__table__", None)
    if not isinstance(table, sa.Table):
        raise TypeError(f"{model.__name__} does not expose a SQLAlchemy table")
    return table


def _expected_tables(target_revision: str) -> tuple[sa.Table, ...]:
    tables = [_source_for_revision(target_revision)]
    if target_revision in {"021", "026", "027"}:
        from plugins.confluent_cloud.storage.preview_tables import (
            CCloudAllocationLineagePortionTable,
            CCloudAllocationLineageRunTable,
        )

        tables.extend(
            (
                _sqlmodel_table(CCloudAllocationLineageRunTable),
                _sqlmodel_table(CCloudAllocationLineagePortionTable),
            )
        )
    if target_revision in {"026", "027"}:
        from plugins.confluent_cloud.storage.preview_tables import (
            CCloudOrganizationAuthorityAttemptTable,
            CCloudSourceCaptureReadinessTable,
            CCloudSourceEvidenceAttemptTable,
        )

        tables.extend(
            (
                _sqlmodel_table(CCloudSourceEvidenceAttemptTable),
                _sqlmodel_table(CCloudSourceCaptureReadinessTable),
                _sqlmodel_table(CCloudOrganizationAuthorityAttemptTable),
            )
        )
    if target_revision == "027":
        from plugins.confluent_cloud.storage.preview_tables import (
            CCloudFocusPreviewRepairDateTable,
            CCloudFocusPreviewRepairTable,
            CCloudSourceCaptureReadinessHistoryTable,
        )

        tables.extend(
            (
                _sqlmodel_table(CCloudSourceCaptureReadinessHistoryTable),
                _sqlmodel_table(CCloudFocusPreviewRepairTable),
                _sqlmodel_table(CCloudFocusPreviewRepairDateTable),
            )
        )
    return tuple(tables)


def _type_family(column_type: sa.types.TypeEngine[Any]) -> type[sa.types.TypeEngine[Any]] | None:
    if isinstance(column_type, sa.types.TypeDecorator):
        column_type = column_type.impl_instance
    for family in (sa.Boolean, sa.DateTime, sa.Date, sa.Integer, sa.String):
        if isinstance(column_type, family):
            return family
    return None


def _columns(connection: Connection, table_name: str) -> dict[str, ReflectedColumn]:
    return {column["name"]: column for column in inspect(connection).get_columns(table_name)}


def _index_shapes(connection: Connection, table_name: str) -> dict[str, tuple[tuple[str, ...], bool]]:
    inspector = inspect(connection)
    shapes: dict[str, tuple[tuple[str, ...], bool]] = {}
    for index in inspector.get_indexes(table_name):
        name = index.get("name")
        column_names = index.get("column_names", [])
        if name is None or any(column_name is None for column_name in column_names):
            continue
        shapes[name] = (
            tuple(column_name for column_name in column_names if column_name is not None),
            bool(index["unique"]),
        )
    for constraint in inspector.get_unique_constraints(table_name):
        name = constraint.get("name")
        if name is not None:
            shapes[name] = (tuple(constraint["column_names"]), True)
    return shapes


def _foreign_key_shapes(
    connection: Connection,
    table_name: str,
) -> set[tuple[tuple[str, ...], str, tuple[str, ...]]]:
    return {
        (
            tuple(foreign_key["constrained_columns"]),
            foreign_key["referred_table"],
            tuple(foreign_key["referred_columns"]),
        )
        for foreign_key in inspect(connection).get_foreign_keys(table_name)
    }


@dataclass(frozen=True)
class _TableRepairPlan:
    table: sa.Table
    create_table: bool
    missing_columns: tuple[sa.Column[Any], ...] = ()
    missing_indexes: tuple[sa.Index, ...] = ()

    def apply(self, connection: Connection) -> None:
        if self.create_table:
            self.table.create(connection)
            return
        _add_nullable_columns(connection, self.table.name, self.missing_columns)
        for index in self.missing_indexes:
            index.create(connection)


def _allowed_missing_columns(table_name: str, target_revision: str) -> frozenset[str]:
    if table_name != _SOURCE_TABLE:
        return frozenset()
    allowed: tuple[str, ...] = ()
    if target_revision in {"021", "026", "027"}:
        allowed += _V21_COLUMNS
    if target_revision in {"026", "027"}:
        allowed += _V26_COLUMNS
    return frozenset(allowed)


def _plan_table_repair(
    connection: Connection,
    table: sa.Table,
    *,
    target_revision: str,
) -> _TableRepairPlan:
    inspector = inspect(connection)
    if table.name not in set(inspector.get_table_names()):
        return _TableRepairPlan(table=table, create_table=True)

    allowed_missing = _allowed_missing_columns(table.name, target_revision)
    actual_columns = _columns(connection, table.name)
    missing_columns: list[sa.Column[Any]] = []
    for expected in table.columns:
        actual = actual_columns.get(expected.name)
        if actual is None:
            if expected.name in allowed_missing:
                missing_columns.append(expected)
                continue
            raise PreviewEvidenceSchemaError(
                f"incompatible {table.name} schema: missing required column {expected.name}"
            )
        expected_family = _type_family(expected.type)
        actual_family = _type_family(actual["type"])
        if expected_family is None or actual_family is not expected_family:
            raise PreviewEvidenceSchemaError(f"incompatible {table.name} schema: column {expected.name} has wrong type")
        if bool(actual["nullable"]) is not bool(expected.nullable):
            raise PreviewEvidenceSchemaError(
                f"incompatible {table.name} schema: column {expected.name} has wrong nullability"
            )

    expected_pk = tuple(column.name for column in table.primary_key.columns)
    actual_pk = tuple(inspector.get_pk_constraint(table.name)["constrained_columns"])
    if actual_pk != expected_pk:
        raise PreviewEvidenceSchemaError(f"incompatible {table.name} schema: wrong primary key")

    actual_indexes = _index_shapes(connection, table.name)
    missing_indexes: list[sa.Index] = []
    for expected_index in sorted(table.indexes, key=lambda index: index.name or ""):
        if expected_index.name is None:
            raise PreviewEvidenceSchemaError(f"incompatible {table.name} schema: unnamed required index")
        expected_shape = (
            tuple(column.name for column in expected_index.columns),
            bool(expected_index.unique),
        )
        actual_shape = actual_indexes.get(expected_index.name)
        if actual_shape is None:
            missing_indexes.append(expected_index)
        elif actual_shape != expected_shape:
            raise PreviewEvidenceSchemaError(
                f"incompatible {table.name} schema: index {expected_index.name} has wrong shape"
            )

    actual_foreign_keys = _foreign_key_shapes(connection, table.name)
    for expected_foreign_key in table.foreign_key_constraints:
        expected_foreign_key_shape = (
            tuple(element.parent.name for element in expected_foreign_key.elements),
            expected_foreign_key.referred_table.name,
            tuple(element.column.name for element in expected_foreign_key.elements),
        )
        if expected_foreign_key_shape not in actual_foreign_keys:
            raise PreviewEvidenceSchemaError(f"incompatible {table.name} schema: missing required foreign key")

    return _TableRepairPlan(
        table=table,
        create_table=False,
        missing_columns=tuple(missing_columns),
        missing_indexes=tuple(missing_indexes),
    )


def _add_nullable_columns(
    connection: Connection,
    table_name: str,
    columns: Iterable[sa.Column[Any]],
) -> None:
    preparer = connection.dialect.identifier_preparer
    for column in columns:
        if not column.nullable:
            raise PreviewEvidenceSchemaError(f"refusing to repair required column {table_name}.{column.name}")
        type_sql = column.type.compile(dialect=connection.dialect)
        connection.exec_driver_sql(
            f"ALTER TABLE {preparer.quote(table_name)} ADD COLUMN {preparer.quote(column.name)} {type_sql}"
        )


class CCloudPreviewSchemaManager:
    def prepare(self, connection: Connection, *, target_revision: str) -> None:
        if target_revision not in {"018", "021", "026", "027"}:
            raise ValueError(f"unknown Preview evidence target revision: {target_revision}")
        plans = tuple(
            _plan_table_repair(connection, table, target_revision=target_revision)
            for table in _expected_tables(target_revision)
        )
        for plan in plans:
            plan.apply(connection)
        if target_revision == "027":
            self._copy_readiness_history(connection)

    @staticmethod
    def _copy_readiness_history(connection: Connection) -> None:
        current = sa.Table(
            "ccloud_source_capture_readiness",
            sa.MetaData(),
            autoload_with=connection,
        )
        history = sa.Table(
            "ccloud_source_capture_readiness_history",
            sa.MetaData(),
            autoload_with=connection,
        )
        for row in connection.execute(sa.select(current)).mappings():
            key = sa.and_(
                history.c.ecosystem == row["ecosystem"],
                history.c.tenant_id == row["tenant_id"],
                history.c.attempt_sequence == row["attempt_sequence"],
                history.c.window_start == row["window_start"],
                history.c.window_end == row["window_end"],
            )
            if connection.execute(sa.select(sa.literal(1)).where(key).limit(1)).first() is None:
                connection.execute(sa.insert(history).values(**dict(row)))

    def downgrade(self, connection: Connection, *, target_revision: str) -> None:
        inspector = inspect(connection)
        names = set(inspector.get_table_names())
        if target_revision == "027":
            for name in (
                "ccloud_focus_preview_repair_dates",
                "ccloud_focus_preview_repairs",
                "ccloud_source_capture_readiness_history",
            ):
                if name in names:
                    sa.Table(name, sa.MetaData(), autoload_with=connection).drop(connection)
            return
        if target_revision == "026":
            for name in (
                "ccloud_source_capture_readiness",
                "ccloud_organization_authority_attempts",
                "ccloud_source_evidence_attempts",
            ):
                if name in names:
                    sa.Table(name, sa.MetaData(), autoload_with=connection).drop(connection)
            if _SOURCE_TABLE in names and "capture_id" in _columns(connection, _SOURCE_TABLE):
                connection.exec_driver_sql("ALTER TABLE ccloud_cost_source_records DROP COLUMN capture_id")
            return
        if target_revision == "021":
            for name in (
                "ccloud_allocation_lineage_portions",
                "ccloud_allocation_lineage_runs",
            ):
                if name in names:
                    sa.Table(name, sa.MetaData(), autoload_with=connection).drop(connection)
            if _SOURCE_TABLE not in names:
                return
            for column in reversed(_V21_COLUMNS):
                if column in _columns(connection, _SOURCE_TABLE):
                    connection.exec_driver_sql(f"ALTER TABLE ccloud_cost_source_records DROP COLUMN {column}")
            return
        if target_revision == "018":
            if _SOURCE_TABLE in names:
                sa.Table(_SOURCE_TABLE, sa.MetaData(), autoload_with=connection).drop(connection)
            return
        raise ValueError(f"unknown Preview evidence target revision: {target_revision}")
