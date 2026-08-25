from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy as sa
from sqlalchemy import inspect

from plugins.self_managed_kafka.storage.tables import (
    SelfManagedKafkaPrincipalTeamSnapshotTable,
    SelfManagedKafkaScopeStateTable,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


class SelfManagedKafkaSchemaError(RuntimeError):
    """Raised when plugin-owned tables are missing or incompatible."""


class SelfManagedKafkaSchemaManager:
    """Create, verify, and remove the self-managed plugin-owned tables."""

    target_revision = "033"

    _scope_columns: Mapping[str, tuple[str, bool]] = {
        "ecosystem": ("string", False),
        "tenant_id": ("string", False),
        "cluster_id": ("string", False),
        "metrics_identifier_label": ("string", False),
        "metrics_identifier": ("string", False),
        "status": ("string", False),
        "opened_at": ("datetime", True),
        "first_blocked_window_start": ("datetime", True),
        "first_blocked_window_end": ("datetime", True),
        "last_failure_reason": ("string", True),
        "last_failure_status": ("string", True),
        "last_failure_detail": ("string", True),
        "last_probe_at": ("datetime", True),
        "last_probe_status": ("string", True),
        "recovered_at": ("datetime", True),
        "recovery_cursor_date": ("date", True),
        "retention_gap_start": ("datetime", True),
        "retention_gap_end": ("datetime", True),
    }
    _snapshot_columns: Mapping[str, tuple[str, bool]] = {
        "timestamp": ("datetime", False),
        "dimension_id": ("integer", False),
        "team": ("string", False),
    }
    _scope_primary_key = ("ecosystem", "tenant_id", "cluster_id")
    _snapshot_primary_key = ("timestamp", "dimension_id")

    def prepare(self, connection: Connection, *, target_revision: str) -> None:
        self._check_revision(target_revision)
        inspector = inspect(connection)
        table_names = set(inspector.get_table_names())
        scope_name = SelfManagedKafkaScopeStateTable.__tablename__
        snapshot_name = SelfManagedKafkaPrincipalTeamSnapshotTable.__tablename__
        present = {name for name in (scope_name, snapshot_name) if name in table_names}
        if present and present != {scope_name, snapshot_name}:
            missing = snapshot_name if scope_name in present else scope_name
            existing = next(iter(present))
            raise SelfManagedKafkaSchemaError(f"partial plugin storage schema; existing {existing}; missing {missing}")
        if not present:
            scope_table = cast("Any", SelfManagedKafkaScopeStateTable).__table__
            snapshot_table = cast("Any", SelfManagedKafkaPrincipalTeamSnapshotTable).__table__
            scope_table.create(connection, checkfirst=False)
            snapshot_table.create(connection, checkfirst=False)
        self._verify(connection)

    def downgrade(self, connection: Connection, *, target_revision: str) -> None:
        self._check_revision(target_revision)
        self._verify(connection)
        snapshot_table = cast("Any", SelfManagedKafkaPrincipalTeamSnapshotTable).__table__
        scope_table = cast("Any", SelfManagedKafkaScopeStateTable).__table__
        snapshot_table.drop(connection, checkfirst=False)
        scope_table.drop(connection, checkfirst=False)

    @classmethod
    def _check_revision(cls, target_revision: str) -> None:
        if target_revision != cls.target_revision:
            raise ValueError(f"unknown plugin storage target revision: {target_revision}")

    @classmethod
    def _verify(cls, connection: Connection) -> None:
        inspector = inspect(connection)
        expected = (
            (
                SelfManagedKafkaScopeStateTable.__tablename__,
                cls._scope_columns,
                cls._scope_primary_key,
                (),
            ),
            (
                SelfManagedKafkaPrincipalTeamSnapshotTable.__tablename__,
                cls._snapshot_columns,
                cls._snapshot_primary_key,
                (("timestamp", "dimension_id"), "chargeback_facts", ("timestamp", "dimension_id")),
            ),
        )
        for table_name, columns, primary_key, foreign_key in expected:
            if table_name not in inspector.get_table_names():
                raise SelfManagedKafkaSchemaError(f"missing plugin storage table: {table_name}")
            actual_columns = {column["name"]: column for column in inspector.get_columns(table_name)}
            if set(actual_columns) != set(columns):
                raise SelfManagedKafkaSchemaError(f"incompatible columns: {table_name}")
            for name, (type_family, nullable) in columns.items():
                actual = actual_columns[name]
                if _type_family(actual["type"]) != type_family or bool(actual["nullable"]) != nullable:
                    raise SelfManagedKafkaSchemaError(f"incompatible column: {table_name}.{name}")
            actual_pk = tuple(inspector.get_pk_constraint(table_name).get("constrained_columns") or ())
            if actual_pk != primary_key:
                raise SelfManagedKafkaSchemaError(f"incompatible primary key: {table_name}")
            actual_fks = {
                (
                    tuple(foreign_key.get("constrained_columns") or ()),
                    str(foreign_key["referred_table"]),
                    tuple(foreign_key.get("referred_columns") or ()),
                )
                for foreign_key in inspector.get_foreign_keys(table_name)
            }
            expected_fks = {foreign_key} if foreign_key else set()
            if actual_fks != expected_fks:
                raise SelfManagedKafkaSchemaError(f"incompatible foreign keys: {table_name}")


def _type_family(value: sa.types.TypeEngine[object]) -> str:
    if isinstance(value, sa.DateTime):
        return "datetime"
    if isinstance(value, sa.Date):
        return "date"
    if isinstance(value, sa.Integer):
        return "integer"
    if isinstance(value, sa.String):
        return "string"
    return type(value).__name__.lower()


__all__ = ["SelfManagedKafkaSchemaError", "SelfManagedKafkaSchemaManager"]
