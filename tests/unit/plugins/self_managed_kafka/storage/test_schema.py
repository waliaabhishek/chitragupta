"""Schema ownership and migration tests for Self-Managed Kafka storage."""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from plugins.self_managed_kafka.storage.tables import SelfManagedKafkaScopeStateTable
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

_SCOPE_TABLE = "self_managed_kafka_scope_state"
_SNAPSHOT_TABLE = "self_managed_kafka_principal_team_snapshots"
_PLUGIN_TABLES = {_SCOPE_TABLE, _SNAPSHOT_TABLE}


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _table_shape(
    url: str, table_name: str
) -> tuple[dict[str, tuple[str, bool]], tuple[str, ...], set[tuple[tuple[str, ...], str, tuple[str, ...]]]]:
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        columns = {
            column["name"]: (type(column["type"]).__name__, bool(column["nullable"]))
            for column in inspector.get_columns(table_name)
        }
        primary_key = tuple(inspector.get_pk_constraint(table_name).get("constrained_columns") or ())
        foreign_keys = {
            (
                tuple(foreign_key.get("constrained_columns") or ()),
                str(foreign_key["referred_table"]),
                tuple(foreign_key.get("referred_columns") or ()),
            )
            for foreign_key in inspector.get_foreign_keys(table_name)
        }
        return columns, primary_key, foreign_keys
    finally:
        engine.dispose()


def test_selected_migration_creates_exact_plugin_schema_after_core_032(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'selected-schema.db'}"
    command.upgrade(_config(url), "032")

    command.upgrade(_config(url, plugin_storage="self_managed_kafka"), "033")

    assert _table_names(url) >= _PLUGIN_TABLES
    scope_columns, scope_pk, scope_foreign_keys = _table_shape(url, _SCOPE_TABLE)
    snapshot_columns, snapshot_pk, snapshot_foreign_keys = _table_shape(url, _SNAPSHOT_TABLE)
    assert scope_columns == {
        "ecosystem": ("VARCHAR", False),
        "tenant_id": ("VARCHAR", False),
        "cluster_id": ("VARCHAR", False),
        "metrics_identifier_label": ("VARCHAR", False),
        "metrics_identifier": ("VARCHAR", False),
        "status": ("VARCHAR", False),
        "opened_at": ("DATETIME", True),
        "first_blocked_window_start": ("DATETIME", True),
        "first_blocked_window_end": ("DATETIME", True),
        "last_failure_reason": ("VARCHAR", True),
        "last_failure_status": ("VARCHAR", True),
        "last_failure_detail": ("VARCHAR", True),
        "last_probe_at": ("DATETIME", True),
        "last_probe_status": ("VARCHAR", True),
        "recovered_at": ("DATETIME", True),
        "recovery_cursor_date": ("DATE", True),
        "retention_gap_start": ("DATETIME", True),
        "retention_gap_end": ("DATETIME", True),
    }
    assert scope_pk == ("ecosystem", "tenant_id", "cluster_id")
    assert scope_foreign_keys == set()
    assert snapshot_columns == {
        "timestamp": ("DATETIME", False),
        "dimension_id": ("INTEGER", False),
        "team": ("VARCHAR", False),
    }
    assert snapshot_pk == ("timestamp", "dimension_id")
    assert snapshot_foreign_keys == {(("timestamp", "dimension_id"), "chargeback_facts", ("timestamp", "dimension_id"))}


def test_selected_prepare_rejects_partial_plugin_schema_at_core_head(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'partial-schema.db'}"
    command.upgrade(_config(url), "033")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(text(f"CREATE TABLE {_SCOPE_TABLE} (ecosystem VARCHAR NOT NULL)"))
    finally:
        engine.dispose()

    with pytest.raises(RuntimeError, match="self_managed_kafka_scope_state"):
        command.upgrade(_config(url, plugin_storage="self_managed_kafka"), "head")

    assert _table_names(url) >= {_SCOPE_TABLE}
    assert _SNAPSHOT_TABLE not in _table_names(url)


@pytest.mark.parametrize("incompatibility", ("type", "nullability", "primary_key", "foreign_key"))
def test_selected_prepare_rejects_incompatible_snapshot_shape(tmp_path: Path, incompatibility: str) -> None:
    url = f"sqlite:///{tmp_path / f'incompatible-{incompatibility}.db'}"
    command.upgrade(_config(url), "033")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            SelfManagedKafkaScopeStateTable.__table__.create(connection)
            snapshot_definition = {
                "type": """
                    CREATE TABLE self_managed_kafka_principal_team_snapshots (
                        timestamp DATETIME NOT NULL,
                        dimension_id INTEGER NOT NULL,
                        team INTEGER NOT NULL,
                        PRIMARY KEY (timestamp, dimension_id),
                        FOREIGN KEY (timestamp, dimension_id)
                            REFERENCES chargeback_facts(timestamp, dimension_id)
                    )
                """,
                "nullability": """
                    CREATE TABLE self_managed_kafka_principal_team_snapshots (
                        timestamp DATETIME NOT NULL,
                        dimension_id INTEGER NOT NULL,
                        team VARCHAR NULL,
                        PRIMARY KEY (timestamp, dimension_id),
                        FOREIGN KEY (timestamp, dimension_id)
                            REFERENCES chargeback_facts(timestamp, dimension_id)
                    )
                """,
                "primary_key": """
                    CREATE TABLE self_managed_kafka_principal_team_snapshots (
                        timestamp DATETIME NOT NULL,
                        dimension_id INTEGER NOT NULL,
                        team VARCHAR NOT NULL,
                        PRIMARY KEY (dimension_id, timestamp),
                        FOREIGN KEY (timestamp, dimension_id)
                            REFERENCES chargeback_facts(timestamp, dimension_id)
                    )
                """,
                "foreign_key": """
                    CREATE TABLE self_managed_kafka_principal_team_snapshots (
                        timestamp DATETIME NOT NULL,
                        dimension_id INTEGER NOT NULL,
                        team VARCHAR NOT NULL,
                        PRIMARY KEY (timestamp, dimension_id),
                        FOREIGN KEY (dimension_id)
                            REFERENCES chargeback_dimensions(dimension_id)
                    )
                """,
            }[incompatibility]
            connection.execute(text(snapshot_definition))
    finally:
        engine.dispose()

    with pytest.raises(RuntimeError, match="incompatible"):
        command.upgrade(_config(url, plugin_storage="self_managed_kafka"), "head")


def test_self_managed_direct_schema_matches_selected_migration_schema(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct.db'}"
    command.upgrade(_config(migrated_url, plugin_storage="self_managed_kafka"), "head")
    direct_backend = SQLModelBackend(direct_url, SelfManagedKafkaStorageModule(), use_migrations=False)

    try:
        direct_backend.create_tables()
        assert "alembic_version" not in _table_names(direct_url)
        for table_name in _PLUGIN_TABLES:
            assert _table_shape(migrated_url, table_name) == _table_shape(direct_url, table_name)
    finally:
        direct_backend.dispose()


def test_direct_core_schema_does_not_create_self_managed_tables(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'core-direct.db'}"
    backend = SQLModelBackend(url, CoreStorageModule(), use_migrations=False)

    try:
        backend.create_tables()
    finally:
        backend.dispose()

    assert _PLUGIN_TABLES.isdisjoint(_table_names(url))


def test_selected_downgrade_removes_only_plugin_tables_and_preserves_core_facts(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'preserve-facts.db'}"
    selected = _config(url, plugin_storage="self_managed_kafka")
    command.upgrade(selected, "head")
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_dimensions (
                        ecosystem, tenant_id, resource_id, product_category, product_type,
                        identity_id, cost_type, allocation_method, allocation_detail, env_id
                    ) VALUES (
                        'self_managed_kafka', 'tenant-1', 'cluster-1', 'network', 'SELF_KAFKA_NETWORK_INGRESS',
                        'User:alice', 'usage', 'principal_quota_ready_v1', 'usage_ratio_allocation', ''
                    )
                    """
                )
            )
            dimension_id = connection.execute(
                text("SELECT dimension_id FROM chargeback_dimensions WHERE identity_id = 'User:alice'")
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO chargeback_facts (timestamp, dimension_id, amount, tags_json) "
                    "VALUES ('2026-08-01T00:00:00+00:00', :dimension_id, '1.0000', '[]')"
                ),
                {"dimension_id": dimension_id},
            )
    finally:
        engine.dispose()

    command.downgrade(selected, "032")

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            fact_count = connection.execute(text("SELECT COUNT(*) FROM chargeback_facts")).scalar_one()
            revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()
    assert fact_count == 1
    assert revision == "032"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(url))
