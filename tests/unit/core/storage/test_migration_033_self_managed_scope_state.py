"""Migration coverage for the self-managed Kafka target-scope state."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

TABLE_NAME = "self_managed_kafka_scope_state"


def test_revision_033_is_the_current_head_and_declares_its_predecessor() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "033_add_self_managed_kafka_scope_state.py"
    )
    source = migration_path.read_text(encoding="utf-8")

    assert script.get_current_head() == "033"
    assert 'revision = "033"' in source
    assert 'down_revision = "032"' in source


def test_upgrade_and_downgrade_create_only_scope_state_table(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'scope-state.db'}"
    config = _config(url, selection="disabled")

    command.upgrade(config, "032")
    command.upgrade(config, "033")
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        columns = {column["name"] for column in inspector.get_columns(TABLE_NAME)}
        primary_key = tuple(inspector.get_pk_constraint(TABLE_NAME)["constrained_columns"])
    finally:
        engine.dispose()

    assert columns == {
        "ecosystem",
        "tenant_id",
        "cluster_id",
        "metrics_identifier_label",
        "metrics_identifier",
        "status",
        "opened_at",
        "first_blocked_window_start",
        "first_blocked_window_end",
        "last_failure_reason",
        "last_failure_status",
        "last_failure_detail",
        "last_probe_at",
        "last_probe_status",
        "recovered_at",
        "recovery_cursor_date",
        "retention_gap_start",
        "retention_gap_end",
    }
    assert primary_key == ("ecosystem", "tenant_id", "cluster_id")

    command.downgrade(config, "032")
    engine = create_engine(url)
    try:
        assert TABLE_NAME not in set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_migration_schema_matches_plugin_storage_registration(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct.db'}"
    config = _config(migrated_url, selection="disabled")

    command.upgrade(config, "033")
    backend = SQLModelBackend(direct_url, SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    migrated_engine = create_engine(migrated_url)
    direct_engine = create_engine(direct_url)
    try:
        migrated_columns = {column["name"] for column in inspect(migrated_engine).get_columns(TABLE_NAME)}
        direct_columns = {column["name"] for column in inspect(direct_engine).get_columns(TABLE_NAME)}
    finally:
        migrated_engine.dispose()
        direct_engine.dispose()
        backend.dispose()

    assert migrated_columns == direct_columns
