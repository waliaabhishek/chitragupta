"""Provider-neutral plugin-storage migration behavior."""

from __future__ import annotations

import io
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text

from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.migrations import plugin_storage_hook
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

_PLUGIN_TABLES = {
    "self_managed_kafka_scope_state",
    "self_managed_kafka_principal_team_snapshots",
}


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _version(url: str) -> str:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()


class _FakePluginStorageModule:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[str, object, str]] = []
        self.fail = fail

    def prepare_plugin_storage_migration(self, connection: object, *, target_revision: str) -> None:
        if self.fail:
            raise RuntimeError("fake plugin preparation failed")
        self.calls.append(("prepare", connection, target_revision))

    def downgrade_plugin_storage_migration(self, connection: object, *, target_revision: str) -> None:
        self.calls.append(("downgrade", connection, target_revision))


def _patch_hook_runtime(monkeypatch: pytest.MonkeyPatch, module: object | None) -> tuple[object, object]:
    connection = object()
    config = SimpleNamespace(
        attributes={
            plugin_storage_hook.CFG_PLUGIN_STORAGE_MODULE: module,
            plugin_storage_hook.CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT: module is not None,
        }
    )
    context = SimpleNamespace(config=config, is_offline_mode=lambda: False)
    operation = SimpleNamespace(get_bind=lambda: connection)
    monkeypatch.setattr(plugin_storage_hook, "context", context)
    monkeypatch.setattr(plugin_storage_hook, "op", operation)
    return context, connection


def test_selected_fake_capability_dispatches_upgrade_and_downgrade(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _FakePluginStorageModule()
    _context, connection = _patch_hook_runtime(monkeypatch, module)

    plugin_storage_hook.run_plugin_storage_step("033")
    plugin_storage_hook.run_plugin_storage_downgrade_step("033")

    assert module.calls == [("prepare", connection, "033"), ("downgrade", connection, "033")]


def test_selected_fake_capability_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _FakePluginStorageModule(fail=True)
    _patch_hook_runtime(monkeypatch, module)

    with pytest.raises(RuntimeError, match="fake plugin preparation failed"):
        plugin_storage_hook.run_plugin_storage_step("033")


def test_post_upgrade_is_noop_without_module(monkeypatch: pytest.MonkeyPatch) -> None:
    _context, _connection = _patch_hook_runtime(monkeypatch, None)

    class _UnexpectedMigrationContext:
        @classmethod
        def configure(cls, connection: object) -> object:
            raise AssertionError(f"unexpected current-head lookup for {connection!r}")

    monkeypatch.setattr(plugin_storage_hook, "MigrationContext", _UnexpectedMigrationContext)
    plugin_storage_hook.run_plugin_storage_post_upgrade(object(), target_revision="033")


def test_post_upgrade_is_noop_for_non_target_head(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _FakePluginStorageModule()
    _context, _connection = _patch_hook_runtime(monkeypatch, module)

    class _MigrationContextAtPreviousHead:
        @classmethod
        def configure(cls, connection: object) -> _MigrationContextAtPreviousHead:
            return cls()

        def get_current_heads(self) -> tuple[str, ...]:
            return ("032",)

    monkeypatch.setattr(plugin_storage_hook, "MigrationContext", _MigrationContextAtPreviousHead)
    plugin_storage_hook.run_plugin_storage_post_upgrade(object(), target_revision="033")

    assert module.calls == []


def test_provider_neutral_revision_is_noop_without_selected_module(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'no-selection.db'}"

    command.upgrade(_config(url), "032")
    command.upgrade(_config(url), "033")

    assert _version(url) == "033"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(url))
    engine = create_engine(url)
    try:
        assert "principal_team" not in {column["name"] for column in inspect(engine).get_columns("chargeback_facts")}
    finally:
        engine.dispose()


def test_core_and_ccloud_reach_head_without_self_managed_schema(tmp_path: Path) -> None:
    core_url = f"sqlite:///{tmp_path / 'core.db'}"
    ccloud_url = f"sqlite:///{tmp_path / 'ccloud.db'}"
    core_backend = SQLModelBackend(core_url, CoreStorageModule(), use_migrations=True)
    ccloud_backend = SQLModelBackend(ccloud_url, CCloudStorageModule(), use_migrations=True)

    try:
        core_backend.create_tables()
        ccloud_backend.create_tables()
    finally:
        core_backend.dispose()
        ccloud_backend.dispose()

    assert _version(core_url) == _version(ccloud_url) == "033"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(core_url))
    assert _PLUGIN_TABLES.isdisjoint(_table_names(ccloud_url))
    ccloud_engine = create_engine(ccloud_url)
    try:
        ccloud_inspector = inspect(ccloud_engine)
        assert "env_id" in {column["name"] for column in ccloud_inspector.get_columns("chargeback_dimensions")}
        assert "principal_team" not in {column["name"] for column in ccloud_inspector.get_columns("chargeback_facts")}
    finally:
        ccloud_engine.dispose()


def test_self_managed_runtime_selection_creates_both_plugin_tables(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'self-managed-runtime.db'}"
    backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=True)

    try:
        backend.create_tables()
    finally:
        backend.dispose()

    assert _version(url) == "033"
    assert _table_names(url) >= _PLUGIN_TABLES


def test_manual_selected_upgrade_prepares_plugin_schema_at_existing_core_head(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'manual-already-head.db'}"

    command.upgrade(_config(url), "033")
    assert _version(url) == "033"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(url))

    command.upgrade(_config(url, plugin_storage="self_managed_kafka"), "head")

    assert _version(url) == "033"
    assert _table_names(url) >= _PLUGIN_TABLES


def test_repeated_selected_prepare_verifies_without_ddl(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'idempotent.db'}"
    config = _config(url, plugin_storage="self_managed_kafka")

    command.upgrade(config, "head")
    before = _table_names(url)
    command.upgrade(config, "head")

    assert _version(url) == "033"
    assert before == _table_names(url)
    assert before >= _PLUGIN_TABLES


def test_manual_self_managed_selector_creates_and_downgrades_plugin_schema(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'selected-downgrade.db'}"
    selected = _config(url, plugin_storage="self_managed_kafka")

    command.upgrade(selected, "head")
    assert _table_names(url) >= _PLUGIN_TABLES

    command.downgrade(selected, "032")

    assert _version(url) == "032"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(url))


def test_downgrade_without_plugin_storage_selection_aborts_at_033(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'omitted-downgrade.db'}"
    selected = _config(url, plugin_storage="self_managed_kafka")
    command.upgrade(selected, "head")

    with pytest.raises(RuntimeError, match="plugin_storage"):
        command.downgrade(_config(url), "032")

    assert _version(url) == "033"
    assert _table_names(url) >= _PLUGIN_TABLES


def test_downgrade_with_disabled_selection_stamps_032_without_provider_ddl(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'disabled-downgrade.db'}"
    selected = _config(url, plugin_storage="self_managed_kafka")
    command.upgrade(selected, "head")
    before = _table_names(url)

    command.downgrade(_config(url, plugin_storage="disabled"), "032")

    assert _version(url) == "032"
    assert _table_names(url) >= _PLUGIN_TABLES
    assert _table_names(url) == before


def test_invalid_manual_plugin_storage_selection_fails_before_schema_changes(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'unknown-plugin.db'}"

    with pytest.raises(ValueError, match="plugin_storage"):
        command.upgrade(_config(url, plugin_storage="not-a-plugin"), "head")


def test_selected_plugin_storage_offline_migration_requires_online_connection() -> None:
    output = io.StringIO()

    with pytest.raises(RuntimeError, match="online"):
        command.upgrade(
            _config(
                "sqlite:///unused.db",
                plugin_storage="self_managed_kafka",
                output=output,
            ),
            "head",
            sql=True,
        )

    assert "CREATE TABLE self_managed_kafka" not in output.getvalue()


def test_core_and_ccloud_source_boundaries_have_no_self_managed_storage_knowledge() -> None:
    root = Path(__file__).resolve().parents[4]
    core_paths = (
        root / "src" / "core" / "models" / "chargeback.py",
        root / "src" / "core" / "storage" / "backends" / "sqlmodel" / "tables.py",
        root / "src" / "core" / "storage" / "backends" / "sqlmodel" / "mappers.py",
        root / "src" / "core" / "storage" / "backends" / "sqlmodel" / "repositories.py",
        root / "src" / "core" / "storage" / "backends" / "sqlmodel" / "unit_of_work.py",
        root / "src" / "core" / "storage" / "migrations" / "env.py",
    )
    ccloud_path = root / "src" / "plugins" / "confluent_cloud" / "storage" / "preview_schema.py"

    assert not (root / "src" / "core" / "api" / "chargeback_serialization.py").exists()
    for path in core_paths:
        source = path.read_text(encoding="utf-8")
        assert "principal_team" not in source
        assert "self_managed_kafka" not in source
        assert "self_managed_kafka_scope_state" not in source
        assert "self_managed_kafka_principal_team_snapshots" not in source
    ccloud_source = ccloud_path.read_text(encoding="utf-8")
    assert "self_managed_kafka" not in ccloud_source
    assert '"033", "034"' not in ccloud_source
