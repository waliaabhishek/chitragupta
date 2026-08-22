from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

if TYPE_CHECKING:
    import pytest

TABLE_NAME = "ccloud_focus_preview_retention_outcomes"


def test_revision_032_uses_guarded_preview_hook_while_the_next_revision_is_head() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "032_add_focus_preview_retention_outcomes.py"
    )

    assert script.get_current_head() == "034"
    source = migration_path.read_text(encoding="utf-8")
    assert 'run_preview_evidence_step("032")' in source
    assert 'run_preview_evidence_downgrade_step("032")' in source


def test_enabled_upgrade_031_to_032_adds_only_retention_outcome_table(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'upgrade.db'}"
    config = _config(url, selection="confluent_cloud")

    command.upgrade(config, "031")
    command.upgrade(config, "032")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert TABLE_NAME in tables
    assert "ccloud_focus_preview_repair_heads" in tables
    assert "ccloud_preview_source_allocation_lineage_portions" in tables


def test_disabled_upgrade_to_head_does_not_create_optional_retention_table(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'disabled.db'}"

    command.upgrade(_config(url, selection="disabled"), "head")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert TABLE_NAME not in tables


def test_downgrade_032_removes_only_retention_outcome_table(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    config = _config(url, selection="confluent_cloud")

    command.upgrade(config, "032")
    command.downgrade(config, "031")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert TABLE_NAME not in tables
    assert "ccloud_focus_preview_repair_heads" in tables
    assert "ccloud_preview_source_allocation_lineage_portions" in tables


def test_runtime_preview_preparation_targets_current_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = CCloudStorageModule()
    calls: list[str] = []
    original = module.prepare_preview_evidence_migration

    def record(connection: object, *, target_revision: str) -> None:
        calls.append(target_revision)
        original(connection, target_revision=target_revision)  # type: ignore[arg-type]

    monkeypatch.setattr(module, "prepare_preview_evidence_migration", record)
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'runtime-target.db'}",
        module,
        use_migrations=False,
        focus_preview_enabled=True,
    )
    try:
        backend.create_tables()
    finally:
        backend.dispose()

    assert calls == ["034"]
