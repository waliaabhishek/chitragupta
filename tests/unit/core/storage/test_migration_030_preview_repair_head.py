from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from core.preview.storage_availability import PreviewEvidenceOfflineMigrationError
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

HEAD_TABLE = "ccloud_focus_preview_repair_heads"
REPAIR_TABLE = "ccloud_focus_preview_repairs"
DATE_TABLE = "ccloud_focus_preview_repair_dates"


def _config(
    url: str,
    *,
    selection: str | None = None,
    output: io.StringIO | None = None,
) -> Config:
    migrations = Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
    config = Config(str(migrations / "alembic.ini"), output_buffer=output)
    config.set_main_option("script_location", str(migrations))
    config.set_main_option("sqlalchemy.url", url)
    config.cmd_opts = SimpleNamespace(x=[] if selection is None else [f"focus_preview={selection}"])
    return config


def _tables(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _insert_repair(
    connection: object,
    *,
    repair_id: str,
    owner: str,
    status: str,
    created_at: datetime,
    completed_at: datetime | None = None,
) -> None:
    connection.execute(  # type: ignore[attr-defined]
        text(
            f"""
            INSERT INTO {REPAIR_TABLE}
                (repair_id, tenant_name, ecosystem, tenant_id, start_date,
                 end_date, status, created_at, started_at, completed_at,
                 diagnostic_code, diagnostic_message, diagnostic_retryable)
            VALUES
                (:repair_id, :owner, 'confluent_cloud', :owner, '2026-07-01',
                 '2026-07-02', :status, :created_at,
                 CASE WHEN :status = 'queued' THEN NULL ELSE :created_at END,
                 :completed_at,
                 CASE WHEN :status = 'failed' THEN 'failed' ELSE NULL END,
                 CASE WHEN :status = 'failed' THEN 'retry repair' ELSE NULL END,
                 CASE WHEN :status = 'failed' THEN 1 ELSE NULL END)
            """
        ),
        {
            "repair_id": repair_id,
            "owner": owner,
            "status": status,
            "created_at": created_at,
            "completed_at": completed_at,
        },
    )


def test_revision_030_is_head_and_uses_guarded_preview_hook() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "030_add_focus_preview_repair_head.py"
    )

    assert script.get_current_head() == "030"
    source = migration_path.read_text(encoding="utf-8")
    assert 'run_preview_evidence_step("030")' in source
    assert 'run_preview_evidence_downgrade_step("030")' in source
    assert "INSERT OR REPLACE" not in source.upper()


def test_fresh_enabled_schema_contains_cumulative_revision_030_shape(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'fresh-enabled.db'}"

    command.upgrade(_config(url, selection="confluent_cloud"), "head")

    assert _tables(url) >= {
        HEAD_TABLE,
        REPAIR_TABLE,
        DATE_TABLE,
        "ccloud_source_capture_readiness_history",
        "preview_artifact_files",
    }
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        head = inspector.get_pk_constraint(HEAD_TABLE)
        foreign_keys = inspector.get_foreign_keys(HEAD_TABLE)
        unique_shapes = {tuple(item["column_names"]) for item in inspector.get_unique_constraints(HEAD_TABLE)} | {
            tuple(item["column_names"]) for item in inspector.get_indexes(HEAD_TABLE) if item["unique"]
        }
    finally:
        engine.dispose()
    assert tuple(head["constrained_columns"]) == ("ecosystem", "tenant_id")
    assert {
        (
            tuple(item["constrained_columns"]),
            item["referred_table"],
            tuple(item["referred_columns"]),
        )
        for item in foreign_keys
    } == {(("repair_id",), REPAIR_TABLE, ("repair_id",))}
    assert ("repair_id",) in unique_shapes


def test_disabled_upgrade_creates_no_optional_revision_030_table(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'disabled.db'}"

    command.upgrade(_config(url, selection="disabled"), "head")

    assert HEAD_TABLE not in _tables(url)


def test_upgrade_from_029_backfills_only_unambiguous_current_repairs(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'backfill.db'}"
    command.upgrade(_config(url, selection="confluent_cloud"), "029")
    created = datetime(2026, 7, 1, tzinfo=UTC)
    later = datetime(2026, 7, 2, tzinfo=UTC)
    engine = create_engine(url)
    with engine.begin() as connection:
        _insert_repair(
            connection,
            repair_id="active-only",
            owner="active-only",
            status="queued",
            created_at=created,
        )
        _insert_repair(
            connection,
            repair_id="active-first",
            owner="active-many",
            status="queued",
            created_at=created,
        )
        _insert_repair(
            connection,
            repair_id="active-second",
            owner="active-many",
            status="running",
            created_at=later,
        )
        _insert_repair(
            connection,
            repair_id="terminal-old",
            owner="terminal-unique",
            status="failed",
            created_at=created,
            completed_at=later,
        )
        _insert_repair(
            connection,
            repair_id="terminal-new",
            owner="terminal-unique",
            status="completed",
            created_at=later,
            completed_at=later,
        )
        _insert_repair(
            connection,
            repair_id="tie-z",
            owner="terminal-tie",
            status="failed",
            created_at=later,
            completed_at=datetime(2026, 7, 4, tzinfo=UTC),
        )
        _insert_repair(
            connection,
            repair_id="tie-a",
            owner="terminal-tie",
            status="completed",
            created_at=later,
            completed_at=datetime(2026, 7, 3, tzinfo=UTC),
        )
    engine.dispose()

    command.upgrade(_config(url, selection="confluent_cloud"), "030")

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            heads = {
                row.tenant_id: row.repair_id
                for row in connection.execute(
                    text(
                        f"""
                        SELECT tenant_id, repair_id
                        FROM {HEAD_TABLE}
                        ORDER BY tenant_id
                        """
                    )
                )
            }
    finally:
        engine.dispose()
    assert heads == {
        "active-many": None,
        "active-only": "active-only",
        "terminal-tie": None,
        "terminal-unique": "terminal-new",
    }
    assert "no-history" not in heads


def test_preview_enabled_after_disabled_alembic_upgrade_gets_complete_030_shape(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'later-enabled.db'}"
    command.upgrade(_config(url, selection="disabled"), "head")
    assert HEAD_TABLE not in _tables(url)

    backend = SQLModelBackend(
        url,
        CCloudStorageModule(),
        use_migrations=True,
        focus_preview_enabled=True,
    )
    try:
        backend.create_tables()
        assert backend.preview_evidence_availability.state.value == "ready"
        assert _tables(url) >= {HEAD_TABLE, REPAIR_TABLE, DATE_TABLE}
    finally:
        backend.dispose()


def test_runtime_preview_preparation_targets_revision_030(
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

    assert calls == ["030"]


def test_downgrade_030_removes_only_head_and_preserves_repair_rows(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    command.upgrade(_config(url, selection="confluent_cloud"), "030")
    engine = create_engine(url)
    with engine.begin() as connection:
        _insert_repair(
            connection,
            repair_id="preserved",
            owner="tenant-1",
            status="queued",
            created_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    engine.dispose()

    command.downgrade(_config(url, selection="confluent_cloud"), "029")

    assert HEAD_TABLE not in _tables(url)
    assert {REPAIR_TABLE, DATE_TABLE} <= _tables(url)
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            assert connection.execute(text(f"SELECT repair_id FROM {REPAIR_TABLE}")).scalar_one() == "preserved"
    finally:
        engine.dispose()


def test_offline_downgrade_030_reports_guarded_029_command() -> None:
    output = io.StringIO()

    with pytest.raises(PreviewEvidenceOfflineMigrationError) as raised:
        command.downgrade(
            _config(
                "sqlite:///offline.db",
                selection="disabled",
                output=output,
            ),
            "030:029",
            sql=True,
        )

    assert str(raised.value) == (
        "Preview evidence downgrades require an online database connection; run "
        "`uv run alembic -c src/core/storage/migrations/alembic.ini "
        "-x focus_preview=confluent_cloud downgrade 029`."
    )
    assert HEAD_TABLE not in output.getvalue()
