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

V27_TABLES = {
    "ccloud_source_capture_readiness_history",
    "ccloud_focus_preview_repairs",
    "ccloud_focus_preview_repair_dates",
}


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


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_revision_027_calls_guarded_preview_hook() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "027_add_focus_preview_historical_repair.py"
    )

    assert script.get_current_head() == "028"
    source = migration_path.read_text(encoding="utf-8")
    assert 'run_preview_evidence_step("027")' in source
    assert 'run_preview_evidence_downgrade_step("027")' in source


def test_disabled_upgrade_to_027_creates_no_repair_or_history_tables(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'disabled.db'}"

    command.upgrade(_config(url, selection="disabled"), "head")

    assert not (_table_names(url) & V27_TABLES)


def test_enabled_upgrade_to_027_creates_repair_and_history_tables(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'enabled.db'}"

    command.upgrade(_config(url, selection="confluent_cloud"), "head")

    assert _table_names(url) >= V27_TABLES


def test_migration_copies_current_readiness_to_history_without_rewriting_attempt_token(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'readiness-copy.db'}"
    command.upgrade(_config(url, selection="confluent_cloud"), "026")
    engine = create_engine(url)
    started = datetime(2026, 7, 1, tzinfo=UTC)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ccloud_source_evidence_attempts
                    (ecosystem, tenant_id, refresh_token, refresh_start, refresh_end,
                     status, started_at, completed_at, failure_reason)
                VALUES
                    ('confluent_cloud', 'tenant-1', 'ordinary-token',
                     :started, :ended, 'complete', :started, :ended, NULL)
                """
            ),
            {"started": started, "ended": datetime(2026, 7, 2, tzinfo=UTC)},
        )
        attempt_sequence = connection.execute(
            text("SELECT attempt_sequence FROM ccloud_source_evidence_attempts")
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO ccloud_source_capture_readiness
                    (ecosystem, tenant_id, window_start, window_end, capture_id,
                     captured_at, source_count, attempt_sequence)
                VALUES
                    ('confluent_cloud', 'tenant-1', :started, :ended, 'capture-1',
                     :ended, 2, :attempt_sequence)
                """
            ),
            {
                "started": started,
                "ended": datetime(2026, 7, 2, tzinfo=UTC),
                "attempt_sequence": attempt_sequence,
            },
        )
    engine.dispose()

    command.upgrade(_config(url, selection="confluent_cloud"), "head")

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            history = connection.execute(
                text(
                    """
                    SELECT capture_id, source_count, attempt_sequence
                    FROM ccloud_source_capture_readiness_history
                    """
                )
            ).one()
            token = connection.execute(
                text(
                    """
                    SELECT refresh_token
                    FROM ccloud_source_evidence_attempts
                    WHERE attempt_sequence = :attempt_sequence
                    """
                ),
                {"attempt_sequence": attempt_sequence},
            ).scalar_one()
    finally:
        engine.dispose()

    assert tuple(history) == ("capture-1", 2, attempt_sequence)
    assert token == "ordinary-token"


def test_offline_downgrade_027_preserves_actionable_guarded_error() -> None:
    output = io.StringIO()

    with pytest.raises(PreviewEvidenceOfflineMigrationError) as raised:
        command.downgrade(
            _config(
                "sqlite:///offline.db",
                selection="disabled",
                output=output,
            ),
            "027:026",
            sql=True,
        )

    assert str(raised.value) == (
        "Preview evidence downgrades require an online database connection; run "
        "`uv run alembic -c src/core/storage/migrations/alembic.ini "
        "-x focus_preview=confluent_cloud downgrade 026`."
    )
    assert "unsupported Preview evidence downgrade revision" not in str(raised.value)
    assert all(name not in output.getvalue() for name in V27_TABLES)
