from __future__ import annotations

import importlib
import inspect
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect

from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config, _seed_legacy_rows, _snapshots


def test_migration_020_adds_nullable_diagnostic_correlation_column_and_preserves_data(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-020.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "019")
    _seed_legacy_rows(connection_string)
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests (
                    request_id, tenant_name, ecosystem, tenant_id, grain,
                    start_date, end_date, column_profile, status, created_at,
                    diagnostic_code, diagnostic_message, diagnostic_retryable
                ) VALUES (
                    'request-legacy', 'production', 'confluent_cloud', 'tenant-1', 'daily',
                    '2026-07-01', '2026-07-02', 'full', 'failed', '2026-07-04 00:00:00',
                    'calculation_unavailable', 'legacy diagnostic', 1
                )
                """
            )
        )
    before = _snapshots(engine)

    command.upgrade(config, "020")

    columns = {column["name"]: column for column in sa_inspect(engine).get_columns("preview_requests")}
    assert columns["diagnostic_source_correlation_ids_json"]["nullable"] is True
    assert str(columns["diagnostic_source_correlation_ids_json"]["type"]) == "TEXT"
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT diagnostic_code, diagnostic_source_correlation_ids_json "
                "FROM preview_requests WHERE request_id = 'request-legacy'"
            )
        ).one()
    assert tuple(row) == ("calculation_unavailable", None)
    assert _snapshots(engine) == before

    command.downgrade(config, "019")

    assert "diagnostic_source_correlation_ids_json" not in {
        column["name"] for column in sa_inspect(engine).get_columns("preview_requests")
    }
    with engine.connect() as connection:
        preserved = connection.execute(
            text(
                "SELECT request_id, diagnostic_code, diagnostic_message, diagnostic_retryable "
                "FROM preview_requests WHERE request_id = 'request-legacy'"
            )
        ).one()
    assert tuple(preserved) == (
        "request-legacy",
        "calculation_unavailable",
        "legacy diagnostic",
        1,
    )
    assert _snapshots(engine) == before
    engine.dispose()


def test_migration_020_drops_only_its_column_on_downgrade() -> None:
    migration = importlib.import_module("core.storage.migrations.versions.020_add_preview_diagnostic_correlations")
    source = inspect.getsource(migration.downgrade)
    normalized = " ".join(source.split())

    assert "drop_column" in normalized
    assert "diagnostic_source_correlation_ids_json" in normalized
    assert "drop_table" not in normalized
    assert "delete" not in normalized.casefold()
