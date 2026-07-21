from __future__ import annotations

import importlib
import json
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config

NEW_COLUMNS = {
    "effective_columns_json",
    "effective_coverage_start_date",
    "effective_coverage_end_date",
    "availability_cutoff_end_date",
    "monthly_status",
}


def _insert_ready_v4(connection_string: str) -> tuple[object, ...]:
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests (
                    request_id, tenant_name, ecosystem, tenant_id, grain,
                    start_date, end_date, column_profile, status, created_at,
                    started_at, completed_at, calculation_timestamp, source_through,
                    calculation_coverage_json, diagnostic_code, diagnostic_message,
                    diagnostic_retryable, diagnostic_source_correlation_ids_json,
                    storage_key, manifest_metadata_json, data_files_json
                ) VALUES (
                    'legacy-ready', 'production', 'confluent_cloud', 'tenant-1', 'daily',
                    '2026-07-01', '2026-07-02', 'full', 'ready', '2026-07-03 00:00:00',
                    '2026-07-03 00:01:00', '2026-07-03 00:02:00', '2026-07-03 00:00:00',
                    '2026-07-02 00:00:00',
                    :calculation_coverage,
                    NULL, NULL, NULL, NULL, 'legacy-ready',
                    :manifest_metadata,
                    :data_files
                )
                """
            ),
            {
                "calculation_coverage": json.dumps(
                    [
                        {
                            "tracking_date": "2026-07-01",
                            "calculation_id": "calculation-1",
                            "calculation_completed_at": "2026-07-03T00:00:00+00:00",
                            "calculation_run_id": 17,
                        }
                    ],
                    separators=(",", ":"),
                ),
                "manifest_metadata": json.dumps(
                    {
                        "media_type": "application/json",
                        "name": "manifest.json",
                        "order": None,
                        "sha256": "a" * 64,
                        "size_bytes": 2,
                    },
                    separators=(",", ":"),
                ),
                "data_files": json.dumps(
                    [
                        {
                            "media_type": "text/csv",
                            "name": "cost-and-usage.csv",
                            "order": 1,
                            "sha256": "b" * 64,
                            "size_bytes": 3,
                        }
                    ],
                    separators=(",", ":"),
                ),
            },
        )
        row = connection.execute(text("SELECT * FROM preview_requests WHERE request_id = 'legacy-ready'"))
        before = tuple(row.one())
    engine.dispose()
    return before


def test_migration_022_adds_only_nullable_preview_request_columns_and_preserves_ready_v4(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-022.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "021")
    before = _insert_ready_v4(connection_string)

    command.upgrade(config, "022")

    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
    assert columns.keys() >= NEW_COLUMNS
    assert all(columns[name]["nullable"] is True for name in NEW_COLUMNS)
    with engine.connect() as connection:
        row = connection.execute(text("SELECT * FROM preview_requests WHERE request_id = 'legacy-ready'"))
        after = tuple(row.one())
        new_values = connection.execute(
            text(
                "SELECT effective_columns_json, effective_coverage_start_date, "
                "effective_coverage_end_date, availability_cutoff_end_date, monthly_status "
                "FROM preview_requests WHERE request_id = 'legacy-ready'"
            )
        ).one()
    assert after[: len(before)] == before
    assert tuple(new_values) == (None, None, None, None, None)
    engine.dispose()


def test_migration_022_revision_chain_and_downgrade_are_narrow(tmp_path: Path) -> None:
    migration = importlib.import_module(
        "core.storage.migrations.versions.022_add_preview_effective_columns_and_coverage"
    )
    assert migration.revision == "022"
    assert migration.down_revision == "021"

    connection_string = f"sqlite:///{tmp_path / 'migration-022-down.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "021")
    before = _insert_ready_v4(connection_string)
    command.upgrade(config, "022")
    command.downgrade(config, "021")

    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)
    assert NEW_COLUMNS.isdisjoint(column["name"] for column in inspector.get_columns("preview_requests"))
    with engine.connect() as connection:
        after = tuple(
            connection.execute(text("SELECT * FROM preview_requests WHERE request_id = 'legacy-ready'")).one()
        )
    assert after == before
    engine.dispose()


def test_migration_022_matches_direct_create_all_preview_request_schema_before_expiry(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated-022.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct-022.db'}"
    command.upgrade(_alembic_config(migrated_url), "022")
    direct_backend = SQLModelBackend(direct_url, CCloudStorageModule(), use_migrations=False)
    direct_backend.create_tables()
    migrated = create_engine(migrated_url)
    direct = create_engine(direct_url)

    assert {column["name"] for column in sa_inspect(migrated).get_columns("preview_requests")} == {
        column["name"]
        for column in sa_inspect(direct).get_columns("preview_requests")
        if column["name"] not in {"expires_at", "worker_id", "lease_expires_at"}
    }

    migrated.dispose()
    direct.dispose()
    direct_backend.dispose()
