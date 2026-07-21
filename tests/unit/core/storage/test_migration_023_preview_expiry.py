from __future__ import annotations

import importlib
import io
from pathlib import Path

from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config


def _seed_lifecycle_rows(connection_string: str) -> None:
    engine = create_engine(connection_string)
    rows = [
        ("queued", None),
        ("running", None),
        ("failed", "2026-07-03 00:03:00"),
        ("ready", "2026-07-03 00:04:00.123456"),
        ("expired", "2026-07-03 00:05:00.654321"),
    ]
    with engine.begin() as connection:
        for index, (status, completed_at) in enumerate(rows):
            connection.execute(
                text(
                    """
                    INSERT INTO preview_requests (
                        request_id, tenant_name, ecosystem, tenant_id, grain,
                        start_date, end_date, column_profile, status, created_at,
                        started_at, completed_at, effective_columns_json
                    ) VALUES (
                        :request_id, 'production', 'confluent_cloud', 'tenant-1', 'daily',
                        '2026-07-01', '2026-07-02', 'full', :status, :created_at,
                        :started_at, :completed_at, '[]'
                    )
                    """
                ),
                {
                    "request_id": f"request-{status}",
                    "status": status,
                    "created_at": f"2026-07-03 00:00:0{index}",
                    "started_at": None if status == "queued" else "2026-07-03 00:01:00",
                    "completed_at": completed_at,
                },
            )
    engine.dispose()


def test_migration_023_backfills_only_ready_and_expired_from_completed_at(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-023.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "022")
    _seed_lifecycle_rows(connection_string)

    command.upgrade(config, "023")

    engine = create_engine(connection_string)
    inspector = inspect(engine)
    columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
    assert columns["expires_at"]["nullable"] is True
    assert "ix_preview_requests_owner_expiry" in {index["name"] for index in inspector.get_indexes("preview_requests")}
    with engine.connect() as connection:
        values = dict(connection.execute(text("SELECT status, expires_at FROM preview_requests ORDER BY status")).all())
    assert values["queued"] is None
    assert values["running"] is None
    assert values["failed"] is None
    assert str(values["ready"]) == "2026-07-10 00:04:00.123456"
    assert str(values["expired"]) == "2026-07-10 00:05:00.654321"
    engine.dispose()


def test_migration_023_upgrade_compiles_a_postgresql_compatible_backfill() -> None:
    migration = importlib.import_module("core.storage.migrations.versions.023_add_preview_request_expiry")
    output = io.StringIO()
    context = MigrationContext.configure(
        url="postgresql://",
        opts={"as_sql": True, "output_buffer": output},
    )

    with Operations(context).context(context):
        migration.upgrade()

    sql = output.getvalue()
    assert "UPDATE preview_requests" in sql
    assert "expires_at" in sql
    assert "completed_at" in sql
    assert "datetime(" not in sql.casefold()


def test_migration_023_backfills_with_portable_bounded_executemany_batches(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-023-batches.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "022")
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests (
                    request_id, tenant_name, ecosystem, tenant_id, grain,
                    start_date, end_date, column_profile, status, created_at,
                    started_at, completed_at, effective_columns_json
                ) VALUES (
                    :request_id, 'production', 'confluent_cloud', 'tenant-1', 'daily',
                    '2026-07-01', '2026-07-02', 'full', 'ready', '2026-07-03 00:00:00',
                    '2026-07-03 00:01:00', :completed_at, '[]'
                )
                """
            ),
            [
                {
                    "request_id": f"request-{index:04d}",
                    "completed_at": f"2026-07-03 00:{index % 60:02d}:00.123456",
                }
                for index in range(1001)
            ],
        )

    batches: list[tuple[bool, int]] = []

    def capture_backfill(
        _connection: object,
        _cursor: object,
        statement: str,
        parameters: object,
        _context: object,
        executemany: bool,
    ) -> None:
        if statement.lstrip().upper().startswith("UPDATE PREVIEW_REQUESTS") and "expires_at" in statement:
            size = len(parameters) if executemany and isinstance(parameters, list) else 1
            batches.append((executemany, size))

    event.listen(Engine, "before_cursor_execute", capture_backfill)
    try:
        command.upgrade(config, "023")
    finally:
        event.remove(Engine, "before_cursor_execute", capture_backfill)

    assert batches == [(True, 400), (True, 400), (True, 201)]
    with engine.connect() as connection:
        assert (
            connection.execute(text("SELECT COUNT(*) FROM preview_requests WHERE expires_at IS NOT NULL")).scalar_one()
            == 1001
        )
    engine.dispose()


def test_migration_023_revision_chain_and_downgrade_preserve_preexisting_data(tmp_path: Path) -> None:
    migration = importlib.import_module("core.storage.migrations.versions.023_add_preview_request_expiry")
    assert migration.revision == "023"
    assert migration.down_revision == "022"
    connection_string = f"sqlite:///{tmp_path / 'migration-023-down.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "022")
    _seed_lifecycle_rows(connection_string)

    command.upgrade(config, "023")
    command.downgrade(config, "022")

    engine = create_engine(connection_string)
    inspector = inspect(engine)
    assert "expires_at" not in {column["name"] for column in inspector.get_columns("preview_requests")}
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM preview_requests")).scalar_one() == 5
    engine.dispose()


def test_migration_023_matches_direct_create_all_schema(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated-023.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct-023.db'}"
    command.upgrade(_alembic_config(migrated_url), "023")
    direct_backend = SQLModelBackend(direct_url, CCloudStorageModule(), use_migrations=False)
    direct_backend.create_tables()
    migrated = create_engine(migrated_url)
    direct = create_engine(direct_url)

    assert {column["name"] for column in inspect(migrated).get_columns("preview_requests")} == {
        column["name"] for column in inspect(direct).get_columns("preview_requests")
    }
    assert "ix_preview_requests_owner_expiry" in {
        index["name"] for index in inspect(direct).get_indexes("preview_requests")
    }

    migrated.dispose()
    direct.dispose()
    direct_backend.dispose()
