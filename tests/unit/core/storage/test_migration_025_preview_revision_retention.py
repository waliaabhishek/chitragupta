from __future__ import annotations

from importlib import import_module
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine, inspect, text

from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config

INDEXES = {
    "ix_preview_revisions_owner_month_visible_history",
    "ix_preview_revisions_owner_retention_due",
    "ix_preview_revisions_owner_retention_pending",
}


def test_migration_025_chain_and_upgrade_downgrade(tmp_path: Path) -> None:
    migration = import_module("core.storage.migrations.versions.025_add_preview_revision_history_retention")
    assert migration.revision == "025"
    assert migration.down_revision == "024"

    url = f"sqlite:///{tmp_path / 'migration.db'}"
    config = _alembic_config(url)
    command.upgrade(config, "025")
    engine = create_engine(url)
    schema = inspect(engine)
    assert "retention_pending_at" in {column["name"] for column in schema.get_columns("preview_revisions")}
    index_names = {index["name"] for index in schema.get_indexes("preview_revisions")}
    assert index_names >= INDEXES
    assert "ux_preview_revisions_owner_month_current" in index_names

    command.downgrade(config, "024")
    schema = inspect(engine)
    assert "retention_pending_at" not in {column["name"] for column in schema.get_columns("preview_revisions")}
    assert INDEXES.isdisjoint({index["name"] for index in schema.get_indexes("preview_revisions")})
    assert "ux_preview_revisions_owner_month_current" in {
        index["name"] for index in schema.get_indexes("preview_revisions")
    }
    engine.dispose()


def test_migration_025_schema_matches_create_all(tmp_path: Path) -> None:
    backend_module = import_module("core.storage.backends.sqlmodel.unit_of_work")
    storage_module = import_module("plugins.confluent_cloud.storage.module")
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct.db'}"

    command.upgrade(_alembic_config(migrated_url), "025")
    direct_backend = backend_module.SQLModelBackend(
        direct_url,
        storage_module.CCloudStorageModule(),
        use_migrations=False,
    )
    direct_backend.create_tables()
    migrated = create_engine(migrated_url)
    direct = create_engine(direct_url)

    assert {column["name"] for column in inspect(migrated).get_columns("preview_revisions")} == {
        column["name"] for column in inspect(direct).get_columns("preview_revisions")
    }
    assert {index["name"] for index in inspect(migrated).get_indexes("preview_revisions")} == {
        index["name"] for index in inspect(direct).get_indexes("preview_revisions")
    }

    migrated.dispose()
    direct.dispose()
    direct_backend.dispose()


def test_history_index_supports_bounded_owner_month_order_without_temp_sort(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'query-plan.db'}"
    command.upgrade(_alembic_config(url), "025")
    engine = create_engine(url)
    with engine.connect() as connection:
        plan = connection.execute(
            text(
                "EXPLAIN QUERY PLAN SELECT * FROM preview_revisions "
                "WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1' "
                "AND month_start = '2026-07-01' AND retention_pending_at IS NULL "
                "ORDER BY published_at DESC, revision_id DESC LIMIT 101"
            )
        ).all()

    rendered = " ".join(str(row) for row in plan)
    assert "ix_preview_revisions_owner_month_visible_history" in rendered
    assert "USE TEMP B-TREE" not in rendered
    engine.dispose()


def test_retention_indexes_support_due_pending_and_tail_queries(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'retention-plans.db'}"
    command.upgrade(_alembic_config(url), "025")
    engine = create_engine(url)
    queries = [
        (
            "ix_preview_revisions_owner_retention_due",
            "SELECT revision_id FROM preview_revisions WHERE ecosystem='confluent_cloud' "
            "AND tenant_id='tenant-1' AND retention_pending_at IS NULL "
            "AND month_end <= '2026-08-01' "
            "ORDER BY month_end, published_at, revision_id LIMIT 100",
        ),
        (
            "ix_preview_revisions_owner_retention_pending",
            "SELECT revision_id FROM preview_revisions WHERE ecosystem='confluent_cloud' "
            "AND tenant_id='tenant-1' AND retention_pending_at IS NOT NULL "
            "ORDER BY retention_pending_at, revision_id LIMIT 100",
        ),
        (
            "ix_preview_revisions_owner_retention_pending",
            "SELECT retention_pending_at FROM preview_revisions "
            "WHERE ecosystem='confluent_cloud' AND tenant_id='tenant-1' "
            "AND retention_pending_at IS NOT NULL "
            "ORDER BY retention_pending_at DESC, revision_id DESC LIMIT 1",
        ),
    ]
    with engine.connect() as connection:
        for index_name, sql in queries:
            plan = connection.execute(text(f"EXPLAIN QUERY PLAN {sql}")).all()
            rendered = " ".join(str(row) for row in plan)
            assert index_name in rendered
            assert "USE TEMP B-TREE" not in rendered
    engine.dispose()
