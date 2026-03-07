from __future__ import annotations

import logging

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlmodel import create_engine

from core.storage.backends.sqlmodel.engine import _engine_lock, _engines
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


@pytest.fixture(autouse=True)
def reset_alembic_logging():
    """Reset logging after each test to prevent alembic from polluting other tests.

    Alembic's command.upgrade/downgrade calls logging.config.fileConfig() which
    reconfigures the root logger and sets disable_existing_loggers=True.
    This disables all existing loggers including core.storage.*, breaking
    pytest's caplog fixture for subsequent tests.
    """
    # Capture state before
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level

    # Capture disabled state of loggers we care about
    storage_logger = logging.getLogger("core.storage.backends.sqlmodel.repositories")
    original_disabled = storage_logger.disabled

    yield

    # Restore root logger state
    root.handlers = original_handlers
    root.setLevel(original_level)

    # Re-enable loggers that alembic disabled
    storage_logger.disabled = original_disabled

    # Clear alembic's loggers
    for name in ["alembic", "alembic.runtime.migration"]:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def clean_engine_cache():
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


EXPECTED_TABLES = {
    "resources",
    "identities",
    "billing",
    "chargeback_dimensions",
    "chargeback_facts",
    "pipeline_state",
    "custom_tags",
}


class TestBaselineMigration:
    def test_upgrade_creates_all_tables(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=True)
        backend.create_tables()

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        table_names = set(inspector.get_table_names())
        assert table_names >= EXPECTED_TABLES
        assert "alembic_version" in table_names
        engine.dispose()
        backend.dispose()

    def test_upgrade_then_downgrade(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"

        import pathlib

        from alembic import command
        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"

        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", conn)

        # Upgrade
        command.upgrade(cfg, "head")
        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        assert set(inspector.get_table_names()) >= EXPECTED_TABLES

        # Downgrade
        command.downgrade(cfg, "base")
        inspector = sa_inspect(engine)
        remaining = set(inspector.get_table_names()) - {"alembic_version"}
        assert remaining == set()
        engine.dispose()

    def test_migration_schema_matches_create_all(self, tmp_path):
        """Verify that migration-created schema has the same tables as create_all."""
        # Migration path
        db_migrate = tmp_path / "migrate.db"
        conn_migrate = f"sqlite:///{db_migrate}"
        backend_m = SQLModelBackend(conn_migrate, CoreStorageModule(), use_migrations=True)
        backend_m.create_tables()

        # create_all path
        db_direct = tmp_path / "direct.db"
        conn_direct = f"sqlite:///{db_direct}"
        backend_d = SQLModelBackend(conn_direct, CoreStorageModule(), use_migrations=False)
        backend_d.create_tables()

        engine_m = create_engine(conn_migrate)
        engine_d = create_engine(conn_direct)
        inspector_m = sa_inspect(engine_m)
        inspector_d = sa_inspect(engine_d)

        # Plugin-specific tables created by migrations but not by CoreStorageModule
        plugin_tables = {"ccloud_billing"}

        tables_m = set(inspector_m.get_table_names()) - {"alembic_version"} - plugin_tables
        tables_d = set(inspector_d.get_table_names())

        assert tables_m == tables_d

        # Check column names match for each table
        for table in EXPECTED_TABLES:
            cols_m = {c["name"] for c in inspector_m.get_columns(table)}
            cols_d = {c["name"] for c in inspector_d.get_columns(table)}
            assert cols_m == cols_d, f"Column mismatch in {table}: migration={cols_m}, create_all={cols_d}"

        engine_m.dispose()
        engine_d.dispose()
        backend_m.dispose()
        backend_d.dispose()


class TestMigration005BillingPK:
    def _get_alembic_cfg(self, conn: str):
        import pathlib

        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", conn)
        return cfg

    def test_migration_005_upgrade_adds_product_category_to_pk(self, tmp_path) -> None:
        """Migration 005 upgrade promotes product_category to PK (pk > 0 in PRAGMA table_info)."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "004")
        command.upgrade(cfg, "005")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(text("PRAGMA table_info(billing)")).fetchall()
        engine.dispose()

        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        col_pk_map = {row[1]: row[5] for row in rows}
        assert col_pk_map["product_category"] > 0, (
            f"product_category must be part of primary key after migration 005 upgrade, "
            f"but pk={col_pk_map['product_category']!r}"
        )

    def test_migration_005_downgrade_restores_5_field_pk(self, tmp_path) -> None:
        """Migration 005 downgrade restores 5-field PK (product_category no longer in PK)."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "005")
        command.downgrade(cfg, "004")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(text("PRAGMA table_info(billing)")).fetchall()
        engine.dispose()

        col_pk_map = {row[1]: row[5] for row in rows}
        assert col_pk_map["product_category"] == 0, (
            f"product_category must NOT be part of primary key after migration 005 downgrade, "
            f"but pk={col_pk_map['product_category']!r}"
        )
