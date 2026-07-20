from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import IntegrityError
from sqlmodel import create_engine

if TYPE_CHECKING:
    from alembic.config import Config

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
    "tags",
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
        plugin_tables = {
            "ccloud_allocation_lineage_portions",
            "ccloud_allocation_lineage_runs",
            "ccloud_billing",
            "ccloud_cost_source_records",
        }

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


class TestMigration009EnvIdChargebackDimensions:
    """Verification items 6-7: Migration 009 adds/removes env_id column with backfill."""

    def _get_alembic_cfg(self, conn: str):
        import pathlib

        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", conn)
        return cfg

    def test_migration_009_upgrade_adds_env_id_column(self, tmp_path) -> None:
        """Item 6 (partial): Migration 009 upgrade adds env_id column to chargeback_dimensions."""
        from alembic import command
        from sqlalchemy import create_engine
        from sqlalchemy import inspect as sa_inspect

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "008")
        command.upgrade(cfg, "009")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("chargeback_dimensions")}
        engine.dispose()

        assert "env_id" in columns

    def test_migration_009_upgrade_ccloud_rows_backfilled_from_ccloud_billing(self, tmp_path) -> None:
        """Item 6: CCloud rows in chargeback_dimensions backfilled with env_id from ccloud_billing."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        # Upgrade to 008 (pre-env_id state)
        command.upgrade(cfg, "008")

        engine = create_engine(conn)
        with engine.connect() as c:
            # Seed a CCloud billing row
            c.execute(
                text("""
                INSERT INTO ccloud_billing
                    (ecosystem, tenant_id, timestamp, env_id, resource_id,
                     product_type, product_category, quantity, unit_price, total_cost, currency, granularity)
                VALUES
                    ('confluent_cloud', 't-1', '2026-01-01 00:00:00', 'env-abc', 'lkc-001',
                     'kafka', 'compute', '100', '0.01', '1.00', 'USD', 'daily')
            """)
            )
            # Seed a matching chargeback dimension row (CCloud)
            c.execute(
                text("""
                INSERT INTO chargeback_dimensions
                    (ecosystem, tenant_id, resource_id, product_category, product_type,
                     identity_id, cost_type, allocation_method, allocation_detail)
                VALUES
                    ('confluent_cloud', 't-1', 'lkc-001', 'compute', 'kafka',
                     'u-1', 'usage', 'direct', NULL)
            """)
            )
            # Seed a non-CCloud dimension row
            c.execute(
                text("""
                INSERT INTO chargeback_dimensions
                    (ecosystem, tenant_id, resource_id, product_category, product_type,
                     identity_id, cost_type, allocation_method, allocation_detail)
                VALUES
                    ('self_managed', 't-1', 'broker-1', 'compute', 'kafka',
                     'u-2', 'usage', 'direct', NULL)
            """)
            )
            c.commit()
        engine.dispose()

        # Run migration 009
        command.upgrade(cfg, "009")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT ecosystem, resource_id, env_id FROM chargeback_dimensions ORDER BY ecosystem")
            ).fetchall()
        engine.dispose()

        row_map = {r[1]: r[2] for r in rows}
        # CCloud row must be backfilled
        assert row_map.get("lkc-001") == "env-abc", f"CCloud row env_id mismatch: {row_map}"
        # Non-CCloud row must have empty string
        assert row_map.get("broker-1") == "", f"Non-CCloud row env_id must be empty: {row_map}"

    def test_migration_009_downgrade_removes_env_id_column(self, tmp_path) -> None:
        """Item 7: Migration 009 downgrade removes env_id column from chargeback_dimensions."""
        from alembic import command
        from sqlalchemy import create_engine
        from sqlalchemy import inspect as sa_inspect

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "009")
        command.downgrade(cfg, "008")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("chargeback_dimensions")}
        engine.dispose()

        assert "env_id" not in columns

    def test_migration_009_downgrade_restores_9_field_unique_constraint(self, tmp_path) -> None:
        """Item 7: Downgrade restores original 9-field unique constraint (no env_id)."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "009")
        command.downgrade(cfg, "008")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT sql FROM sqlite_master WHERE type='index' AND name='uq_chargeback_dimensions'")
            ).fetchall()
        engine.dispose()

        assert rows, "uq_chargeback_dimensions index not found after downgrade"
        index_sql = rows[0][0] or ""
        # env_id must NOT be in the 9-field constraint after downgrade
        assert "env_id" not in index_sql


class TestMigration014PipelineColumn:
    """Verification: Migration 014 adds pipeline column to emission_records."""

    def _get_alembic_cfg(self, conn: str):
        import pathlib

        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", conn)
        return cfg

    def test_migration_014_upgrade_adds_pipeline_column(self, tmp_path) -> None:
        """Migration 014 upgrade adds pipeline column to emission_records."""
        from alembic import command
        from sqlalchemy import create_engine

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "013")
        command.upgrade(cfg, "014")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("emission_records")}
        engine.dispose()

        assert "pipeline" in columns

    def test_migration_014_existing_rows_default_to_chargeback(self, tmp_path) -> None:
        """Migration 014 upgrade: existing emission_records rows get pipeline='chargeback'."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "013")

        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(
                text("""
                INSERT INTO emission_records
                    (ecosystem, tenant_id, emitter_name, date, status, attempt_count)
                VALUES
                    ('eco', 't1', 'csv', '2025-01-01', 'emitted', 1)
            """)
            )
            c.commit()
        engine.dispose()

        command.upgrade(cfg, "014")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(text("SELECT pipeline FROM emission_records")).fetchall()
        engine.dispose()

        assert rows, "Expected at least one row after migration"
        assert all(r[0] == "chargeback" for r in rows), "All pre-existing rows must default to pipeline='chargeback'"

    def test_migration_014_unique_constraint_includes_pipeline(self, tmp_path) -> None:
        """Migration 014 upgrade: new unique constraint includes pipeline column."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "014")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT sql FROM sqlite_master WHERE type='table' AND name='emission_records'")
            ).fetchall()
        engine.dispose()

        assert rows, "emission_records table not found after upgrade"
        table_sql = rows[0][0] or ""
        assert "pipeline" in table_sql, "Unique constraint must include pipeline column"


class TestMigration015RemoveAmountServerDefault:
    """Verification: Migration 015 removes server_default from topic_attribution_facts.amount."""

    def _get_alembic_cfg(self, conn: str) -> Config:
        import pathlib

        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        alembic_ini = migrations_dir / "alembic.ini"
        cfg = Config(str(alembic_ini))
        cfg.set_main_option("script_location", str(migrations_dir))
        cfg.set_main_option("sqlalchemy.url", conn)
        return cfg

    def test_migration_015_upgrade_removes_amount_server_default(self, tmp_path) -> None:
        """After upgrading to 015, INSERT into topic_attribution_facts without amount raises IntegrityError."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "014")
        command.upgrade(cfg, "015")

        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(
                text("""
                INSERT INTO topic_attribution_dimensions
                    (ecosystem, tenant_id, env_id, cluster_resource_id, topic_name,
                     product_category, product_type, attribution_method)
                VALUES
                    ('eco', 't-1', 'env-1', 'lkc-001', 'topic-a',
                     'compute', 'kafka', 'proportional')
            """)
            )
            c.commit()
            dim_id = c.execute(text("SELECT dimension_id FROM topic_attribution_dimensions LIMIT 1")).scalar()

        with engine.connect() as c, pytest.raises(IntegrityError):
            c.execute(
                text(
                    "INSERT INTO topic_attribution_facts"
                    " (timestamp, dimension_id) VALUES ('2026-01-01 00:00:00', :dim_id)"
                ).bindparams(dim_id=dim_id)
            )
            c.commit()

        engine.dispose()

    def test_migration_015_downgrade_restores_amount_server_default(self, tmp_path) -> None:
        """After downgrading from 015 to 014, INSERT without amount succeeds with amount=''."""
        from alembic import command
        from sqlalchemy import create_engine, text

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = self._get_alembic_cfg(conn)

        command.upgrade(cfg, "015")
        command.downgrade(cfg, "014")

        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(
                text("""
                INSERT INTO topic_attribution_dimensions
                    (ecosystem, tenant_id, env_id, cluster_resource_id, topic_name,
                     product_category, product_type, attribution_method)
                VALUES
                    ('eco', 't-1', 'env-1', 'lkc-001', 'topic-a',
                     'compute', 'kafka', 'proportional')
            """)
            )
            c.commit()
            dim_id = c.execute(text("SELECT dimension_id FROM topic_attribution_dimensions LIMIT 1")).scalar()
            c.execute(
                text(
                    "INSERT INTO topic_attribution_facts"
                    " (timestamp, dimension_id) VALUES ('2026-01-01 00:00:00', :dim_id)"
                ).bindparams(dim_id=dim_id)
            )
            c.commit()
            row = c.execute(text("SELECT amount FROM topic_attribution_facts LIMIT 1")).fetchone()

        engine.dispose()

        assert row is not None, "Row should have been inserted after downgrade restores server_default"
        assert row[0] == "", f"amount should be empty string from server_default, got {row[0]!r}"

    def test_migration_012_amount_column_has_no_server_default(self) -> None:
        """Migration 012 source must NOT have server_default on the amount column."""
        import pathlib

        migration_file = (
            pathlib.Path(__file__).resolve().parents[4]
            / "src"
            / "core"
            / "storage"
            / "migrations"
            / "versions"
            / "012_add_topic_attribution_tables.py"
        )
        content = migration_file.read_text()

        # Find the line defining the amount column
        amount_lines = [line for line in content.splitlines() if '"amount"' in line or "'amount'" in line]
        assert amount_lines, "Could not find amount column definition in migration 012"

        for line in amount_lines:
            assert "server_default" not in line, (
                f"Migration 012 amount column must not have server_default, but found: {line.strip()!r}"
            )


class TestMigration018CCloudCostSourceRecords:
    @staticmethod
    def _config(connection_string: str) -> Config:
        import pathlib

        from alembic.config import Config

        migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
        config = Config(str(migrations_dir / "alembic.ini"))
        config.set_main_option("script_location", str(migrations_dir))
        config.set_main_option("sqlalchemy.url", connection_string)
        return config

    def test_upgrade_creates_source_table_primary_key_and_indexes(self, tmp_path) -> None:
        from alembic import command
        from sqlalchemy import create_engine

        connection_string = f"sqlite:///{tmp_path / 'migration-018.db'}"
        config = self._config(connection_string)
        command.upgrade(config, "017")

        command.upgrade(config, "018")

        engine = create_engine(connection_string)
        inspector = sa_inspect(engine)
        assert "ccloud_cost_source_records" in inspector.get_table_names()
        columns = {column["name"]: column for column in inspector.get_columns("ccloud_cost_source_records")}
        assert set(columns) == {
            "ecosystem",
            "tenant_id",
            "source_record_id",
            "identity_scheme",
            "provider_cost_id",
            "source_period_start",
            "source_period_end",
            "collection_window_start",
            "collection_window_end",
            "evidence_scope_start",
            "evidence_scope_end",
            "allocation_timestamp",
            "retention_timestamp",
            "granularity",
            "product",
            "line_type",
            "amount",
            "original_amount",
            "discount_amount",
            "price",
            "quantity",
            "unit",
            "description",
            "network_access_type",
            "resource_id",
            "resource_name",
            "environment_id",
            "tier_dimensions_json",
            "malformed",
            "diagnostics_json",
            "raw_payload_json",
        }
        assert inspector.get_pk_constraint("ccloud_cost_source_records")["constrained_columns"] == [
            "ecosystem",
            "tenant_id",
            "source_record_id",
            "evidence_scope_start",
            "evidence_scope_end",
        ]
        indexes = {tuple(index["column_names"]) for index in inspector.get_indexes("ccloud_cost_source_records")}
        assert indexes == {
            ("ecosystem", "tenant_id", "allocation_timestamp"),
            ("ecosystem", "tenant_id", "retention_timestamp"),
            (
                "ecosystem",
                "tenant_id",
                "source_period_start",
                "evidence_scope_start",
                "evidence_scope_end",
            ),
        }
        engine.dispose()

    def test_upgrade_does_not_fabricate_source_rows_and_preserves_existing_data(self, tmp_path) -> None:
        from alembic import command
        from sqlalchemy import create_engine, text

        connection_string = f"sqlite:///{tmp_path / 'migration-018-preserve.db'}"
        config = self._config(connection_string)
        command.upgrade(config, "017")
        engine = create_engine(connection_string)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO ccloud_billing
                        (ecosystem, tenant_id, timestamp, env_id, resource_id,
                         product_type, product_category, quantity, unit_price,
                         total_cost, currency, granularity, allocation_attempts,
                         topic_attribution_attempts)
                    VALUES
                        ('confluent_cloud', 'org-1', '2026-07-01 00:00:00', 'env-1', 'lkc-1',
                         'KAFKA_NUM_CKU', 'KAFKA', '1', '2', '2', 'USD', 'daily', 0, 0)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_dimensions
                        (ecosystem, tenant_id, resource_id, product_category, product_type,
                         identity_id, cost_type, allocation_method, allocation_detail, env_id)
                    VALUES
                        ('confluent_cloud', 'org-1', 'lkc-1', 'KAFKA', 'KAFKA_NUM_CKU',
                         'user-1', 'usage', 'direct', NULL, 'env-1')
                    """
                )
            )
            dimension_id = connection.execute(
                text("SELECT dimension_id FROM chargeback_dimensions WHERE tenant_id = 'org-1'")
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO chargeback_facts (timestamp, dimension_id, amount, tags_json) "
                    "VALUES ('2026-07-01 00:00:00', :dimension_id, '2', '[]')"
                ),
                {"dimension_id": dimension_id},
            )
        command.upgrade(config, "018")

        with engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM ccloud_cost_source_records")).scalar_one() == 0
            assert connection.execute(text("SELECT total_cost FROM ccloud_billing")).scalar_one() == "2"
            assert connection.execute(text("SELECT amount FROM chargeback_facts")).scalar_one() == "2"
        engine.dispose()

    def test_downgrade_removes_only_source_table(self, tmp_path) -> None:
        from alembic import command
        from sqlalchemy import create_engine, text

        connection_string = f"sqlite:///{tmp_path / 'migration-018-downgrade.db'}"
        config = self._config(connection_string)
        command.upgrade(config, "017")
        engine = create_engine(connection_string)
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO ccloud_billing
                        (ecosystem, tenant_id, timestamp, env_id, resource_id,
                         product_type, product_category, quantity, unit_price,
                         total_cost, currency, granularity, allocation_attempts,
                         topic_attribution_attempts)
                    VALUES
                        ('confluent_cloud', 'org-1', '2026-07-01 00:00:00', 'env-1', 'lkc-1',
                         'KAFKA_NUM_CKU', 'KAFKA', '1', '2', '2', 'USD', 'daily', 0, 0)
                    """
                )
            )
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_dimensions
                        (ecosystem, tenant_id, resource_id, product_category, product_type,
                         identity_id, cost_type, allocation_method, allocation_detail, env_id)
                    VALUES
                        ('confluent_cloud', 'org-1', 'lkc-1', 'KAFKA', 'KAFKA_NUM_CKU',
                         'user-1', 'usage', 'direct', NULL, 'env-1')
                    """
                )
            )
            dimension_id = connection.execute(
                text("SELECT dimension_id FROM chargeback_dimensions WHERE tenant_id = 'org-1'")
            ).scalar_one()
            connection.execute(
                text(
                    "INSERT INTO chargeback_facts (timestamp, dimension_id, amount, tags_json) "
                    "VALUES ('2026-07-01 00:00:00', :dimension_id, '2', '[]')"
                ),
                {"dimension_id": dimension_id},
            )

        command.upgrade(config, "018")
        with engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM chargeback_dimensions")).scalar_one() == 1
            assert connection.execute(text("SELECT amount FROM chargeback_facts")).scalar_one() == "2"

        command.downgrade(config, "017")

        inspector = sa_inspect(engine)
        assert "ccloud_cost_source_records" not in inspector.get_table_names()
        with engine.connect() as connection:
            assert connection.execute(text("SELECT total_cost FROM ccloud_billing")).scalar_one() == "2"
            assert connection.execute(text("SELECT COUNT(*) FROM chargeback_dimensions")).scalar_one() == 1
            assert connection.execute(text("SELECT amount FROM chargeback_facts")).scalar_one() == "2"
        engine.dispose()
