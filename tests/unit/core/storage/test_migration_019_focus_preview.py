from __future__ import annotations

import importlib
import inspect
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule


def _alembic_config(connection_string: str) -> Config:
    migrations_dir = Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
    config = Config(str(migrations_dir / "alembic.ini"))
    config.set_main_option("script_location", str(migrations_dir))
    config.set_main_option("sqlalchemy.url", connection_string)
    return config


def _seed_legacy_rows(connection_string: str) -> dict[str, list[tuple[object, ...]]]:
    engine = create_engine(connection_string)
    statements = {
        "pipeline_state": """
            INSERT INTO pipeline_state (
                ecosystem, tenant_id, tracking_date, billing_gathered,
                resources_gathered, chargeback_calculated,
                topic_overlay_gathered, topic_attribution_calculated
            ) VALUES ('confluent_cloud', 'tenant-1', '2026-07-01', 1, 1, 1, 1, 1)
        """,
        "ccloud_billing": """
            INSERT INTO ccloud_billing (
                ecosystem, tenant_id, timestamp, env_id, resource_id,
                product_type, product_category, quantity, unit_price,
                total_cost, currency, granularity, allocation_attempts,
                topic_attribution_attempts, metadata_json
            ) VALUES (
                'confluent_cloud', 'tenant-1', '2026-07-01 00:00:00',
                'env-1', 'lkc-1', 'KAFKA_STORAGE', 'KAFKA', '5', '2',
                '8', 'USD', 'daily', 2, 3, '{}'
            )
        """,
        "chargeback_dimensions": """
            INSERT INTO chargeback_dimensions (
                dimension_id, ecosystem, tenant_id, resource_id,
                product_category, product_type, identity_id, cost_type,
                allocation_method, allocation_detail, env_id
            ) VALUES (
                41, 'confluent_cloud', 'tenant-1', 'lkc-1', 'KAFKA',
                'KAFKA_STORAGE', 'sa-1', 'usage', 'direct', NULL, 'env-1'
            )
        """,
        "chargeback_facts": """
            INSERT INTO chargeback_facts (timestamp, dimension_id, amount, tags_json)
            VALUES ('2026-07-01 00:00:00', 41, '8', '[]')
        """,
        "topic_attribution_dimensions": """
            INSERT INTO topic_attribution_dimensions (
                dimension_id, ecosystem, tenant_id, env_id,
                cluster_resource_id, topic_name, resource_id,
                product_category, product_type, attribution_method
            ) VALUES (
                51, 'confluent_cloud', 'tenant-1', 'env-1', 'lkc-1',
                'orders', 'lkc-1:topic:orders', 'KAFKA', 'KAFKA_STORAGE',
                'bytes_ratio'
            )
        """,
        "topic_attribution_facts": """
            INSERT INTO topic_attribution_facts (timestamp, dimension_id, amount)
            VALUES ('2026-07-01 00:00:00', 51, '8')
        """,
    }
    with engine.begin() as connection:
        for statement in statements.values():
            connection.execute(text(statement))
    snapshots = _snapshots(engine)
    engine.dispose()
    return snapshots


def _snapshots(engine: object) -> dict[str, list[tuple[object, ...]]]:
    tables = (
        "pipeline_state",
        "ccloud_billing",
        "chargeback_dimensions",
        "chargeback_facts",
        "topic_attribution_dimensions",
        "topic_attribution_facts",
    )
    with engine.connect() as connection:  # type: ignore[union-attr]
        return {
            table: [tuple(row) for row in connection.execute(text(f"SELECT * FROM {table} ORDER BY 1")).fetchall()]
            for table in tables
        }


def test_migration_019_adds_nullable_correlation_preview_table_indexes_and_fk(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-019.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "018")
    _seed_legacy_rows(connection_string)

    command.upgrade(config, "019")

    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)
    pipeline_columns = {column["name"]: column for column in inspector.get_columns("pipeline_state")}
    assert pipeline_columns["calculation_id"]["nullable"] is True
    assert pipeline_columns["calculation_completed_at"]["nullable"] is True
    assert pipeline_columns["calculation_run_id"]["nullable"] is True
    assert "preview_requests" in inspector.get_table_names()
    assert "ix_pipeline_state_preview_coverage" in {index["name"] for index in inspector.get_indexes("pipeline_state")}
    assert any(
        fk["referred_table"] == "pipeline_runs" and fk["constrained_columns"] == ["calculation_run_id"]
        for fk in inspector.get_foreign_keys("pipeline_state")
    )
    with engine.connect() as connection:
        legacy = connection.execute(
            text(
                "SELECT calculation_id, calculation_completed_at, calculation_run_id "
                "FROM pipeline_state WHERE tracking_date = '2026-07-01'"
            )
        ).one()
    assert tuple(legacy) == (None, None, None)
    engine.dispose()


def test_migration_019_upgrade_and_downgrade_preserve_legacy_facts_flags_and_retry_counters(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-019-preserve.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "018")
    before = _seed_legacy_rows(connection_string)

    command.upgrade(config, "019")
    engine = create_engine(connection_string)
    after_upgrade = _snapshots(engine)
    assert after_upgrade["pipeline_state"][:1] == [before["pipeline_state"][0] + (None, None, None)]
    assert after_upgrade | {"pipeline_state": before["pipeline_state"]} == before

    command.downgrade(config, "018")
    after_downgrade = _snapshots(engine)
    assert after_downgrade == before
    engine.dispose()


def test_migration_019_contains_no_data_repair_dml() -> None:
    migration = importlib.import_module(
        "core.storage.migrations.versions.019_add_focus_preview_and_calculation_identity"
    )
    source = inspect.getsource(migration)
    normalized = " ".join(source.lower().split())

    assert "update pipeline_state" not in normalized
    assert "delete from pipeline_state" not in normalized
    assert "delete from chargeback" not in normalized
    assert "delete from topic_attribution" not in normalized


def test_migration_019_matches_create_all_schema(tmp_path: Path) -> None:
    migration_connection = f"sqlite:///{tmp_path / 'migration.db'}"
    direct_connection = f"sqlite:///{tmp_path / 'direct.db'}"
    config = _alembic_config(migration_connection)
    command.upgrade(config, "019")

    direct_backend = SQLModelBackend(direct_connection, CCloudStorageModule(), use_migrations=False)
    direct_backend.create_tables()
    migrated = create_engine(migration_connection)
    direct = create_engine(direct_connection)
    migrated_inspector = sa_inspect(migrated)
    direct_inspector = sa_inspect(direct)

    assert set(migrated_inspector.get_table_names()) - {"alembic_version"} == set(direct_inspector.get_table_names())
    for table in ("pipeline_state", "preview_requests"):
        assert {column["name"] for column in migrated_inspector.get_columns(table)} == {
            column["name"] for column in direct_inspector.get_columns(table)
        }

    migrated.dispose()
    direct.dispose()
    direct_backend.dispose()
