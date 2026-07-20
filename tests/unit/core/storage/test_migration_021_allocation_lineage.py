from __future__ import annotations

import importlib
import inspect
from pathlib import Path

from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config

_ASSOCIATION_COLUMNS = {
    "billing_timestamp",
    "billing_env_id",
    "billing_resource_id",
    "billing_product_type",
    "billing_product_category",
}


def _insert_legacy_source(connection_string: str) -> None:
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ccloud_cost_source_records (
                    ecosystem, tenant_id, source_record_id, identity_scheme,
                    provider_cost_id, source_period_start, source_period_end,
                    collection_window_start, collection_window_end,
                    evidence_scope_start, evidence_scope_end, allocation_timestamp,
                    retention_timestamp, granularity, product, line_type, amount,
                    original_amount, discount_amount, price, quantity, unit,
                    description, network_access_type, resource_id, resource_name,
                    environment_id, tier_dimensions_json, malformed,
                    diagnostics_json, raw_payload_json
                ) VALUES (
                    'confluent_cloud', 'org-1', 'provider:legacy',
                    'provider_cost_id', 'legacy', '2026-07-01 00:00:00',
                    '2026-07-02 00:00:00', '2026-07-01 00:00:00',
                    '2026-07-02 00:00:00', '2026-07-01 00:00:00',
                    '2026-07-02 00:00:00', '2026-07-01 00:00:00',
                    '2026-07-01 00:00:00', 'DAILY', 'KAFKA', 'KAFKA_STORAGE',
                    '8', '10', '2', '2', '5', 'GB', 'legacy', 'PUBLIC_INTERNET',
                    NULL, NULL, NULL, '{}', 0, '[]', '{}'
                )
                """
            )
        )
    engine.dispose()


def test_migration_021_preserves_legacy_source_with_nullable_unbackfilled_association(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-021.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "020")
    _insert_legacy_source(connection_string)

    command.upgrade(config, "021")

    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)
    source_columns = {column["name"]: column for column in inspector.get_columns("ccloud_cost_source_records")}
    assert source_columns.keys() >= _ASSOCIATION_COLUMNS
    assert all(source_columns[name]["nullable"] is True for name in _ASSOCIATION_COLUMNS)
    with engine.connect() as connection:
        legacy = connection.execute(
            text(
                "SELECT billing_timestamp, billing_env_id, billing_resource_id, "
                "billing_product_type, billing_product_category "
                "FROM ccloud_cost_source_records WHERE source_record_id = 'provider:legacy'"
            )
        ).one()
    assert tuple(legacy) == (None, None, None, None, None)
    engine.dispose()


def test_migration_021_creates_closed_run_and_exact_portion_schema(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-021-schema.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "021")
    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)

    assert {"ccloud_allocation_lineage_runs", "ccloud_allocation_lineage_portions"} <= set(inspector.get_table_names())
    run_columns = {column["name"] for column in inspector.get_columns("ccloud_allocation_lineage_runs")}
    assert run_columns == {
        "ecosystem",
        "tenant_id",
        "tracking_date",
        "calculation_id",
        "calculation_completed_at",
        "capture_status",
        "capture_reason",
        "portion_count",
    }
    portion_columns = {column["name"] for column in inspector.get_columns("ccloud_allocation_lineage_portions")}
    assert portion_columns == {
        "ecosystem",
        "tenant_id",
        "tracking_date",
        "calculation_id",
        "origin_timestamp",
        "origin_env_id",
        "origin_resource_id",
        "origin_product_type",
        "origin_product_category",
        "portion_ordinal",
        "target_kind",
        "target_id",
        "allocated_cost",
        "allocated_quantity",
        "allocation_ratio",
        "method_id",
        "method_version",
        "method_details_json",
    }
    assert inspector.get_pk_constraint("ccloud_allocation_lineage_runs")["constrained_columns"] == [
        "ecosystem",
        "tenant_id",
        "tracking_date",
    ]
    assert inspector.get_pk_constraint("ccloud_allocation_lineage_portions")["constrained_columns"] == [
        "ecosystem",
        "tenant_id",
        "tracking_date",
        "calculation_id",
        "origin_timestamp",
        "origin_env_id",
        "origin_resource_id",
        "origin_product_type",
        "origin_product_category",
        "portion_ordinal",
    ]
    assert any(
        index["column_names"] == ["tenant_id", "calculation_id", "tracking_date"]
        for index in inspector.get_indexes("ccloud_allocation_lineage_portions")
    )
    engine.dispose()


def test_migration_021_contains_no_association_backfill_or_chargeback_rewrite() -> None:
    migration = importlib.import_module("core.storage.migrations.versions.021_add_ccloud_allocation_lineage")
    normalized = " ".join(inspect.getsource(migration.upgrade).casefold().split())

    assert "update ccloud_cost_source_records" not in normalized
    assert "insert into ccloud_allocation_lineage" not in normalized
    assert "update chargeback" not in normalized
    assert "delete from chargeback" not in normalized


def test_migration_021_downgrade_removes_only_lineage_schema_and_preserves_legacy_source(tmp_path: Path) -> None:
    connection_string = f"sqlite:///{tmp_path / 'migration-021-downgrade.db'}"
    config = _alembic_config(connection_string)
    command.upgrade(config, "020")
    _insert_legacy_source(connection_string)
    command.upgrade(config, "021")

    command.downgrade(config, "020")

    engine = create_engine(connection_string)
    inspector = sa_inspect(engine)
    assert "ccloud_allocation_lineage_runs" not in inspector.get_table_names()
    assert "ccloud_allocation_lineage_portions" not in inspector.get_table_names()
    assert _ASSOCIATION_COLUMNS.isdisjoint(
        column["name"] for column in inspector.get_columns("ccloud_cost_source_records")
    )
    with engine.connect() as connection:
        preserved = connection.execute(
            text(
                "SELECT source_record_id, amount, resource_id, environment_id "
                "FROM ccloud_cost_source_records WHERE source_record_id = 'provider:legacy'"
            )
        ).one()
    assert tuple(preserved) == ("provider:legacy", "8", None, None)
    engine.dispose()


def test_migration_021_matches_direct_create_all_for_owned_tables(tmp_path: Path) -> None:
    migration_connection = f"sqlite:///{tmp_path / 'migration-021-parity.db'}"
    direct_connection = f"sqlite:///{tmp_path / 'direct-021-parity.db'}"
    command.upgrade(_alembic_config(migration_connection), "021")
    direct_backend = SQLModelBackend(direct_connection, CCloudStorageModule(), use_migrations=False)
    direct_backend.create_tables()
    migrated = create_engine(migration_connection)
    direct = create_engine(direct_connection)
    migrated_inspector = sa_inspect(migrated)
    direct_inspector = sa_inspect(direct)

    for table in (
        "ccloud_cost_source_records",
        "ccloud_allocation_lineage_runs",
        "ccloud_allocation_lineage_portions",
    ):
        assert {column["name"] for column in migrated_inspector.get_columns(table)} == {
            column["name"] for column in direct_inspector.get_columns(table)
        }
        assert {index["name"] for index in migrated_inspector.get_indexes(table)} == {
            index["name"] for index in direct_inspector.get_indexes(table)
        }
    migrated.dispose()
    direct.dispose()
    direct_backend.dispose()
