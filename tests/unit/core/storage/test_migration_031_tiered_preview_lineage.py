from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

if TYPE_CHECKING:
    import pytest

SIDECAR_TABLE = "ccloud_preview_source_allocation_lineage_portions"
EXPECTED_COLUMNS = (
    "ecosystem",
    "tenant_id",
    "tracking_date",
    "calculation_id",
    "source_record_id",
    "evidence_scope_start",
    "evidence_scope_end",
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
    "allocated_original_cost",
    "allocation_ratio",
    "method_id",
    "method_version",
    "method_details_json",
)
EXPECTED_PRIMARY_KEY = (
    "ecosystem",
    "tenant_id",
    "tracking_date",
    "calculation_id",
    "source_record_id",
    "evidence_scope_start",
    "evidence_scope_end",
    "portion_ordinal",
)


def _shape(url: str) -> tuple[tuple[str, ...], tuple[str, ...], set[tuple[str, ...]]]:
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        columns = tuple(column["name"] for column in inspector.get_columns(SIDECAR_TABLE))
        primary_key = tuple(inspector.get_pk_constraint(SIDECAR_TABLE)["constrained_columns"])
        indexes = {tuple(index["column_names"]) for index in inspector.get_indexes(SIDECAR_TABLE)}
        return columns, primary_key, indexes
    finally:
        engine.dispose()


def test_revision_031_is_head_and_uses_guarded_preview_hook() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "031_add_tiered_preview_source_lineage.py"
    )

    assert script.get_current_head() == "031"
    source = migration_path.read_text(encoding="utf-8")
    assert 'run_preview_evidence_step("031")' in source
    assert 'run_preview_evidence_downgrade_step("031")' in source


def test_enabled_upgrade_030_to_031_adds_exact_source_sidecar_without_rewriting_existing_data(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'upgrade.db'}"
    config = _config(url, selection="confluent_cloud")
    command.upgrade(config, "030")
    engine = create_engine(url)
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
                    ('confluent_cloud', 'tenant-1', '2026-07-01 00:00:00',
                     'env-1', 'lkc-1', 'KAFKA_STORAGE', 'KAFKA', '5', '0',
                     '8', 'USD', 'daily', 0, 0)
                """
            )
        )
    engine.dispose()

    command.upgrade(config, "031")

    assert _shape(url) == (
        EXPECTED_COLUMNS,
        EXPECTED_PRIMARY_KEY,
        {("tenant_id", "calculation_id", "tracking_date")},
    )
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT total_cost FROM ccloud_billing")).scalar_one() == "8"
            assert connection.execute(text(f"SELECT COUNT(*) FROM {SIDECAR_TABLE}")).scalar_one() == 0
    finally:
        engine.dispose()


def test_disabled_upgrade_does_not_create_optional_tiered_preview_sidecar(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'disabled.db'}"

    command.upgrade(_config(url, selection="disabled"), "head")

    engine = create_engine(url)
    try:
        assert SIDECAR_TABLE not in inspect(engine).get_table_names()
    finally:
        engine.dispose()


def test_downgrade_031_removes_only_sidecar_and_preserves_030_preview_tables(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    config = _config(url, selection="confluent_cloud")
    command.upgrade(config, "031")

    command.downgrade(config, "030")

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert SIDECAR_TABLE not in tables
    assert {
        "ccloud_allocation_lineage_runs",
        "ccloud_allocation_lineage_portions",
        "ccloud_cost_source_records",
        "ccloud_focus_preview_repair_heads",
    } <= tables


def test_create_all_and_migrated_sidecar_schemas_match(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url, selection="confluent_cloud"), "031")
    created_url = f"sqlite:///{tmp_path / 'created.db'}"
    backend = SQLModelBackend(
        created_url,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    try:
        backend.create_tables()
        assert _shape(created_url) == _shape(migrated_url)
    finally:
        backend.dispose()


def test_runtime_preview_preparation_targets_revision_031(
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

    assert calls == ["031"]
