"""Migration coverage for the nullable principal-team fact snapshot."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

if TYPE_CHECKING:
    from pytest import MonkeyPatch


def test_revision_034_is_the_current_head_and_declares_its_predecessor() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "034_add_self_managed_principal_team.py"
    )
    source = migration_path.read_text(encoding="utf-8")

    assert script.get_current_head() == "034"
    assert 'revision = "034"' in source
    assert 'down_revision = "033"' in source


def test_upgrade_adds_a_nullable_principal_team_column_and_downgrade_removes_only_that_column(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'principal-team.db'}"
    config = _config(url, selection="disabled")

    command.upgrade(config, "033")
    engine = create_engine(url)
    try:
        before = {column["name"] for column in inspect(engine).get_columns("chargeback_facts")}
    finally:
        engine.dispose()

    command.upgrade(config, "034")
    engine = create_engine(url)
    try:
        columns = {column["name"]: column for column in inspect(engine).get_columns("chargeback_facts")}
    finally:
        engine.dispose()

    assert set(columns) == before | {"principal_team"}
    assert columns["principal_team"]["nullable"] is True

    command.downgrade(config, "033")
    engine = create_engine(url)
    try:
        after = {column["name"] for column in inspect(engine).get_columns("chargeback_facts")}
    finally:
        engine.dispose()

    assert after == before


def test_direct_sqlmodel_schema_matches_the_migrated_principal_team_column(tmp_path: Path) -> None:
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    direct_url = f"sqlite:///{tmp_path / 'direct.db'}"

    command.upgrade(_config(migrated_url, selection="disabled"), "034")
    backend = SQLModelBackend(direct_url, SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    migrated_engine = create_engine(migrated_url)
    direct_engine = create_engine(direct_url)
    try:
        migrated_columns = {column["name"] for column in inspect(migrated_engine).get_columns("chargeback_facts")}
        direct_columns = {column["name"] for column in inspect(direct_engine).get_columns("chargeback_facts")}
    finally:
        migrated_engine.dispose()
        direct_engine.dispose()
        backend.dispose()

    assert migrated_columns == direct_columns


def test_pre_034_database_remains_readable_and_writable_when_migrations_are_disabled(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    command.upgrade(_config(url, selection="disabled"), "033")
    backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    row = ChargebackRow(
        ecosystem="arbitrary",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="resource-1",
        product_category="category",
        product_type="type",
        identity_id="identity",
        cost_type=CostType.USAGE,
        amount=Decimal("1.0000"),
        allocation_method="method",
        allocation_detail="detail",
        metadata={"env_id": "env-legacy"},
        principal_team="team-new",
    )

    try:
        with backend.create_unit_of_work() as uow:
            stored = uow.chargebacks.upsert(row)
            uow.commit()
        with backend.create_read_only_unit_of_work() as uow:
            restored = uow.chargebacks.find_by_date("arbitrary", "tenant-1", date(2026, 8, 1))
    finally:
        backend.dispose()

    assert stored.principal_team is None
    assert len(restored) == 1
    assert restored[0].principal_team is None
    assert restored[0].metadata == {"env_id": "env-legacy"}


def test_pre_034_fact_survives_upgrade_and_new_team_snapshot_round_trips(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'upgrade-principal-team.db'}"
    legacy_row = ChargebackRow(
        ecosystem="arbitrary",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="resource-1",
        product_category="category",
        product_type="type",
        identity_id="identity-legacy",
        cost_type=CostType.USAGE,
        amount=Decimal("1.0000"),
        allocation_method="method",
        allocation_detail="detail",
        metadata={"env_id": "env-legacy"},
    )
    command.upgrade(_config(url, selection="disabled"), "033")
    legacy_backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=False)
    legacy_backend.create_tables()
    try:
        with legacy_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(legacy_row)
            uow.commit()
    finally:
        legacy_backend.dispose()

    command.upgrade(_config(url, selection="disabled"), "034")
    migrated_backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=False)
    migrated_backend.create_tables()
    new_row = ChargebackRow(
        ecosystem="arbitrary",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 2, tzinfo=UTC),
        resource_id="resource-1",
        product_category="category",
        product_type="type",
        identity_id="identity-new",
        cost_type=CostType.USAGE,
        amount=Decimal("2.0000"),
        allocation_method="method",
        allocation_detail="detail",
        metadata={"env_id": "env-new"},
        principal_team="team-new",
    )
    try:
        with migrated_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(new_row)
            uow.commit()
        with migrated_backend.create_read_only_unit_of_work() as uow:
            legacy_rows = uow.chargebacks.find_by_date("arbitrary", "tenant-1", date(2026, 8, 1))
            new_rows = uow.chargebacks.find_by_date("arbitrary", "tenant-1", date(2026, 8, 2))
    finally:
        migrated_backend.dispose()

    assert [(row.principal_team, row.metadata) for row in legacy_rows] == [(None, {"env_id": "env-legacy"})]
    assert [(row.principal_team, row.metadata) for row in new_rows] == [("team-new", {"env_id": "env-new"})]


def test_pre_034_filtered_and_csv_reads_remain_available(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    from core.api.app import create_app
    from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
    from tests.integration.core.api.backend_provider import install_backend

    url = f"sqlite:///{tmp_path / 'legacy-filters.db'}"
    command.upgrade(_config(url, selection="disabled"), "033")
    backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    row = ChargebackRow(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="resource-1",
        product_category="category",
        product_type="type",
        identity_id="identity",
        cost_type=CostType.USAGE,
        amount=Decimal("1.0000"),
        allocation_method="method",
        allocation_detail="detail",
        metadata={"env_id": "env-legacy"},
    )
    settings = AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={
            "tenant": TenantConfig(
                ecosystem="self_managed_kafka",
                tenant_id="tenant-1",
                storage=StorageConfig(connection_string=url),
            )
        },
    )
    app = create_app(settings)
    try:
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(row)
            uow.commit()
        with backend.create_read_only_unit_of_work() as uow:
            filtered, total = uow.chargebacks.find_by_filters("self_managed_kafka", "tenant-1")
            streamed = list(uow.chargebacks.iter_by_filters("self_managed_kafka", "tenant-1"))
        with TestClient(app) as client:
            install_backend(app, "tenant", backend)
            list_response = client.get(
                "/api/v1/tenants/tenant/chargebacks",
                params={"start_date": "2026-08-01", "end_date": "2026-08-01"},
            )
            export_response = client.post(
                "/api/v1/tenants/tenant/export",
                json={"start_date": "2026-08-01", "end_date": "2026-08-01", "columns": ["identity_id", "metadata"]},
            )
    finally:
        backend.dispose()

    assert total == len(filtered) == len(streamed) == 1
    assert filtered[0].principal_team is None
    assert list_response.status_code == 200
    assert list_response.json()["items"][0]["metadata"] == {"env_id": "env-legacy"}
    assert export_response.status_code == 200
    assert "identity,{'env_id': 'env-legacy'}" in export_response.text


def test_legacy_schema_capability_is_reflected_once_at_the_backend_boundary(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    from core.storage.backends.sqlmodel import unit_of_work as unit_of_work_module

    url = f"sqlite:///{tmp_path / 'legacy-capability.db'}"
    command.upgrade(_config(url, selection="disabled"), "033")
    inspections = 0
    original_inspect = unit_of_work_module.inspect

    def count_inspections(*args: object, **kwargs: object) -> object:
        nonlocal inspections
        inspections += 1
        return original_inspect(*args, **kwargs)

    monkeypatch.setattr(unit_of_work_module, "inspect", count_inspections)
    backend = SQLModelBackend(url, SelfManagedKafkaStorageModule(), use_migrations=False)
    backend.create_tables()
    try:
        for _ in range(3):
            with backend.create_read_only_unit_of_work() as uow:
                assert uow.chargebacks.find_by_filters("arbitrary", "tenant-1") == ([], 0)
    finally:
        backend.dispose()

    assert inspections == 1
