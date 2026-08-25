"""PostgreSQL verification for selected Self-Managed Kafka plugin storage."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
from tests.unit.core.storage.test_migration_030_preview_repair_head import _config

_TEST_POSTGRESQL_URL = os.environ.get("TEST_POSTGRESQL_URL")
_PLUGIN_TABLES = {"self_managed_kafka_scope_state", "self_managed_kafka_principal_team_snapshots"}
pytestmark = pytest.mark.skipif(_TEST_POSTGRESQL_URL is None, reason="TEST_POSTGRESQL_URL is not configured")


@pytest.fixture
def postgresql_schema_url() -> Iterator[str]:
    assert _TEST_POSTGRESQL_URL is not None
    schema = f"smk_plugin_storage_{uuid4().hex}"
    admin_engine = create_engine(_TEST_POSTGRESQL_URL)
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    schema_url = (
        make_url(_TEST_POSTGRESQL_URL)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    try:
        yield schema_url
    finally:
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin_engine.dispose()


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _version(url: str) -> str:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()


def _row() -> ChargebackRow:
    return ChargebackRow(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 8, 1, tzinfo=UTC),
        resource_id="cluster-1",
        product_category="network",
        product_type="SELF_KAFKA_NETWORK_INGRESS",
        identity_id="User:alice",
        cost_type=CostType.USAGE,
        amount=Decimal("1.0000"),
        allocation_method="principal_quota_ready_v1",
        allocation_detail="usage_ratio_allocation",
        metadata={"team": "team-data"},
    )


def test_postgresql_selected_migration_and_repository_round_trip(postgresql_schema_url: str) -> None:
    backend = SQLModelBackend(postgresql_schema_url, SelfManagedKafkaStorageModule(), use_migrations=True)

    try:
        backend.create_tables()
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(_row())
            uow.commit()
        with backend.create_read_only_unit_of_work() as uow:
            rows = uow.chargebacks.find_by_date("self_managed_kafka", "tenant-1", datetime(2026, 8, 1).date())
    finally:
        backend.dispose()

    assert _version(postgresql_schema_url) == "033"
    assert _table_names(postgresql_schema_url) >= _PLUGIN_TABLES
    assert [row.metadata for row in rows] == [{"team": "team-data"}]


def test_postgresql_already_head_selected_prepare_and_downgrade_selection(postgresql_schema_url: str) -> None:
    command.upgrade(_config(postgresql_schema_url), "head")
    assert _version(postgresql_schema_url) == "033"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(postgresql_schema_url))

    selected = _config(postgresql_schema_url, plugin_storage="self_managed_kafka")
    command.upgrade(selected, "head")
    assert _table_names(postgresql_schema_url) >= _PLUGIN_TABLES

    with pytest.raises(RuntimeError, match="plugin_storage"):
        command.downgrade(_config(postgresql_schema_url), "032")
    assert _version(postgresql_schema_url) == "033"
    assert _table_names(postgresql_schema_url) >= _PLUGIN_TABLES

    command.downgrade(_config(postgresql_schema_url, plugin_storage="disabled"), "032")
    assert _version(postgresql_schema_url) == "032"
    assert _table_names(postgresql_schema_url) >= _PLUGIN_TABLES

    command.upgrade(selected, "head")
    command.downgrade(selected, "032")
    assert _version(postgresql_schema_url) == "032"
    assert _PLUGIN_TABLES.isdisjoint(_table_names(postgresql_schema_url))
