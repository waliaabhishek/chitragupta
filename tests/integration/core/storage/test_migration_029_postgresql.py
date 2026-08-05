from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_019_focus_preview import (
    _alembic_config,
)

TEST_POSTGRESQL_URL = os.environ.get("TEST_POSTGRESQL_URL")
pytestmark = pytest.mark.skipif(
    TEST_POSTGRESQL_URL is None,
    reason="TEST_POSTGRESQL_URL is not configured",
)


@pytest.fixture
def postgresql_schema_url() -> Iterator[str]:
    assert TEST_POSTGRESQL_URL is not None
    schema = f"task_254_51_{uuid4().hex}"
    admin_engine = create_engine(TEST_POSTGRESQL_URL)
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    schema_url = (
        make_url(TEST_POSTGRESQL_URL)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    try:
        yield schema_url
    finally:
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin_engine.dispose()


BILLING_PK_004 = (
    "ecosystem",
    "tenant_id",
    "timestamp",
    "resource_id",
    "product_type",
)
BILLING_PK_005 = (*BILLING_PK_004, "product_category")
CHARGEBACK_UNIQUE_008 = (
    "ecosystem",
    "tenant_id",
    "resource_id",
    "product_category",
    "product_type",
    "identity_id",
    "cost_type",
    "allocation_method",
    "allocation_detail",
)
CHARGEBACK_UNIQUE_009 = (*CHARGEBACK_UNIQUE_008, "env_id")


def _head_revision(url: str) -> str:
    return ScriptDirectory.from_config(_alembic_config(url, preview_enabled=False)).get_current_head()


def _version(url: str) -> str:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


def _column_names(url: str, table_name: str) -> set[str]:
    engine = create_engine(url)
    try:
        return {column["name"] for column in inspect(engine).get_columns(table_name)}
    finally:
        engine.dispose()


def _pk_columns(url: str, table_name: str) -> tuple[str, ...]:
    engine = create_engine(url)
    try:
        pk = inspect(engine).get_pk_constraint(table_name)
        return tuple(pk.get("constrained_columns") or ())
    finally:
        engine.dispose()


def _index_names(url: str, table_name: str) -> set[str]:
    engine = create_engine(url)
    try:
        return {index["name"] for index in inspect(engine).get_indexes(table_name)}
    finally:
        engine.dispose()


def _named_unique_columns(url: str, table_name: str, unique_name: str) -> tuple[str, ...]:
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        for constraint in inspector.get_unique_constraints(table_name):
            if constraint["name"] == unique_name:
                return tuple(constraint.get("column_names") or ())
        for index in inspector.get_indexes(table_name):
            if index["name"] == unique_name and index.get("unique"):
                return tuple(index.get("column_names") or ())
    finally:
        engine.dispose()
    raise AssertionError(f"{unique_name} was not found on {table_name}")


def _query_rows(url: str, statement: str, parameters: dict[str, object] | None = None) -> list[tuple[object, ...]]:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return [tuple(row) for row in connection.execute(text(statement), parameters or {}).all()]
    finally:
        engine.dispose()


def _insert_billing_row(
    url: str,
    *,
    timestamp: str,
    product_category: str,
    resource_id: str = "resource-1",
    ecosystem: str = "generic_metrics_only",
    product_type: str = "cpu",
    total_cost: str = "10",
    metadata_json: str | None = None,
) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO billing (
                        ecosystem, tenant_id, timestamp, resource_id,
                        product_type, product_category, quantity, unit_price,
                        total_cost, currency, granularity, metadata_json
                    ) VALUES (
                        :ecosystem, 'tenant-1', :timestamp, :resource_id,
                        :product_type, :product_category, '5', '2',
                        :total_cost, 'USD', 'daily', :metadata_json
                    )
                    """
                ),
                {
                    "ecosystem": ecosystem,
                    "timestamp": timestamp,
                    "resource_id": resource_id,
                    "product_type": product_type,
                    "product_category": product_category,
                    "total_cost": total_cost,
                    "metadata_json": metadata_json,
                },
            )
    finally:
        engine.dispose()


def _insert_ccloud_billing_row(
    url: str,
    *,
    timestamp: str,
    env_id: str,
    resource_id: str = "lkc-1",
    total_cost: str = "21",
    metadata_json: str = "{}",
) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO ccloud_billing (
                        ecosystem, tenant_id, timestamp, env_id, resource_id,
                        product_type, product_category, quantity, unit_price,
                        total_cost, currency, granularity, metadata_json
                    ) VALUES (
                        'confluent_cloud', 'tenant-1', :timestamp, :env_id, :resource_id,
                        'KAFKA_STORAGE', 'KAFKA', '7', '3',
                        :total_cost, 'USD', 'daily', :metadata_json
                    )
                    """
                ),
                {
                    "timestamp": timestamp,
                    "env_id": env_id,
                    "resource_id": resource_id,
                    "total_cost": total_cost,
                    "metadata_json": metadata_json,
                },
            )
    finally:
        engine.dispose()


def _insert_chargeback_dimension_revision_008(url: str, *, dimension_id: int, resource_id: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_dimensions (
                        dimension_id, ecosystem, tenant_id, resource_id,
                        product_category, product_type, identity_id, cost_type,
                        allocation_method, allocation_detail
                    ) VALUES (
                        :dimension_id, 'confluent_cloud', 'tenant-1', :resource_id,
                        'KAFKA', 'KAFKA_STORAGE', 'identity-1', 'usage',
                        'direct', ''
                    )
                    """
                ),
                {"dimension_id": dimension_id, "resource_id": resource_id},
            )
    finally:
        engine.dispose()


def _insert_chargeback_dimension_revision_009(
    url: str,
    *,
    dimension_id: int,
    resource_id: str,
    env_id: str,
) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO chargeback_dimensions (
                        dimension_id, ecosystem, tenant_id, resource_id,
                        product_category, product_type, identity_id, cost_type,
                        allocation_method, allocation_detail, env_id
                    ) VALUES (
                        :dimension_id, 'confluent_cloud', 'tenant-1', :resource_id,
                        'KAFKA', 'KAFKA_STORAGE', 'identity-1', 'usage',
                        'direct', '', :env_id
                    )
                    """
                ),
                {
                    "dimension_id": dimension_id,
                    "resource_id": resource_id,
                    "env_id": env_id,
                },
            )
    finally:
        engine.dispose()


def test_postgresql_production_create_tables_reaches_head_with_expected_billing_identity(
    postgresql_schema_url: str,
) -> None:
    backend = SQLModelBackend(
        postgresql_schema_url,
        CCloudStorageModule(),
        focus_preview_enabled=True,
    )

    backend.create_tables()
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-01T00:00:00+00:00",
        product_category="compute",
    )
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-01T00:00:00+00:00",
        product_category="storage",
    )
    rows = _query_rows(
        postgresql_schema_url,
        """
        SELECT product_category, total_cost
        FROM billing
        ORDER BY product_category
        """,
    )

    with pytest.raises(IntegrityError):
        _insert_billing_row(
            postgresql_schema_url,
            timestamp="2026-08-01T00:00:00+00:00",
            product_category="compute",
        )

    backend.dispose()
    assert _version(postgresql_schema_url) == _head_revision(postgresql_schema_url)
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_005
    assert "ix_billing_product_category" not in _index_names(postgresql_schema_url, "billing")
    assert rows == [("compute", "10"), ("storage", "10")]


def test_postgresql_revision_004_upgrades_to_005_with_expected_pk_replacement(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "004")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-02T00:00:00+00:00",
        product_category="compute",
    )

    assert _version(postgresql_schema_url) == "004"
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_004
    assert "ix_billing_product_category" in _index_names(postgresql_schema_url, "billing")

    command.upgrade(config, "005")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-02T00:00:00+00:00",
        product_category="storage",
    )

    assert _version(postgresql_schema_url) == "005"
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_005
    assert "ix_billing_product_category" not in _index_names(postgresql_schema_url, "billing")
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT product_category
        FROM billing
        WHERE timestamp = '2026-08-02T00:00:00+00:00'
        ORDER BY product_category
        """,
    ) == [("compute",), ("storage",)]


def test_postgresql_revision_004_with_rows_upgrades_directly_to_head(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)
    timestamp = "2026-08-02T01:00:00+00:00"

    command.upgrade(config, "004")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp=timestamp,
        product_category="compute",
        resource_id="generic-resource",
    )
    _insert_billing_row(
        postgresql_schema_url,
        timestamp=timestamp,
        product_category="KAFKA",
        resource_id="lkc-direct-head",
        ecosystem="confluent_cloud",
        product_type="KAFKA_STORAGE",
        total_cost="21",
        metadata_json='{"env_id":"env-1"}',
    )
    _insert_chargeback_dimension_revision_008(
        postgresql_schema_url,
        dimension_id=31,
        resource_id="lkc-direct-head",
    )

    command.upgrade(config, "head")

    assert _version(postgresql_schema_url) == _head_revision(postgresql_schema_url)
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT ecosystem, resource_id, product_category, total_cost
        FROM billing
        ORDER BY ecosystem, resource_id
        """,
    ) == [("generic_metrics_only", "generic-resource", "compute", "10")]
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT ecosystem, env_id, resource_id, product_category, total_cost
        FROM ccloud_billing
        ORDER BY ecosystem, env_id, resource_id
        """,
    ) == [("confluent_cloud", "env-1", "lkc-direct-head", "KAFKA", "21")]
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT dimension_id, ecosystem, tenant_id, resource_id, env_id
        FROM chargeback_dimensions
        ORDER BY dimension_id
        """,
    ) == [(31, "confluent_cloud", "tenant-1", "lkc-direct-head", "env-1")]


def test_postgresql_revision_005_with_product_category_variants_upgrades_to_head(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "005")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-03T00:00:00+00:00",
        product_category="compute",
    )
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-03T00:00:00+00:00",
        product_category="storage",
    )

    command.upgrade(config, "head")

    assert _version(postgresql_schema_url) == _head_revision(postgresql_schema_url)
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_005
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT product_category, total_cost
        FROM billing
        WHERE timestamp = '2026-08-03T00:00:00+00:00'
        ORDER BY product_category
        """,
    ) == [("compute", "10"), ("storage", "10")]


def test_postgresql_revision_008_upgrades_to_head_without_losing_ccloud_env_identity(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "008")
    _insert_ccloud_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-04T00:00:00+00:00",
        env_id="env-1",
        resource_id="lkc-1",
        metadata_json='{"env_id":"env-1"}',
    )
    _insert_chargeback_dimension_revision_008(
        postgresql_schema_url,
        dimension_id=41,
        resource_id="lkc-1",
    )

    command.upgrade(config, "head")

    assert _version(postgresql_schema_url) == _head_revision(postgresql_schema_url)
    assert (
        _named_unique_columns(
            postgresql_schema_url,
            "chargeback_dimensions",
            "uq_chargeback_dimensions",
        )
        == CHARGEBACK_UNIQUE_009
    )
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT env_id, resource_id
        FROM chargeback_dimensions
        WHERE dimension_id = 41
        """,
    ) == [("env-1", "lkc-1")]
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT env_id, resource_id, total_cost
        FROM ccloud_billing
        ORDER BY env_id, resource_id
        """,
    ) == [("env-1", "lkc-1", "21")]

    _insert_chargeback_dimension_revision_009(
        postgresql_schema_url,
        dimension_id=42,
        resource_id="lkc-1",
        env_id="env-2",
    )
    with pytest.raises(IntegrityError):
        _insert_chargeback_dimension_revision_009(
            postgresql_schema_url,
            dimension_id=43,
            resource_id="lkc-1",
            env_id="env-2",
        )


def test_postgresql_downgrade_to_004_preserves_no_conflict_billing_data(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "head")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-05T00:00:00+00:00",
        product_category="compute",
    )
    _insert_ccloud_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-05T00:00:00+00:00",
        env_id="env-1",
        resource_id="lkc-rollback",
    )
    _insert_chargeback_dimension_revision_009(
        postgresql_schema_url,
        dimension_id=51,
        resource_id="lkc-rollback",
        env_id="env-1",
    )

    command.downgrade(config, "004")

    assert _version(postgresql_schema_url) == "004"
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_004
    assert "ix_billing_product_category" in _index_names(postgresql_schema_url, "billing")
    assert "ccloud_billing" not in _table_names(postgresql_schema_url)
    assert "env_id" not in _column_names(postgresql_schema_url, "chargeback_dimensions")
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT dimension_id, ecosystem, tenant_id, resource_id,
               product_category, product_type, identity_id, cost_type,
               allocation_method, allocation_detail
        FROM chargeback_dimensions
        WHERE dimension_id = 51
        """,
    ) == [
        (
            51,
            "confluent_cloud",
            "tenant-1",
            "lkc-rollback",
            "KAFKA",
            "KAFKA_STORAGE",
            "identity-1",
            "usage",
            "direct",
            "",
        )
    ]
    assert _query_rows(
        postgresql_schema_url,
        """
        SELECT ecosystem, resource_id, product_category, total_cost
        FROM billing
        ORDER BY ecosystem, resource_id, product_category
        """,
    ) == [
        ("confluent_cloud", "lkc-rollback", "KAFKA", "21"),
        ("generic_metrics_only", "resource-1", "compute", "10"),
    ]


def test_postgresql_downgrade_005_to_004_fails_before_product_category_collapse(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "005")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-06T00:00:00+00:00",
        product_category="compute",
    )
    _insert_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-06T00:00:00+00:00",
        product_category="storage",
    )
    before = _query_rows(
        postgresql_schema_url,
        """
        SELECT product_category, total_cost
        FROM billing
        WHERE timestamp = '2026-08-06T00:00:00+00:00'
        ORDER BY product_category
        """,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "migration 005 downgrade cannot restore the revision-004 billing primary key "
            "because billing rows differ only by product_category; downgrade aborted before "
            "constraint changes"
        ),
    ):
        command.downgrade(config, "004")

    assert _version(postgresql_schema_url) == "005"
    assert _pk_columns(postgresql_schema_url, "billing") == BILLING_PK_005
    assert (
        _query_rows(
            postgresql_schema_url,
            """
        SELECT product_category, total_cost
        FROM billing
        WHERE timestamp = '2026-08-06T00:00:00+00:00'
        ORDER BY product_category
        """,
        )
        == before
    )


def test_postgresql_dde_downgrade_fails_before_ccloud_env_collapse(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "ddebea2fe0a8")
    _insert_ccloud_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-07T00:00:00+00:00",
        env_id="env-1",
        resource_id="lkc-collapse",
    )
    _insert_ccloud_billing_row(
        postgresql_schema_url,
        timestamp="2026-08-07T00:00:00+00:00",
        env_id="env-2",
        resource_id="lkc-collapse",
    )
    before = _query_rows(
        postgresql_schema_url,
        """
        SELECT env_id, resource_id, total_cost
        FROM ccloud_billing
        WHERE resource_id = 'lkc-collapse'
        ORDER BY env_id
        """,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "migration ddebea2fe0a8 downgrade cannot merge ccloud_billing rows into billing "
            "because rows differ only by env_id; downgrade aborted before data changes"
        ),
    ):
        command.downgrade(config, "005")

    assert _version(postgresql_schema_url) == "ddebea2fe0a8"
    assert "ccloud_billing" in _table_names(postgresql_schema_url)
    assert (
        _query_rows(
            postgresql_schema_url,
            """
        SELECT env_id, resource_id, total_cost
        FROM ccloud_billing
        WHERE resource_id = 'lkc-collapse'
        ORDER BY env_id
        """,
        )
        == before
    )


def test_postgresql_dde_downgrade_fails_before_target_billing_collision(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)
    timestamp = "2026-08-07T01:00:00+00:00"
    resource_id = "lkc-target-collision"

    command.upgrade(config, "ddebea2fe0a8")
    _insert_billing_row(
        postgresql_schema_url,
        timestamp=timestamp,
        product_category="KAFKA",
        resource_id=resource_id,
        ecosystem="confluent_cloud",
        product_type="KAFKA_STORAGE",
        total_cost="10",
    )
    _insert_ccloud_billing_row(
        postgresql_schema_url,
        timestamp=timestamp,
        env_id="env-1",
        resource_id=resource_id,
        total_cost="21",
    )
    billing_before = _query_rows(
        postgresql_schema_url,
        """
        SELECT ecosystem, tenant_id, timestamp, resource_id,
               product_type, product_category, total_cost
        FROM billing
        WHERE resource_id = :resource_id
        """,
        {"resource_id": resource_id},
    )
    ccloud_before = _query_rows(
        postgresql_schema_url,
        """
        SELECT ecosystem, tenant_id, timestamp, env_id, resource_id,
               product_type, product_category, total_cost
        FROM ccloud_billing
        WHERE resource_id = :resource_id
        """,
        {"resource_id": resource_id},
    )
    assert len(billing_before) == 1
    assert len(ccloud_before) == 1

    with pytest.raises(
        RuntimeError,
        match=(
            "migration ddebea2fe0a8 downgrade cannot copy ccloud_billing rows into billing "
            "because target billing keys already exist; downgrade aborted before data changes"
        ),
    ):
        command.downgrade(config, "005")

    assert _version(postgresql_schema_url) == "ddebea2fe0a8"
    assert {"billing", "ccloud_billing"} <= _table_names(postgresql_schema_url)
    assert (
        _query_rows(
            postgresql_schema_url,
            """
            SELECT ecosystem, tenant_id, timestamp, resource_id,
                   product_type, product_category, total_cost
            FROM billing
            WHERE resource_id = :resource_id
            """,
            {"resource_id": resource_id},
        )
        == billing_before
    )
    assert (
        _query_rows(
            postgresql_schema_url,
            """
            SELECT ecosystem, tenant_id, timestamp, env_id, resource_id,
                   product_type, product_category, total_cost
            FROM ccloud_billing
            WHERE resource_id = :resource_id
            """,
            {"resource_id": resource_id},
        )
        == ccloud_before
    )


def test_postgresql_009_downgrade_fails_before_env_id_collapse(
    postgresql_schema_url: str,
) -> None:
    config = _alembic_config(postgresql_schema_url, preview_enabled=False)

    command.upgrade(config, "009")
    _insert_chargeback_dimension_revision_009(
        postgresql_schema_url,
        dimension_id=61,
        resource_id="lkc-collapse",
        env_id="env-1",
    )
    _insert_chargeback_dimension_revision_009(
        postgresql_schema_url,
        dimension_id=62,
        resource_id="lkc-collapse",
        env_id="env-2",
    )
    before = _query_rows(
        postgresql_schema_url,
        """
        SELECT env_id, resource_id
        FROM chargeback_dimensions
        WHERE resource_id = 'lkc-collapse'
        ORDER BY env_id
        """,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "migration 009 downgrade cannot remove chargeback_dimensions.env_id because rows "
            "differ only by env_id; downgrade aborted before constraint or column changes"
        ),
    ):
        command.downgrade(config, "008")

    assert _version(postgresql_schema_url) == "009"
    assert "env_id" in _column_names(postgresql_schema_url, "chargeback_dimensions")
    assert _named_unique_columns(postgresql_schema_url, "chargeback_dimensions", "uq_chargeback_dimensions") == (
        CHARGEBACK_UNIQUE_009
    )
    assert (
        _query_rows(
            postgresql_schema_url,
            """
        SELECT env_id, resource_id
        FROM chargeback_dimensions
        WHERE resource_id = 'lkc-collapse'
        ORDER BY env_id
        """,
        )
        == before
    )


def _upgrade_028(url: str) -> None:
    command.upgrade(_alembic_config(url), "028")


def _insert_billing(
    url: str,
    *,
    timestamp: str,
    total_cost: str = "8",
    resource_id: str = "lkc-1",
) -> None:
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ccloud_billing (
                    ecosystem, tenant_id, timestamp, env_id, resource_id,
                    product_type, product_category, quantity, unit_price,
                    total_cost, currency, granularity, allocation_attempts,
                    topic_attribution_attempts, metadata_json
                ) VALUES (
                    'confluent_cloud', 'tenant-1', :timestamp, 'env-1',
                    :resource_id, 'KAFKA_STORAGE', 'KAFKA', '5', '2',
                    :total_cost, 'USD', 'daily', 2, 3, '{}'
                )
                """
            ),
            {
                "timestamp": timestamp,
                "resource_id": resource_id,
                "total_cost": total_cost,
            },
        )
    engine.dispose()


def test_postgresql_identical_same_second_rows_converge_deterministically(
    postgresql_schema_url: str,
) -> None:
    _upgrade_028(postgresql_schema_url)
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:00.100000+00:00",
    )
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:00.900000+00:00",
    )

    command.upgrade(_alembic_config(postgresql_schema_url), "029")

    engine = create_engine(postgresql_schema_url)
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT timestamp, total_cost FROM ccloud_billing ORDER BY timestamp")).all()
        version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    engine.dispose()
    assert version == "029"
    assert rows == [(datetime(2026, 7, 1, tzinfo=UTC), "8")]


def test_postgresql_conflict_rolls_back_rows_and_alembic_version(
    postgresql_schema_url: str,
) -> None:
    _upgrade_028(postgresql_schema_url)
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:00.100000+00:00",
        total_cost="8",
    )
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:00.900000+00:00",
        total_cost="9",
    )
    engine = create_engine(postgresql_schema_url)
    with engine.connect() as connection:
        before = connection.execute(text("SELECT timestamp, total_cost FROM ccloud_billing ORDER BY timestamp")).all()
    engine.dispose()

    with pytest.raises(
        Exception,
        match=r"timestamp canonicalization conflict.*ccloud_billing",
    ):
        command.upgrade(_alembic_config(postgresql_schema_url), "029")

    engine = create_engine(postgresql_schema_url)
    with engine.connect() as connection:
        after = connection.execute(text("SELECT timestamp, total_cost FROM ccloud_billing ORDER BY timestamp")).all()
        version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    engine.dispose()
    assert version == "028"
    assert after == before


def test_postgresql_scalar_json_and_retry_schema_round_trip_upgrade_downgrade(
    postgresql_schema_url: str,
) -> None:
    _upgrade_028(postgresql_schema_url)
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:00.987654+00:00",
        resource_id="lkc-1",
    )
    _insert_billing(
        postgresql_schema_url,
        timestamp="2026-07-01T00:00:01.123456+00:00",
        resource_id="lkc-2",
    )
    coverage = [
        {
            "tracking_date": "2026-07-01",
            "calculation_id": "calculation-1",
            "calculation_completed_at": "2026-07-03T01:02:03.987654-07:00",
            "calculation_run_id": 41,
        }
    ]
    snapshot = {
        "calculation_timestamp": "2026-07-03T01:02:03.987654-07:00",
        "calculation_coverage": coverage,
        "source_through": "2026-07-04T04:05:06.456789+02:00",
        "effective_coverage_start_date": "2026-07-01",
        "effective_coverage_end_date": "2026-07-02",
        "availability_cutoff_end_date": "2026-07-03",
    }
    engine = create_engine(postgresql_schema_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests (
                    request_id, tenant_name, ecosystem, tenant_id, grain,
                    start_date, end_date, column_profile, status, created_at,
                    calculation_coverage_json
                ) VALUES (
                    'request-1', 'production', 'confluent_cloud', 'tenant-1',
                    'daily', '2026-07-01', '2026-07-02', 'full', 'queued',
                    '2026-07-03T01:02:03.987654+00:00', :coverage
                )
                """
            ),
            {"coverage": json.dumps(coverage, sort_keys=True)},
        )
        connection.execute(
            text(
                """
                INSERT INTO preview_revisions (
                    revision_id, tenant_name_at_publication, ecosystem,
                    tenant_id, month_start, month_end, monthly_status,
                    material_sha256, source_snapshot_json, published_at,
                    is_current, storage_key, manifest_metadata_json
                ) VALUES (
                    'revision-1', 'production', 'confluent_cloud', 'tenant-1',
                    '2026-07-01', '2026-08-01', 'settled', :material,
                    :snapshot, '2026-08-07T01:02:03.987654+00:00',
                    TRUE, 'storage-1', '{}'
                )
                """
            ),
            {
                "material": "a" * 64,
                "snapshot": json.dumps(snapshot, sort_keys=True),
            },
        )
    engine.dispose()

    config = _alembic_config(postgresql_schema_url)
    command.upgrade(config, "029")

    engine = create_engine(postgresql_schema_url)
    with engine.connect() as connection:
        billing = connection.execute(text("SELECT timestamp FROM ccloud_billing ORDER BY timestamp")).scalars().all()
        request = connection.execute(
            text("SELECT created_at, calculation_coverage_json FROM preview_requests WHERE request_id = 'request-1'")
        ).one()
        revision = connection.execute(
            text(
                "SELECT published_at, source_snapshot_json, "
                "retention_retry_count FROM preview_revisions "
                "WHERE revision_id = 'revision-1'"
            )
        ).one()
    assert billing == [
        datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 1, 0, 0, 1, tzinfo=UTC),
    ]
    assert request.created_at == datetime(2026, 7, 3, 1, 2, 3, tzinfo=UTC)
    assert json.loads(request.calculation_coverage_json)[0]["calculation_completed_at"] == "2026-07-03T08:02:03+00:00"
    assert revision.published_at == datetime(2026, 8, 7, 1, 2, 3, tzinfo=UTC)
    assert json.loads(revision.source_snapshot_json)["source_through"] == ("2026-07-04T02:05:06+00:00")
    assert revision.retention_retry_count == 0
    engine.dispose()

    command.downgrade(config, "028")
    engine = create_engine(postgresql_schema_url)
    try:
        assert "retention_retry_count" not in {
            column["name"] for column in inspect(engine).get_columns("preview_revisions")
        }
    finally:
        engine.dispose()


def test_postgresql_production_repository_binds_aware_utc_seconds(
    postgresql_schema_url: str,
) -> None:
    command.upgrade(_alembic_config(postgresql_schema_url), "029")
    backend = SQLModelBackend(
        postgresql_schema_url,
        CCloudStorageModule(),
        focus_preview_enabled=True,
    )
    backend.create_tables()
    line = CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        timestamp=datetime(
            2026,
            7,
            1,
            0,
            0,
            0,
            987_654,
            tzinfo=UTC,
        ),
        env_id="env-1",
        resource_id="lkc-1",
        product_type="KAFKA_STORAGE",
        product_category="KAFKA",
        quantity=Decimal("5"),
        unit_price=Decimal("2"),
        total_cost=Decimal("8"),
        currency="USD",
        granularity="daily",
    )
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(line)
        uow.commit()

    engine = create_engine(postgresql_schema_url)
    with engine.connect() as connection:
        persisted = connection.execute(text("SELECT timestamp FROM ccloud_billing")).scalar_one()
    engine.dispose()
    backend.dispose()
    assert persisted == datetime(2026, 7, 1, tzinfo=UTC)
