from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

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
    schema = f"task_254_18_{uuid4().hex}"
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
