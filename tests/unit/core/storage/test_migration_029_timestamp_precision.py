from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from sqlalchemy import Column, DateTime, MetaData, String, Table, create_engine, inspect, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from tests.unit.core.storage.test_migration_019_focus_preview import (
    _alembic_config,
    _seed_legacy_rows,
)
from tests.unit.core.storage.test_migration_021_allocation_lineage import (
    _insert_legacy_source,
)

SECOND = "2026-07-01 00:00:00"
FRACTIONAL_ZERO = "2026-07-01 00:00:00.000000"
OPTIONAL_PREVIEW_TABLES = {
    "ccloud_allocation_lineage_portions",
    "ccloud_allocation_lineage_runs",
    "ccloud_cost_source_records",
    "ccloud_organization_authority_attempts",
    "ccloud_source_capture_readiness",
    "ccloud_source_evidence_attempts",
}


def _upgrade_028(url: str, *, preview_enabled: bool = True) -> None:
    command.upgrade(
        _alembic_config(url, preview_enabled=preview_enabled),
        "028",
    )


def _row_snapshot(url: str, table: str) -> list[tuple[object, ...]]:
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            return [
                tuple(row)
                for row in connection.execute(
                    text(f'SELECT * FROM "{table}" ORDER BY 1, 2, 3')  # noqa: S608
                ).all()
            ]
    finally:
        engine.dispose()


def _duplicate_row(
    connection: Any,
    table: str,
    *,
    where: str,
    replacements: dict[str, object],
) -> None:
    columns = [
        row[1]
        for row in connection.exec_driver_sql(
            f'PRAGMA table_info("{table}")'  # noqa: S608
        ).all()
    ]
    original = dict(
        connection.execute(
            text(f'SELECT * FROM "{table}" WHERE {where}')  # noqa: S608
        )
        .mappings()
        .one()
    )
    values = original | replacements
    names = ", ".join(f'"{name}"' for name in columns)
    parameters = ", ".join(f":{name}" for name in columns)
    connection.execute(
        text(f'INSERT INTO "{table}" ({names}) VALUES ({parameters})'),  # noqa: S608
        {name: values[name] for name in columns},
    )


def _seed_mixed_financial_rows(url: str, *, conflicting_billing: bool = False) -> None:
    _seed_legacy_rows(url)
    _insert_legacy_source(url)
    engine = create_engine(url)
    with engine.begin() as connection:
        for timestamp in (SECOND, FRACTIONAL_ZERO):
            connection.execute(
                text(
                    """
                    INSERT INTO billing (
                        ecosystem, tenant_id, timestamp, resource_id,
                        product_type, product_category, quantity, unit_price,
                        total_cost, currency, granularity, allocation_attempts,
                        topic_attribution_attempts, metadata_json
                    ) VALUES (
                        'generic', 'tenant-1', :timestamp, 'resource-1',
                        'compute', 'compute', '5', '2', '8', 'USD', 'daily',
                        2, 3, '{}'
                    )
                    """
                ),
                {"timestamp": timestamp},
            )
        _duplicate_row(
            connection,
            "ccloud_billing",
            where=f"timestamp = '{SECOND}'",
            replacements={
                "timestamp": FRACTIONAL_ZERO,
                "total_cost": "9" if conflicting_billing else "8",
            },
        )
        _duplicate_row(
            connection,
            "chargeback_facts",
            where=f"timestamp = '{SECOND}'",
            replacements={"timestamp": FRACTIONAL_ZERO},
        )
        _duplicate_row(
            connection,
            "topic_attribution_facts",
            where=f"timestamp = '{SECOND}'",
            replacements={"timestamp": FRACTIONAL_ZERO},
        )
        _duplicate_row(
            connection,
            "ccloud_cost_source_records",
            where="source_record_id = 'provider:legacy'",
            replacements={
                "source_period_start": FRACTIONAL_ZERO,
                "source_period_end": "2026-07-02 00:00:00.000000",
                "collection_window_start": FRACTIONAL_ZERO,
                "collection_window_end": "2026-07-02 00:00:00.000000",
                "evidence_scope_start": FRACTIONAL_ZERO,
                "evidence_scope_end": "2026-07-02 00:00:00.000000",
                "allocation_timestamp": FRACTIONAL_ZERO,
                "retention_timestamp": FRACTIONAL_ZERO,
            },
        )
    engine.dispose()


def _seed_lineage_collision(url: str) -> None:
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ccloud_allocation_lineage_runs (
                    ecosystem, tenant_id, tracking_date, calculation_id,
                    calculation_completed_at, capture_status, capture_reason,
                    portion_count
                ) VALUES (
                    'confluent_cloud', 'tenant-1', '2026-07-01',
                    'calculation-1', '2026-07-03 01:02:03.987654',
                    'complete', NULL, 2
                )
                """
            )
        )
        portion = """
            INSERT INTO ccloud_allocation_lineage_portions (
                ecosystem, tenant_id, tracking_date, calculation_id,
                origin_timestamp, origin_env_id, origin_resource_id,
                origin_product_type, origin_product_category, portion_ordinal,
                target_kind, target_id, allocated_cost, allocated_quantity,
                allocation_ratio, method_id, method_version, method_details_json
            ) VALUES (
                'confluent_cloud', 'tenant-1', '2026-07-01', 'calculation-1',
                :origin_timestamp, 'env-1', 'lkc-1', 'KAFKA_STORAGE', 'KAFKA',
                0, 'identity', 'sa-1', '8', '5', '1',
                'direct', 'v1', '{}'
            )
        """
        connection.execute(text(portion), {"origin_timestamp": SECOND})
        connection.execute(
            text(portion),
            {"origin_timestamp": FRACTIONAL_ZERO},
        )
    engine.dispose()


def _seed_preview_json(url: str) -> None:
    coverage = {
        "calculation_id": "calculation-1",
        "calculation_completed_at": "2026-07-03T01:02:03.987654-07:00",
        "calculation_run_id": 41,
        "tracking_date": "2026-07-01",
    }
    source_snapshot = {
        "availability_cutoff_end_date": "2026-07-03",
        "calculation_coverage": [coverage],
        "calculation_timestamp": "2026-07-03T01:02:03.987654-07:00",
        "effective_coverage_end_date": "2026-07-02",
        "effective_coverage_start_date": "2026-07-01",
        "source_through": "2026-07-04T04:05:06.456789+02:00",
    }
    engine = create_engine(url)
    with engine.begin() as connection:
        for request_id, status in (
            ("request-ready", "ready"),
            ("request-expired", "expired"),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO preview_requests (
                        request_id, tenant_name, ecosystem, tenant_id, grain,
                        start_date, end_date, column_profile, status, created_at,
                        completed_at, expires_at, calculation_timestamp,
                        source_through, calculation_coverage_json
                    ) VALUES (
                        :request_id, 'production', 'confluent_cloud', 'tenant-1',
                        'daily', '2026-07-01', '2026-07-02', 'full', :status,
                        '2026-07-03 01:02:03.987654',
                        '2026-07-03 01:02:04.987654',
                        '2026-07-10 01:02:04.987654',
                        '2026-07-03 08:02:03.987654',
                        '2026-07-04 02:05:06.456789',
                        :coverage
                    )
                    """
                ),
                {
                    "request_id": request_id,
                    "status": status,
                    "coverage": json.dumps([coverage], sort_keys=True),
                },
            )
        for revision_id, is_current, supersedes, superseded_by in (
            ("revision-old", False, None, "revision-current"),
            ("revision-current", True, "revision-old", None),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO preview_revisions (
                        revision_id, tenant_name_at_publication, ecosystem,
                        tenant_id, month_start, month_end, monthly_status,
                        material_sha256, source_snapshot_json, published_at,
                        supersedes_revision_id, superseded_by_revision_id,
                        is_current, storage_key, manifest_metadata_json
                    ) VALUES (
                        :revision_id, 'production', 'confluent_cloud', 'tenant-1',
                        '2026-07-01', '2026-08-01', 'settled', :material,
                        :snapshot, '2026-08-07 01:02:03.987654', :supersedes,
                        :superseded_by, :is_current, :storage_key,
                        '{"name":"manifest.json","sha256":"unchanged"}'
                    )
                    """
                ),
                {
                    "revision_id": revision_id,
                    "material": ("a" if is_current else "b") * 64,
                    "snapshot": json.dumps(source_snapshot, sort_keys=True),
                    "supersedes": supersedes,
                    "superseded_by": superseded_by,
                    "is_current": is_current,
                    "storage_key": f"storage-{revision_id}",
                },
            )
        connection.execute(
            text(
                """
                INSERT INTO pipeline_state (
                    ecosystem, tenant_id, tracking_date, billing_gathered,
                    resources_gathered, chargeback_calculated,
                    calculation_id, calculation_completed_at,
                    topic_overlay_gathered, topic_attribution_calculated
                ) VALUES (
                    'confluent_cloud', 'tenant-1', '2026-07-01', 1, 1, 1,
                    'calculation-1', '2026-07-03 01:02:03.987654', 1, 1
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_source_evidence_attempts (
                    attempt_sequence, ecosystem, tenant_id, refresh_token,
                    refresh_start, refresh_end, status, started_at, completed_at
                ) VALUES (
                    71, 'confluent_cloud', 'tenant-1', 'refresh-1',
                    '2026-07-01 00:00:00.111111',
                    '2026-07-02 00:00:00.222222', 'complete',
                    '2026-07-03 01:02:03.333333',
                    '2026-07-03 01:02:04.444444'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_source_capture_readiness (
                    ecosystem, tenant_id, window_start, window_end, capture_id,
                    captured_at, source_count, attempt_sequence
                ) VALUES (
                    'confluent_cloud', 'tenant-1',
                    '2026-07-01 00:00:00.111111',
                    '2026-07-02 00:00:00.222222', 'capture-1',
                    '2026-07-03 01:02:04.444444', 1, 71
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_source_capture_readiness_history (
                    ecosystem, tenant_id, attempt_sequence, window_start,
                    window_end, capture_id, captured_at, source_count
                ) VALUES (
                    'confluent_cloud', 'tenant-1', 71,
                    '2026-07-01 00:00:00.111111',
                    '2026-07-02 00:00:00.222222', 'capture-1',
                    '2026-07-03 01:02:04.444444', 1
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_focus_preview_repairs (
                    repair_id, tenant_name, ecosystem, tenant_id, start_date,
                    end_date, status, created_at, started_at, completed_at
                ) VALUES (
                    'repair-1', 'production', 'confluent_cloud', 'tenant-1',
                    '2026-07-01', '2026-07-02', 'completed',
                    '2026-07-03 01:02:03.111111',
                    '2026-07-03 01:02:03.222222',
                    '2026-07-03 01:02:04.333333'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_focus_preview_repair_dates (
                    repair_id, tracking_date, status, started_at, completed_at,
                    calculation_id, calculation_completed_at, rows_written
                ) VALUES (
                    'repair-1', '2026-07-01', 'succeeded',
                    '2026-07-03 01:02:03.222222',
                    '2026-07-03 01:02:04.333333', 'calculation-1',
                    '2026-07-03 01:02:03.987654', 1
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO ccloud_organization_authority_attempts (
                    attempt_sequence, ecosystem, tenant_id, status, started_at,
                    completed_at, organization_id
                ) VALUES (
                    91, 'confluent_cloud', 'tenant-1', 'available',
                    '2026-07-03 01:02:03.111111',
                    '2026-07-03 01:02:04.222222', 'tenant-1'
                )
                """
            )
        )
    engine.dispose()


def test_migration_029_converges_identical_mixed_financial_rows_without_double_counting(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'mixed-identical.db'}"
    _upgrade_028(url)
    _seed_mixed_financial_rows(url)
    _seed_lineage_collision(url)

    command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "029"
        assert connection.execute(text("SELECT COUNT(*) FROM billing")).scalar_one() == 1
        assert connection.execute(text("SELECT SUM(CAST(total_cost AS NUMERIC)) FROM billing")).scalar_one() == 8
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_billing")).scalar_one() == 1
        assert connection.execute(text("SELECT SUM(CAST(total_cost AS NUMERIC)) FROM ccloud_billing")).scalar_one() == 8
        assert connection.execute(text("SELECT COUNT(*) FROM chargeback_facts")).scalar_one() == 1
        assert connection.execute(text("SELECT SUM(CAST(amount AS NUMERIC)) FROM chargeback_facts")).scalar_one() == 8
        assert connection.execute(text("SELECT COUNT(*) FROM topic_attribution_facts")).scalar_one() == 1
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_cost_source_records")).scalar_one() == 1
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_allocation_lineage_portions")).scalar_one() == 1
        assert (
            connection.execute(
                text("SELECT portion_count FROM ccloud_allocation_lineage_runs WHERE tracking_date = '2026-07-01'")
            ).scalar_one()
            == 1
        )
        raw_timestamps = connection.execute(
            text(
                "SELECT timestamp FROM ccloud_billing "
                "UNION ALL SELECT timestamp FROM billing "
                "UNION ALL SELECT timestamp FROM chargeback_facts "
                "UNION ALL SELECT timestamp FROM topic_attribution_facts "
                "UNION ALL SELECT origin_timestamp "
                "FROM ccloud_allocation_lineage_portions"
            )
        ).scalars()
        assert set(raw_timestamps) == {SECOND}
    engine.dispose()


def test_migration_029_conflicting_duplicate_aborts_before_any_row_or_version_change(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'mixed-conflict.db'}"
    _upgrade_028(url)
    _seed_mixed_financial_rows(url, conflicting_billing=True)
    before = {
        table: _row_snapshot(url, table)
        for table in (
            "billing",
            "ccloud_billing",
            "chargeback_facts",
            "topic_attribution_facts",
            "ccloud_cost_source_records",
        )
    }

    with pytest.raises(
        Exception,
        match=r"timestamp canonicalization conflict.*ccloud_billing",
    ):
        command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "028"
    engine.dispose()
    assert {table: _row_snapshot(url, table) for table in before} == before


def test_migration_029_canonicalizes_only_named_preview_json_timestamps_and_scalars(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'preview-json.db'}"
    _upgrade_028(url)
    _seed_preview_json(url)

    command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        requests = (
            connection.execute(
                text(
                    "SELECT request_id, status, created_at, completed_at, expires_at, "
                    "calculation_timestamp, source_through, calculation_coverage_json "
                    "FROM preview_requests ORDER BY request_id"
                )
            )
            .mappings()
            .all()
        )
        revisions = (
            connection.execute(
                text(
                    "SELECT revision_id, is_current, supersedes_revision_id, "
                    "superseded_by_revision_id, storage_key, manifest_metadata_json, "
                    "published_at, source_snapshot_json "
                    "FROM preview_revisions ORDER BY revision_id"
                )
            )
            .mappings()
            .all()
        )
        scalar_rows = {
            "pipeline": connection.execute(
                text("SELECT calculation_completed_at FROM pipeline_state WHERE tracking_date = '2026-07-01'")
            ).scalar_one(),
            "attempt": tuple(
                connection.execute(
                    text(
                        "SELECT refresh_start, refresh_end, started_at, completed_at "
                        "FROM ccloud_source_evidence_attempts "
                        "WHERE attempt_sequence = 71"
                    )
                ).one()
            ),
            "readiness": tuple(
                connection.execute(
                    text(
                        "SELECT window_start, window_end, captured_at "
                        "FROM ccloud_source_capture_readiness "
                        "WHERE capture_id = 'capture-1'"
                    )
                ).one()
            ),
            "readiness_history": tuple(
                connection.execute(
                    text(
                        "SELECT window_start, window_end, captured_at "
                        "FROM ccloud_source_capture_readiness_history "
                        "WHERE attempt_sequence = 71"
                    )
                ).one()
            ),
            "repair": tuple(
                connection.execute(
                    text(
                        "SELECT created_at, started_at, completed_at "
                        "FROM ccloud_focus_preview_repairs "
                        "WHERE repair_id = 'repair-1'"
                    )
                ).one()
            ),
            "repair_date": tuple(
                connection.execute(
                    text(
                        "SELECT started_at, completed_at, calculation_completed_at "
                        "FROM ccloud_focus_preview_repair_dates "
                        "WHERE repair_id = 'repair-1'"
                    )
                ).one()
            ),
            "authority": tuple(
                connection.execute(
                    text(
                        "SELECT started_at, completed_at "
                        "FROM ccloud_organization_authority_attempts "
                        "WHERE attempt_sequence = 91"
                    )
                ).one()
            ),
        }
    engine.dispose()

    assert [(row["request_id"], row["status"]) for row in requests] == [
        ("request-expired", "expired"),
        ("request-ready", "ready"),
    ]
    for row in requests:
        assert row["created_at"] == "2026-07-03 01:02:03"
        assert row["completed_at"] == "2026-07-03 01:02:04"
        assert row["expires_at"] == "2026-07-10 01:02:04"
        assert row["calculation_timestamp"] == "2026-07-03 08:02:03"
        assert row["source_through"] == "2026-07-04 02:05:06"
        coverage = json.loads(row["calculation_coverage_json"])
        assert coverage == [
            {
                "calculation_completed_at": "2026-07-03T08:02:03+00:00",
                "calculation_id": "calculation-1",
                "calculation_run_id": 41,
                "tracking_date": "2026-07-01",
            }
        ]
    assert [
        (
            row["revision_id"],
            row["is_current"],
            row["supersedes_revision_id"],
            row["superseded_by_revision_id"],
            row["storage_key"],
            json.loads(row["manifest_metadata_json"]),
        )
        for row in revisions
    ] == [
        (
            "revision-current",
            True,
            "revision-old",
            None,
            "storage-revision-current",
            {"name": "manifest.json", "sha256": "unchanged"},
        ),
        (
            "revision-old",
            False,
            None,
            "revision-current",
            "storage-revision-old",
            {"name": "manifest.json", "sha256": "unchanged"},
        ),
    ]
    for row in revisions:
        assert row["published_at"] == "2026-08-07 01:02:03"
        snapshot = json.loads(row["source_snapshot_json"])
        assert snapshot["calculation_timestamp"] == "2026-07-03T08:02:03+00:00"
        assert snapshot["calculation_coverage"][0]["calculation_completed_at"] == "2026-07-03T08:02:03+00:00"
        assert snapshot["source_through"] == "2026-07-04T02:05:06+00:00"
        assert snapshot["effective_coverage_start_date"] == "2026-07-01"
    assert scalar_rows == {
        "pipeline": "2026-07-03 01:02:03",
        "attempt": (
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
            "2026-07-03 01:02:03",
            "2026-07-03 01:02:04",
        ),
        "readiness": (
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
            "2026-07-03 01:02:04",
        ),
        "readiness_history": (
            "2026-07-01 00:00:00",
            "2026-07-02 00:00:00",
            "2026-07-03 01:02:04",
        ),
        "repair": (
            "2026-07-03 01:02:03",
            "2026-07-03 01:02:03",
            "2026-07-03 01:02:04",
        ),
        "repair_date": (
            "2026-07-03 01:02:03",
            "2026-07-03 01:02:04",
            "2026-07-03 01:02:03",
        ),
        "authority": (
            "2026-07-03 01:02:03",
            "2026-07-03 01:02:04",
        ),
    }


def test_migration_029_invalid_timestamp_rolls_back_every_table_and_version(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'invalid.db'}"
    _upgrade_028(url)
    _seed_legacy_rows(url)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("UPDATE ccloud_billing SET timestamp = 'not-a-timestamp' WHERE resource_id = 'lkc-1'"))
    engine.dispose()
    before = _row_snapshot(url, "ccloud_billing")

    with pytest.raises(
        Exception,
        match=r"ccloud_billing.*timestamp",
    ):
        command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "028"
    engine.dispose()
    assert _row_snapshot(url, "ccloud_billing") == before


@pytest.mark.parametrize(
    "parent_mutation",
    [
        "DELETE FROM ccloud_allocation_lineage_runs",
        (
            "UPDATE ccloud_allocation_lineage_runs "
            "SET capture_status = 'unavailable', capture_reason = 'capture_failed', portion_count = 0"
        ),
        "UPDATE ccloud_allocation_lineage_runs SET calculation_id = 'other-calculation'",
    ],
    ids=("missing", "non-complete", "mismatched"),
)
def test_migration_029_unsafe_lineage_parent_aborts_before_mutation(
    tmp_path: Path,
    parent_mutation: str,
) -> None:
    url = f"sqlite:///{tmp_path / 'unsafe-lineage-parent.db'}"
    _upgrade_028(url)
    _seed_lineage_collision(url)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text(parent_mutation))
    engine.dispose()
    before = {
        table: _row_snapshot(url, table)
        for table in (
            "ccloud_allocation_lineage_runs",
            "ccloud_allocation_lineage_portions",
        )
    }

    with pytest.raises(
        Exception,
        match=r"unsafe timestamp canonicalization.*missing, mismatched, or not complete",
    ):
        command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "028"
    engine.dispose()
    assert {
        table: _row_snapshot(url, table)
        for table in (
            "ccloud_allocation_lineage_runs",
            "ccloud_allocation_lineage_portions",
        )
    } == before


def test_migration_029_handles_database_without_optional_preview_tables(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'preview-disabled.db'}"
    _upgrade_028(url, preview_enabled=False)

    command.upgrade(
        _alembic_config(url, preview_enabled=False),
        "029",
    )

    engine = create_engine(url)
    try:
        table_names = set(inspect(engine).get_table_names())
        with engine.connect() as connection:
            version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        engine.dispose()
    assert version == "029"
    assert table_names >= {"preview_requests", "preview_revisions"}
    assert not (table_names & OPTIONAL_PREVIEW_TABLES)


def test_sqlite_downgrade_restores_revision_028_fractional_form_for_old_style_upsert(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade-upsert.db'}"
    _upgrade_028(url)
    _seed_legacy_rows(url)
    config = _alembic_config(url)
    command.upgrade(config, "029")
    command.downgrade(config, "028")
    engine = create_engine(url)
    legacy_table = Table(
        "ccloud_billing",
        MetaData(),
        Column("ecosystem", String, primary_key=True),
        Column("tenant_id", String, primary_key=True),
        Column("timestamp", DateTime(timezone=True), primary_key=True),
        Column("env_id", String, primary_key=True),
        Column("resource_id", String, primary_key=True),
        Column("product_type", String, primary_key=True),
        Column("product_category", String, primary_key=True),
        autoload_with=engine,
        extend_existing=True,
    )
    with engine.begin() as connection:
        raw = connection.execute(text("SELECT timestamp FROM ccloud_billing")).scalar_one()
        assert raw == FRACTIONAL_ZERO
        values = dict(connection.execute(legacy_table.select()).mappings().one())
        values["timestamp"] = datetime(2026, 7, 1, tzinfo=UTC)
        values["total_cost"] = "9"
        statement = sqlite_insert(legacy_table).values(**values)
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=[
                    "ecosystem",
                    "tenant_id",
                    "timestamp",
                    "env_id",
                    "resource_id",
                    "product_type",
                    "product_category",
                ],
                set_={"total_cost": "9"},
            )
        )
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_billing")).scalar_one() == 1
        assert connection.execute(text("SELECT total_cost FROM ccloud_billing")).scalar_one() == "9"
    engine.dispose()

    command.upgrade(config, "029")

    engine = create_engine(url)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_billing")).scalar_one() == 1
        assert connection.execute(text("SELECT timestamp FROM ccloud_billing")).scalar_one() == SECOND
        assert connection.execute(text("SELECT total_cost FROM ccloud_billing")).scalar_one() == "9"
    engine.dispose()


def test_migration_029_adds_retry_count_and_replaces_pending_index(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'retry-schema.db'}"
    _upgrade_028(url)

    command.upgrade(_alembic_config(url), "029")

    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        columns = {column["name"]: column for column in inspector.get_columns("preview_revisions")}
        indexes = {index["name"]: tuple(index["column_names"]) for index in inspector.get_indexes("preview_revisions")}
    finally:
        engine.dispose()
    assert columns["retention_retry_count"]["nullable"] is False
    assert indexes["ix_preview_revisions_owner_retention_pending"] == (
        "ecosystem",
        "tenant_id",
        "retention_retry_count",
        "retention_pending_at",
        "revision_id",
    )
