from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from importlib import import_module

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, text
from sqlalchemy.dialects import postgresql, sqlite


def _timestamps() -> object:
    return import_module("core.storage.backends.sqlmodel.timestamps")


def test_canonical_utc_second_normalizes_offset_and_truncates_fraction() -> None:
    timestamps = _timestamps()
    value = datetime(
        2026,
        7,
        23,
        5,
        4,
        3,
        987_654,
        tzinfo=timezone(timedelta(hours=-7)),
    )

    canonical = timestamps.canonical_utc_second(value, field="published_at")

    assert canonical == datetime(2026, 7, 23, 12, 4, 3, tzinfo=UTC)
    assert canonical.tzinfo is UTC
    assert canonical.microsecond == 0


def test_canonical_utc_second_rejects_naive_values_with_field_name() -> None:
    timestamps = _timestamps()

    with pytest.raises(ValueError, match="created_at.*timezone-aware"):
        timestamps.canonical_utc_second(
            datetime(2026, 7, 23, 12, 4, 3),
            field="created_at",
        )


def test_utc_second_type_writes_raw_sqlite_seconds_and_reads_both_legacy_forms() -> None:
    timestamps = _timestamps()
    engine = create_engine("sqlite://")
    table = Table(
        "timestamp_probe",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("value", timestamps.UTCSecondDateTime(), nullable=False),
    )
    table.metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(
            insert(table),
            {
                "id": 1,
                "value": datetime(2026, 7, 23, 12, 4, 3, 999_999, tzinfo=UTC),
            },
        )
        connection.execute(text("INSERT INTO timestamp_probe (id, value) VALUES (2, '2026-07-23 12:04:03.000000')"))
        raw = connection.execute(text("SELECT id, value FROM timestamp_probe ORDER BY id")).all()
        hydrated = connection.execute(table.select().order_by(table.c.id)).all()

    assert raw == [
        (1, "2026-07-23 12:04:03"),
        (2, "2026-07-23 12:04:03.000000"),
    ]
    assert [row.value for row in hydrated] == [
        datetime(2026, 7, 23, 12, 4, 3, tzinfo=UTC),
        datetime(2026, 7, 23, 12, 4, 3, tzinfo=UTC),
    ]
    engine.dispose()


def test_utc_second_type_uses_native_postgresql_datetime_and_no_sqlite_text_cast() -> None:
    timestamps = _timestamps()
    column = Column("timestamp", timestamps.UTCSecondDateTime())
    value = datetime(2026, 7, 23, 12, 4, 3, 999_999, tzinfo=UTC)

    postgres_type = column.type.dialect_impl(postgresql.dialect())
    sqlite_type = column.type.dialect_impl(sqlite.dialect())
    bound = column.type.process_bind_param(value, postgresql.dialect())
    postgres_sql = str(postgres_type.compile(dialect=postgresql.dialect())).upper()
    sqlite_sql = str(sqlite_type.compile(dialect=sqlite.dialect())).upper()

    assert "DATETIME" not in postgres_sql
    assert "TIMESTAMP" in postgres_sql
    assert "DATETIME" in sqlite_sql
    assert bound == datetime(2026, 7, 23, 12, 4, 3, tzinfo=UTC)
    assert bound.tzinfo is UTC


def test_every_in_scope_financial_and_preview_scalar_uses_utc_second_type() -> None:
    timestamps = _timestamps()
    core_tables = import_module("core.storage.backends.sqlmodel.tables")
    base_tables = import_module("core.storage.backends.sqlmodel.base_tables")
    persistence = import_module("core.preview.persistence")
    plugin_tables = import_module("plugins.confluent_cloud.storage.tables")
    preview_tables = import_module("plugins.confluent_cloud.storage.preview_tables")
    expected = timestamps.UTCSecondDateTime

    columns = (
        base_tables.BillingTable.__table__.c.timestamp,
        core_tables.ChargebackFactTable.__table__.c.timestamp,
        core_tables.TopicAttributionFactTable.__table__.c.timestamp,
        core_tables.PipelineStateTable.__table__.c.calculation_completed_at,
        plugin_tables.CCloudBillingTable.__table__.c.timestamp,
        plugin_tables.CCloudCostSourceTable.__table__.c.source_period_start,
        plugin_tables.CCloudCostSourceTable.__table__.c.source_period_end,
        plugin_tables.CCloudCostSourceTable.__table__.c.collection_window_start,
        plugin_tables.CCloudCostSourceTable.__table__.c.collection_window_end,
        plugin_tables.CCloudCostSourceTable.__table__.c.evidence_scope_start,
        plugin_tables.CCloudCostSourceTable.__table__.c.evidence_scope_end,
        plugin_tables.CCloudCostSourceTable.__table__.c.allocation_timestamp,
        plugin_tables.CCloudCostSourceTable.__table__.c.retention_timestamp,
        plugin_tables.CCloudCostSourceTable.__table__.c.billing_timestamp,
        plugin_tables.CCloudAllocationLineageRunTable.__table__.c.calculation_completed_at,
        plugin_tables.CCloudAllocationLineagePortionTable.__table__.c.origin_timestamp,
        preview_tables.CCloudSourceEvidenceAttemptTable.__table__.c.refresh_start,
        preview_tables.CCloudSourceEvidenceAttemptTable.__table__.c.refresh_end,
        preview_tables.CCloudSourceEvidenceAttemptTable.__table__.c.started_at,
        preview_tables.CCloudSourceEvidenceAttemptTable.__table__.c.completed_at,
        preview_tables.CCloudSourceCaptureReadinessTable.__table__.c.window_start,
        preview_tables.CCloudSourceCaptureReadinessTable.__table__.c.window_end,
        preview_tables.CCloudSourceCaptureReadinessTable.__table__.c.captured_at,
        preview_tables.CCloudSourceCaptureReadinessHistoryTable.__table__.c.window_start,
        preview_tables.CCloudSourceCaptureReadinessHistoryTable.__table__.c.window_end,
        preview_tables.CCloudSourceCaptureReadinessHistoryTable.__table__.c.captured_at,
        preview_tables.CCloudFocusPreviewRepairTable.__table__.c.created_at,
        preview_tables.CCloudFocusPreviewRepairTable.__table__.c.started_at,
        preview_tables.CCloudFocusPreviewRepairTable.__table__.c.completed_at,
        preview_tables.CCloudFocusPreviewRepairDateTable.__table__.c.started_at,
        preview_tables.CCloudFocusPreviewRepairDateTable.__table__.c.completed_at,
        preview_tables.CCloudFocusPreviewRepairDateTable.__table__.c.calculation_completed_at,
        preview_tables.CCloudOrganizationAuthorityAttemptTable.__table__.c.started_at,
        preview_tables.CCloudOrganizationAuthorityAttemptTable.__table__.c.completed_at,
        persistence.PreviewRequestTable.__table__.c.created_at,
        persistence.PreviewRequestTable.__table__.c.started_at,
        persistence.PreviewRequestTable.__table__.c.completed_at,
        persistence.PreviewRequestTable.__table__.c.expires_at,
        persistence.PreviewRequestTable.__table__.c.lease_expires_at,
        persistence.PreviewRequestTable.__table__.c.calculation_timestamp,
        persistence.PreviewRequestTable.__table__.c.source_through,
        persistence.PreviewRevisionTable.__table__.c.published_at,
        persistence.PreviewRevisionTable.__table__.c.retention_pending_at,
    )

    assert all(isinstance(column.type, expected) for column in columns)
