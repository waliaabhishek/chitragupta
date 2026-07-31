from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, text

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

TABLE_NAME = "ccloud_focus_preview_retention_outcomes"


def _backend(tmp_path: Path, name: str) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / f'{name}.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    return backend


def test_create_all_registers_retention_outcome_table_when_preview_is_enabled(
    tmp_path: Path,
) -> None:
    backend = _backend(tmp_path, "enabled")
    engine = create_engine(f"sqlite:///{tmp_path / 'enabled.db'}")
    try:
        with engine.connect() as connection:
            count = connection.execute(
                text("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = :name"),
                {"name": TABLE_NAME},
            ).scalar_one()
        assert count == 1
    finally:
        engine.dispose()
        backend.dispose()


def test_retention_outcome_repository_round_trips_latest_rows_portably(
    tmp_path: Path,
) -> None:
    from importlib import import_module

    retention = import_module("core.preview.retention")
    backend = _backend(tmp_path, "roundtrip")
    try:
        ordinary_failure = retention.PreviewRetentionOutcome(
            owner="tenant-1",
            cleanup_kind=retention.PreviewRetentionCleanupKind.ORDINARY,
            attempted_at=datetime(2026, 7, 30, 23, 25, 1, tzinfo=UTC),
            status=retention.PreviewRetentionOutcomeStatus.FAILURE,
            diagnostic=retention.PreviewRetentionDiagnostic(
                code="focus_preview_ordinary_retention_failed",
                message=(
                    "Ordinary tenant retention cleanup failed. Review worker logs and "
                    "restore tenant storage; existing valid Preview data remains available."
                ),
                error_type="OperationalError",
            ),
        )
        evidence_success = retention.PreviewRetentionOutcome(
            owner="tenant-1",
            cleanup_kind=retention.PreviewRetentionCleanupKind.PREVIEW_EVIDENCE,
            attempted_at=datetime(2026, 7, 30, 23, 40, 1, tzinfo=UTC),
            status=retention.PreviewRetentionOutcomeStatus.SUCCESS,
            diagnostic=None,
        )

        with backend.create_preview_evidence_unit_of_work() as uow:
            uow.retention_outcomes.upsert_latest(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                outcome=ordinary_failure,
            )
            uow.retention_outcomes.upsert_latest(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                outcome=evidence_success,
            )
            uow.commit()

        with backend.create_preview_generation_read_unit_of_work() as uow:
            loaded = uow.retention_outcomes.get_latest_for_owner(
                "confluent_cloud",
                "tenant-1",
            )

        assert loaded.ordinary == ordinary_failure
        assert loaded.preview_evidence == evidence_success
    finally:
        backend.dispose()


def test_corrupt_retention_outcome_row_fails_closed_for_readiness_reads(
    tmp_path: Path,
) -> None:
    from core.preview.storage_availability import PreviewEvidenceSchemaError

    backend = _backend(tmp_path, "corrupt")
    engine = create_engine(f"sqlite:///{tmp_path / 'corrupt.db'}")
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"""
                    INSERT INTO {TABLE_NAME} (
                        ecosystem,
                        tenant_id,
                        cleanup_kind,
                        attempted_at,
                        status,
                        diagnostic_code,
                        diagnostic_message,
                        diagnostic_error_type
                    ) VALUES (
                        'confluent_cloud',
                        'tenant-1',
                        'ordinary',
                        '2026-07-30 23:25:01+00:00',
                        'failure',
                        NULL,
                        NULL,
                        NULL
                    )
                    """
                )
            )

        with backend.create_preview_generation_read_unit_of_work() as uow:
            try:
                uow.retention_outcomes.get_latest_for_owner(
                    "confluent_cloud",
                    "tenant-1",
                )
            except PreviewEvidenceSchemaError:
                return
        raise AssertionError("corrupt retention outcome row must fail closed")
    finally:
        engine.dispose()
        backend.dispose()
