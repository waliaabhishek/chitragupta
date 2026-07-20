from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import anyio.to_thread
import pytest
from alembic import command
from sqlalchemy import create_engine, text

from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.preview.artifacts import LocalPreviewArtifactStore
from core.preview.mapping import (
    FOCUS_1_4_SUMMARY_COLUMNS,
    LEGACY_DAILY_FULL_V4_COLUMNS,
)
from core.preview.models import (
    PreviewArtifactPayload,
    PreviewCalculationCoverageEntry,
    PreviewPackagePayload,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.test_focus_preview import SameThreadApiClient
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config


@pytest.fixture(autouse=True)
def _inline_startup_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)
    monkeypatch.setattr("workflow_runner.cleanup_orphaned_runs_for_all_tenants", lambda *_args, **_kwargs: None)


def _settings(connection_string: str, artifact_root: Path) -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        preview=PreviewConfig(artifact_root=artifact_root, max_workers=1),
        tenants={
            "production": TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                storage=StorageConfig(connection_string=connection_string),
                focus_preview={
                    "commercial_profile": "direct_payg",
                    "billing_currency": "USD",
                    "effective_start_date": "2020-01-01",
                    "effective_end_date": "2030-01-01",
                },
            )
        },
    )


def _artifact_payload(*, request_id: str, manifest_fields: dict[str, object]) -> PreviewPackagePayload:
    csv_body = f"request_id,BilledCost\n{request_id},8.00\n".encode()
    file_metadata = {
        "name": "focus.csv",
        "media_type": "text/csv",
        "size_bytes": len(csv_body),
        "sha256": hashlib.sha256(csv_body).hexdigest(),
        "order": 0,
    }
    manifest_body = json.dumps(
        {**manifest_fields, "files": [file_metadata]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return PreviewPackagePayload(
        manifest_body=manifest_body,
        data_files=(
            PreviewArtifactPayload(
                name="focus.csv",
                media_type="text/csv",
                order=0,
                body=csv_body,
            ),
        ),
    )


def _coverage(start: date, end: date) -> tuple[PreviewCalculationCoverageEntry, ...]:
    return tuple(
        PreviewCalculationCoverageEntry(
            tracking_date=start + timedelta(days=offset),
            calculation_id=f"calc-{(start + timedelta(days=offset)).isoformat()}",
            calculation_completed_at=datetime.combine(
                start + timedelta(days=offset),
                datetime.min.time(),
                tzinfo=UTC,
            )
            + timedelta(hours=3),
            calculation_run_id=100 + offset,
        )
        for offset in range((end - start).days)
    )


def _persist_ready_request(
    *,
    backend: SQLModelBackend,
    artifact_store: LocalPreviewArtifactStore,
    request_id: str,
    grain: str,
    start: date,
    end: date,
    profile: str,
    effective_columns: tuple[str, ...],
    created_at: datetime,
    effective_end: date,
    cutoff_end: date | None,
    monthly_status: str | None,
) -> PreviewPackagePayload:
    request = PreviewRequest(
        request_id=request_id,
        tenant_name="production",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        grain=grain,  # type: ignore[arg-type]
        start_date=start,
        end_date=end,
        column_profile=profile,  # type: ignore[arg-type]
        status=PreviewRequestStatus.QUEUED,
        created_at=created_at,
        started_at=None,
        completed_at=None,
        source_snapshot=None,
        diagnostic=None,
        storage_key=None,
        package=None,
        effective_columns=effective_columns,
    )
    coverage = _coverage(start, effective_end)
    snapshot = PreviewSourceSnapshot(
        calculation_timestamp=max((entry.calculation_completed_at for entry in coverage), default=None),
        calculation_coverage=coverage,
        source_through=(None if not coverage else datetime.combine(effective_end, datetime.min.time(), tzinfo=UTC)),
        effective_coverage_start_date=start,
        effective_coverage_end_date=effective_end,
        availability_cutoff_end_date=cutoff_end,
        monthly_status=monthly_status,  # type: ignore[arg-type]
    )
    package = _artifact_payload(
        request_id=request_id,
        manifest_fields={
            "mapping_profile_version": "focus-1.4-preview-v5",
            "request_id": request_id,
            "grain": grain,
            "column_profile": profile,
            "effective_columns": list(effective_columns),
            "effective_coverage_start_date": start.isoformat(),
            "effective_coverage_end_date": effective_end.isoformat(),
            "availability_cutoff_end_date": cutoff_end.isoformat() if cutoff_end else None,
            "monthly_status": monthly_status,
        },
    )
    stored = artifact_store.finalize_package(request_id=request_id, package=package)
    with backend.create_preview_write_unit_of_work() as uow:
        uow.requests.create_queued(request)
        uow.commit()
    with backend.create_preview_write_unit_of_work() as uow:
        running = uow.requests.mark_running(request_id, created_at + timedelta(minutes=1))
        assert running is not None
        uow.commit()
    with backend.create_preview_write_unit_of_work() as uow:
        assert uow.requests.mark_ready(
            request_id,
            created_at + timedelta(minutes=2),
            snapshot,
            stored,
        )
        uow.commit()
    return package


def test_revision_021_ready_daily_full_artifacts_survive_022_and_hydrate_through_api(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'legacy-ready.db'}"
    artifact_root = tmp_path / "legacy-artifacts"
    migration = _alembic_config(connection_string)
    command.upgrade(migration, "021")

    request_id = "legacy-ready-daily-full"
    storage_key = "legacy-storage-key"
    package = _artifact_payload(
        request_id=request_id,
        manifest_fields={
            "mapping_profile_version": "focus-1.4-preview-v4",
            "request_id": request_id,
            "grain": "daily",
            "column_profile": "full",
        },
    )
    manifest_body = package.manifest_body
    csv_body = package.data_files[0].body
    storage_dir = artifact_root / storage_key
    storage_dir.mkdir(parents=True)
    (storage_dir / "manifest.json").write_bytes(manifest_body)
    (storage_dir / "focus.csv").write_bytes(csv_body)
    manifest_metadata = {
        "name": "manifest.json",
        "media_type": "application/json",
        "size_bytes": len(manifest_body),
        "sha256": hashlib.sha256(manifest_body).hexdigest(),
        "order": None,
    }
    file_metadata = {
        "name": "focus.csv",
        "media_type": "text/csv",
        "size_bytes": len(csv_body),
        "sha256": hashlib.sha256(csv_body).hexdigest(),
        "order": 0,
    }
    calculation_timestamp = datetime(2026, 7, 1, 3, tzinfo=UTC)
    calculation_coverage = [
        {
            "tracking_date": "2026-07-01",
            "calculation_id": "legacy-calc-2026-07-01",
            "calculation_completed_at": calculation_timestamp.isoformat(),
            "calculation_run_id": 41,
        }
    ]
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests (
                    request_id, tenant_name, ecosystem, tenant_id, grain,
                    start_date, end_date, column_profile, status, created_at,
                    started_at, completed_at, calculation_timestamp, source_through,
                    calculation_coverage_json, storage_key,
                    manifest_metadata_json, data_files_json
                ) VALUES (
                    :request_id, 'production', 'confluent_cloud', 'tenant-1', 'daily',
                    '2026-07-01', '2026-07-02', 'full', 'ready',
                    '2026-07-02 00:00:00', '2026-07-02 00:01:00', '2026-07-02 00:02:00',
                    :calculation_timestamp, '2026-07-02 00:00:00',
                    :calculation_coverage_json, :storage_key,
                    :manifest_metadata_json, :data_files_json
                )
                """
            ),
            {
                "request_id": request_id,
                "calculation_timestamp": calculation_timestamp.isoformat(),
                "calculation_coverage_json": json.dumps(calculation_coverage, separators=(",", ":")),
                "storage_key": storage_key,
                "manifest_metadata_json": json.dumps(manifest_metadata, separators=(",", ":")),
                "data_files_json": json.dumps([file_metadata], separators=(",", ":")),
            },
        )
    engine.dispose()

    command.upgrade(migration, "022")
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        upgraded = connection.execute(
            text(
                """
                SELECT status, storage_key, manifest_metadata_json, data_files_json,
                       effective_columns_json, effective_coverage_start_date,
                       effective_coverage_end_date, availability_cutoff_end_date, monthly_status
                FROM preview_requests WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        ).one()
    engine.dispose()
    assert tuple(upgraded[:4]) == (
        "ready",
        storage_key,
        json.dumps(manifest_metadata, separators=(",", ":")),
        json.dumps([file_metadata], separators=(",", ":")),
    )
    assert tuple(upgraded[4:]) == (None, None, None, None, None)

    app = create_app(_settings(connection_string, artifact_root))
    with SameThreadApiClient(app) as client:
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert response.status_code == 200
        status = response.json()
        assert status["status"] == "ready"
        assert status["grain"] == "daily"
        assert status["month"] is None
        assert status["column_profile"] == "full"
        assert status["effective_columns"] == list(LEGACY_DAILY_FULL_V4_COLUMNS)
        assert status["source_snapshot"] == {
            "calculation_timestamp": calculation_timestamp.isoformat().replace("+00:00", "Z"),
            "calculation_coverage": [
                {
                    **calculation_coverage[0],
                    "calculation_completed_at": calculation_timestamp.isoformat().replace("+00:00", "Z"),
                }
            ],
            "source_through": "2026-07-02T00:00:00Z",
            "effective_coverage_start_date": "2026-07-01",
            "effective_coverage_end_date": "2026-07-02",
            "evidence_through_date": "2026-07-01",
            "availability_cutoff_end_date": None,
            "monthly_status": None,
        }
        assert status["package"]["manifest"]["sha256"] == hashlib.sha256(manifest_body).hexdigest()
        assert status["package"]["files"][0]["sha256"] == hashlib.sha256(csv_body).hexdigest()

        manifest_url = status["package"]["manifest"]["download_url"]
        file_url = status["package"]["files"][0]["download_url"]
        assert [client.get(manifest_url).content for _ in range(2)] == [manifest_body, manifest_body]
        assert [client.get(file_url).content for _ in range(2)] == [csv_body, csv_body]


@pytest.mark.parametrize(
    (
        "request_id",
        "grain",
        "start",
        "end",
        "profile",
        "effective_columns",
        "created_at",
        "effective_end",
        "cutoff_end",
        "monthly_status",
        "expected_month",
    ),
    [
        (
            "v5-daily-custom",
            "daily",
            date(2026, 7, 1),
            date(2026, 7, 3),
            "custom",
            ("Tags", "BilledCost"),
            datetime(2026, 7, 3, tzinfo=UTC),
            date(2026, 7, 3),
            None,
            None,
            None,
        ),
        (
            "v5-monthly-summary",
            "monthly",
            date(2026, 7, 1),
            date(2026, 8, 1),
            "summary",
            FOCUS_1_4_SUMMARY_COLUMNS,
            datetime(2026, 8, 2, 12, tzinfo=UTC),
            date(2026, 7, 3),
            date(2026, 7, 3),
            "provisional",
            "2026-07",
        ),
    ],
    ids=("daily", "monthly"),
)
def test_v5_daily_and_monthly_ready_rows_round_trip_through_sqlite_and_api(
    tmp_path: Path,
    request_id: str,
    grain: str,
    start: date,
    end: date,
    profile: str,
    effective_columns: tuple[str, ...],
    created_at: datetime,
    effective_end: date,
    cutoff_end: date | None,
    monthly_status: str | None,
    expected_month: str | None,
) -> None:
    connection_string = f"sqlite:///{tmp_path / f'{request_id}.db'}"
    artifact_root = tmp_path / f"{request_id}-artifacts"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    artifact_store = LocalPreviewArtifactStore(artifact_root)
    package = _persist_ready_request(
        backend=backend,
        artifact_store=artifact_store,
        request_id=request_id,
        grain=grain,
        start=start,
        end=end,
        profile=profile,
        effective_columns=effective_columns,
        created_at=created_at,
        effective_end=effective_end,
        cutoff_end=cutoff_end,
        monthly_status=monthly_status,
    )
    backend.dispose()

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        persisted = connection.execute(
            text(
                """
                SELECT grain, start_date, end_date, column_profile, status,
                       effective_columns_json, effective_coverage_start_date,
                       effective_coverage_end_date, availability_cutoff_end_date,
                       monthly_status, manifest_metadata_json, data_files_json
                FROM preview_requests WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        ).one()
    engine.dispose()
    assert persisted.grain == grain
    assert str(persisted.start_date) == start.isoformat()
    assert str(persisted.end_date) == end.isoformat()
    assert persisted.column_profile == profile
    assert persisted.status == "ready"
    assert json.loads(persisted.effective_columns_json) == list(effective_columns)
    assert str(persisted.effective_coverage_start_date) == start.isoformat()
    assert str(persisted.effective_coverage_end_date) == effective_end.isoformat()
    assert (
        None if persisted.availability_cutoff_end_date is None else str(persisted.availability_cutoff_end_date)
    ) == (None if cutoff_end is None else cutoff_end.isoformat())
    assert persisted.monthly_status == monthly_status
    assert json.loads(persisted.manifest_metadata_json)["sha256"] == hashlib.sha256(package.manifest_body).hexdigest()
    assert json.loads(persisted.data_files_json)[0]["sha256"] == hashlib.sha256(package.data_files[0].body).hexdigest()

    app = create_app(_settings(connection_string, artifact_root))
    with SameThreadApiClient(app) as client:
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert response.status_code == 200
        status = response.json()
        assert status["status"] == "ready"
        assert status["grain"] == grain
        assert status["month"] == expected_month
        assert status["column_profile"] == profile
        assert status["effective_columns"] == list(effective_columns)
        snapshot = status["source_snapshot"]
        assert snapshot["effective_coverage_start_date"] == start.isoformat()
        assert snapshot["effective_coverage_end_date"] == effective_end.isoformat()
        assert snapshot["availability_cutoff_end_date"] == (None if cutoff_end is None else cutoff_end.isoformat())
        assert snapshot["monthly_status"] == monthly_status
        manifest_metadata = status["package"]["manifest"]
        file_metadata = status["package"]["files"][0]
        assert manifest_metadata["sha256"] == hashlib.sha256(package.manifest_body).hexdigest()
        assert file_metadata["sha256"] == hashlib.sha256(package.data_files[0].body).hexdigest()
        assert client.get(manifest_metadata["download_url"]).content == package.manifest_body
        assert client.get(file_metadata["download_url"]).content == package.data_files[0].body
