from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

import anyio.to_thread
import pytest
from alembic import command
from sqlalchemy import create_engine, text

import core.preview.mapping as preview_mapping
from core.api.app import create_app
from core.config.models import ApiConfig, AppSettings, PreviewConfig, StorageConfig, TenantConfig
from core.preview.artifacts import LocalPreviewArtifactStore, PreviewArtifactOwner
from core.preview.mapping import (
    FOCUS_1_4_SUMMARY_COLUMNS,
    PreviewDataPackageDraft,
    PreviewPackageReconciliation,
)
from core.preview.models import (
    PreviewArtifactPayload,
    PreviewCalculationCoverageEntry,
    PreviewRequest,
    PreviewRequestStatus,
    PreviewSourceSnapshot,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from tests.integration.core.api.test_focus_preview import SameThreadApiClient
from tests.unit.core.preview.test_revision_mapping import assert_public_known_gaps
from tests.unit.core.storage.test_migration_019_focus_preview import _alembic_config


@pytest.fixture(autouse=True)
def _inline_startup_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    async def to_thread_inline(function: Any, *args: object, **kwargs: object) -> object:
        return function(*args, **kwargs)

    async def run_sync_inline(function: Any, *args: object, **_kwargs: object) -> object:
        return function(*args)

    monkeypatch.setattr("core.api.app.asyncio.to_thread", to_thread_inline)
    monkeypatch.setattr(anyio.to_thread, "run_sync", run_sync_inline)


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
                plugin_settings={
                    "ccloud_api": {
                        "key": "test-key",
                        "secret": "test-secret",  # pragma: allowlist secret
                    }
                },
            )
        },
    )


@dataclass(frozen=True)
class ArtifactBodies:
    manifest_body: bytes
    data_files: tuple[PreviewArtifactPayload, ...]


def _artifact_payload(*, request_id: str, manifest_fields: dict[str, object]) -> ArtifactBodies:
    csv_body = f"request_id,BilledCost\n{request_id},8.00\n".encode()
    file_metadata = {
        "name": "focus.csv",
        "media_type": "text/csv",
        "size_bytes": len(csv_body),
        "sha256": hashlib.sha256(csv_body).hexdigest(),
        "order": 1,
    }
    manifest_body = json.dumps(
        {**manifest_fields, "files": [file_metadata]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return ArtifactBodies(
        manifest_body=manifest_body,
        data_files=(
            PreviewArtifactPayload(
                name="focus.csv",
                media_type="text/csv",
                order=1,
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
) -> ArtifactBodies:
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
        expires_at=None,
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
    csv_body = f"request_id,BilledCost\n{request_id},8.00\n".encode()
    data_files = (PreviewArtifactPayload("focus.csv", "text/csv", 1, csv_body),)
    draft = PreviewDataPackageDraft(
        data_files=data_files,
        source_records=1,
        rows=1,
        reconciliation=PreviewPackageReconciliation(
            source_records=1,
            source_cost=Decimal("8"),
            allocated_cost=Decimal("8"),
            source_quantity=Decimal("1"),
            allocated_quantity=Decimal("1"),
        ),
        logical_data_sha256=hashlib.sha256(csv_body).hexdigest(),
    )
    ready_at = created_at + timedelta(minutes=2)
    expires_at = ready_at + timedelta(days=7)
    with artifact_store.stage_data_files(
        owner=PreviewArtifactOwner(
            "production",
            "confluent_cloud",
            "tenant-1",
            storage_backend_fingerprint="a" * 64,
        ),
        request_id=request_id,
        data_files=data_files,
    ) as staged:
        running_request = replace(
            request,
            status=PreviewRequestStatus.RUNNING,
            started_at=created_at + timedelta(minutes=1),
        )
        manifest_body = preview_mapping.build_requested_preview_manifest(
            request=running_request,
            snapshot=snapshot,
            draft=draft,
            files=staged.files,
            ready_at=ready_at,
            expires_at=expires_at,
        )
        stored = staged.publish(manifest_body=manifest_body)
    package = ArtifactBodies(manifest_body, data_files)
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
            ready_at,
            expires_at,
            snapshot,
            stored,
        )
        uow.commit()
    return package


def test_incomplete_internal_schema_v1_on_current_row_returns_artifact_unavailable(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'schema-v1-collision.db'}"
    artifact_root = tmp_path / "schema-v1-collision-artifacts"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    artifact_store = LocalPreviewArtifactStore(artifact_root)
    request_id = "current-row-incomplete-internal-schema-v1"
    _persist_ready_request(
        backend=backend,
        artifact_store=artifact_store,
        request_id=request_id,
        grain="daily",
        start=date(2026, 7, 1),
        end=date(2026, 7, 2),
        profile="full",
        effective_columns=preview_mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        created_at=datetime(2026, 7, 19, tzinfo=UTC),
        effective_end=date(2026, 7, 2),
        cutoff_end=None,
        monthly_status=None,
    )
    with backend.create_preview_metadata_read_unit_of_work() as uow:
        ready = uow.requests.get_for_owner(request_id, "confluent_cloud", "tenant-1")
    assert ready is not None
    assert ready.package is not None
    assert ready.storage_key is not None
    file_metadata = ready.package.files[0]
    incomplete_body = (
        json.dumps(
            {
                "schema_version": "chitragupta.preview-manifest.v1",
                "files": [
                    {
                        "name": file_metadata.name,
                        "media_type": file_metadata.media_type,
                        "size_bytes": file_metadata.size_bytes,
                        "sha256": file_metadata.sha256,
                        "order": file_metadata.order,
                    }
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    manifest_path = artifact_root / ready.storage_key / "manifest.json"
    manifest_path.write_bytes(incomplete_body)
    manifest_metadata = {
        "name": "manifest.json",
        "media_type": "application/json",
        "size_bytes": len(incomplete_body),
        "sha256": hashlib.sha256(incomplete_body).hexdigest(),
        "order": None,
    }
    with backend._engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE preview_requests
                SET manifest_metadata_json = :manifest_metadata
                WHERE request_id = :request_id
                """
            ),
            {
                "manifest_metadata": json.dumps(manifest_metadata, separators=(",", ":")),
                "request_id": request_id,
            },
        )

    app = create_app(_settings(connection_string, artifact_root))
    provider = FixedTenantBackendProvider({"production": backend})
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
        SameThreadApiClient(app) as client,
    ):
        base = f"/api/v1/tenants/production/focus-preview/requests/{request_id}"
        status = client.get(base)
        assert status.status_code == 200
        for path in (
            f"{base}/manifest",
            f"{base}/files/{file_metadata.name}",
            f"{base}/archive",
        ):
            response = client.get(path)
            assert response.status_code == 500
            assert response.json() == {"detail": "Stored preview artifact is unavailable"}
    backend.dispose()


def test_invalid_current_effective_columns_metadata_preserves_recovery_unavailable(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'invalid-effective-columns.db'}"
    artifact_root = tmp_path / "invalid-effective-columns-artifacts"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    artifact_store = LocalPreviewArtifactStore(artifact_root)
    request_id = "current-row-invalid-effective-columns"
    _persist_ready_request(
        backend=backend,
        artifact_store=artifact_store,
        request_id=request_id,
        grain="daily",
        start=date(2026, 7, 1),
        end=date(2026, 7, 2),
        profile="full",
        effective_columns=preview_mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        created_at=datetime(2026, 7, 19, tzinfo=UTC),
        effective_end=date(2026, 7, 2),
        cutoff_end=None,
        monthly_status=None,
    )
    with backend._engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE preview_requests
                SET effective_columns_json = 'not-json'
                WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        )

    app = create_app(_settings(connection_string, artifact_root))
    provider = FixedTenantBackendProvider({"production": backend})
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
        SameThreadApiClient(app) as client,
    ):
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")

    assert response.status_code == 503
    assert response.json() == {"detail": "FOCUS Mapping Preview recovery is unavailable"}
    backend.dispose()


def test_obsolete_revision_021_package_survives_upgrade_physically_but_all_delivery_fails_closed(
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
                        '2026-07-19 00:00:00.111111', '2026-07-19 00:01:00.222222',
                        '2026-07-19 00:02:00.345678',
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

    command.upgrade(migration, "024")
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        upgraded = connection.execute(
            text(
                """
                SELECT status, storage_key, manifest_metadata_json, data_files_json,
                       effective_columns_json, effective_coverage_start_date,
                       effective_coverage_end_date, availability_cutoff_end_date, monthly_status,
                       expires_at
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
    assert tuple(upgraded[4:9]) == (None, None, None, None, None)
    assert str(upgraded.expires_at) == "2026-07-26 00:02:00.345678"
    assert (storage_dir / "manifest.json").read_bytes() == manifest_body
    assert (storage_dir / "focus.csv").read_bytes() == csv_body

    app = create_app(_settings(connection_string, artifact_root))
    with SameThreadApiClient(app) as client:
        base = f"/api/v1/tenants/production/focus-preview/requests/{request_id}"
        for path in (
            base,
            f"{base}/manifest",
            f"{base}/files/focus.csv",
            f"{base}/archive",
        ):
            response = client.get(path)
            assert response.status_code == 503
            assert response.json() == {"detail": "FOCUS Mapping Preview storage is unavailable"}

    assert (storage_dir / "manifest.json").read_bytes() == manifest_body
    assert (storage_dir / "focus.csv").read_bytes() == csv_body


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
            "current-daily-custom",
            "daily",
            date(2026, 7, 1),
            date(2026, 7, 3),
            "custom",
            ("Tags", "BilledCost"),
            datetime(2026, 7, 19, tzinfo=UTC),
            date(2026, 7, 3),
            None,
            None,
            None,
        ),
        (
            "current-monthly-summary",
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
def test_current_daily_and_monthly_ready_rows_round_trip_through_sqlite_and_api(
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
    backend = SQLModelBackend(
        connection_string, CCloudStorageModule(), use_migrations=False, focus_preview_enabled=True
    )
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
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        persisted = connection.execute(
            text(
                """
                SELECT grain, start_date, end_date, column_profile, status,
                       effective_columns_json, effective_coverage_start_date,
                       effective_coverage_end_date, availability_cutoff_end_date,
                       monthly_status, manifest_metadata_json, data_files_json,
                       artifact_file_count,
                       completed_at, expires_at
                FROM preview_requests WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        ).one()
        revision_count = connection.execute(text("SELECT COUNT(*) FROM preview_revisions")).scalar_one()
        catalog_sha256 = connection.execute(
            text(
                """
                SELECT sha256 FROM preview_artifact_files
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                  AND package_kind = 'requested' AND package_id = :request_id
                  AND file_order = 1
                """
            ),
            {"request_id": request_id},
        ).scalar_one()
    engine.dispose()
    assert persisted.grain == grain
    assert str(persisted.start_date) == start.isoformat()
    assert str(persisted.end_date) == end.isoformat()
    assert persisted.column_profile == profile
    assert persisted.status == "ready"
    assert str(persisted.completed_at).startswith(
        (created_at + timedelta(minutes=2)).replace(tzinfo=None).isoformat(sep=" ")
    )
    assert str(persisted.expires_at).startswith(
        (created_at + timedelta(minutes=2, days=7)).replace(tzinfo=None).isoformat(sep=" ")
    )
    assert json.loads(persisted.effective_columns_json) == list(effective_columns)
    assert str(persisted.effective_coverage_start_date) == start.isoformat()
    assert str(persisted.effective_coverage_end_date) == effective_end.isoformat()
    assert (
        None if persisted.availability_cutoff_end_date is None else str(persisted.availability_cutoff_end_date)
    ) == (None if cutoff_end is None else cutoff_end.isoformat())
    assert persisted.monthly_status == monthly_status
    assert revision_count == 0
    assert json.loads(persisted.manifest_metadata_json)["sha256"] == hashlib.sha256(package.manifest_body).hexdigest()
    assert persisted.data_files_json is None
    assert persisted.artifact_file_count == 1
    assert catalog_sha256 == hashlib.sha256(package.data_files[0].body).hexdigest()

    app = create_app(_settings(connection_string, artifact_root))
    provider = FixedTenantBackendProvider({"production": backend})
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
        SameThreadApiClient(app) as client,
    ):
        response = client.get(f"/api/v1/tenants/production/focus-preview/requests/{request_id}")
        assert response.status_code == 200
        status = response.json()
        assert status["status"] == "ready"
        assert status["expires_at"] == (created_at + timedelta(minutes=2, days=7)).isoformat().replace("+00:00", "Z")
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
        manifest_responses = [client.get(manifest_metadata["download_url"]) for _ in range(2)]
        assert [response.status_code for response in manifest_responses] == [200, 200]
        assert [response.content for response in manifest_responses] == [
            package.manifest_body,
            package.manifest_body,
        ]
        assert_public_known_gaps(manifest_responses[0].json())
        assert client.get(file_metadata["download_url"]).content == package.data_files[0].body
        archive_response = client.get(status["package"]["download_all_url"])
        assert archive_response.status_code == 200
        with zipfile.ZipFile(io.BytesIO(archive_response.content)) as archive:
            assert archive.read("manifest.json") == package.manifest_body
    backend.dispose()


def test_already_normalized_current_package_is_byte_identical_across_supported_upgrade(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'current-upgrade.db'}"
    artifact_root = tmp_path / "current-upgrade-artifacts"
    migration = _alembic_config(connection_string)
    command.upgrade(migration, "029")
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    artifact_store = LocalPreviewArtifactStore(artifact_root)
    request_id = "already-normalized-current-package"
    package = _persist_ready_request(
        backend=backend,
        artifact_store=artifact_store,
        request_id=request_id,
        grain="daily",
        start=date(2026, 7, 1),
        end=date(2026, 7, 2),
        profile="full",
        effective_columns=preview_mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        created_at=datetime(2026, 7, 19, tzinfo=UTC),
        effective_end=date(2026, 7, 2),
        cutoff_end=None,
        monthly_status=None,
    )
    manifest = json.loads(package.manifest_body)
    assert manifest["mapping_profile_version"] == "focus-1.4-preview-v1"
    assert manifest["schema_version"] == "chitragupta.preview-manifest.v1"

    engine = create_engine(connection_string)
    with engine.connect() as connection:
        row_before = tuple(
            connection.execute(
                text(
                    """
                    SELECT storage_key, manifest_metadata_json, data_files_json,
                           artifact_file_count, artifact_file_catalog_sha256,
                           effective_columns_json, effective_coverage_start_date,
                           effective_coverage_end_date
                    FROM preview_requests WHERE request_id = :request_id
                    """
                ),
                {"request_id": request_id},
            ).one()
        )
    engine.dispose()
    storage_key = str(row_before[0])
    manifest_path = artifact_root / storage_key / "manifest.json"
    csv_path = artifact_root / storage_key / package.data_files[0].name
    physical_before = (manifest_path.read_bytes(), csv_path.read_bytes())

    app = create_app(_settings(connection_string, artifact_root))
    provider = FixedTenantBackendProvider({"production": backend})
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=provider),
        SameThreadApiClient(app) as client,
    ):
        base = f"/api/v1/tenants/production/focus-preview/requests/{request_id}"
        status_before = client.get(base).json()
        urls_before = (
            status_before["package"]["manifest"]["download_url"],
            status_before["package"]["files"][0]["download_url"],
            status_before["package"]["download_all_url"],
        )
        downloads_before = tuple(tuple(client.get(url).content for _ in range(2)) for url in urls_before)
        assert all(first == second for first, second in downloads_before)
    backend.dispose()

    command.upgrade(migration, "head")
    upgraded_backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        row_after = tuple(
            connection.execute(
                text(
                    """
                    SELECT storage_key, manifest_metadata_json, data_files_json,
                           artifact_file_count, artifact_file_catalog_sha256,
                           effective_columns_json, effective_coverage_start_date,
                           effective_coverage_end_date
                    FROM preview_requests WHERE request_id = :request_id
                    """
                ),
                {"request_id": request_id},
            ).one()
        )
    engine.dispose()
    physical_after = (manifest_path.read_bytes(), csv_path.read_bytes())

    upgraded_app = create_app(_settings(connection_string, artifact_root))
    upgraded_provider = FixedTenantBackendProvider({"production": upgraded_backend})
    with (
        patch("core.api.app.ApiTenantBackendProvider", return_value=upgraded_provider),
        SameThreadApiClient(upgraded_app) as client,
    ):
        status_after = client.get(base).json()
        urls_after = (
            status_after["package"]["manifest"]["download_url"],
            status_after["package"]["files"][0]["download_url"],
            status_after["package"]["download_all_url"],
        )
        downloads_after = tuple(tuple(client.get(url).content for _ in range(2)) for url in urls_after)
    upgraded_backend.dispose()

    assert row_after == row_before
    assert (
        physical_after
        == physical_before
        == (
            package.manifest_body,
            package.data_files[0].body,
        )
    )
    assert all(first == second for first, second in downloads_after)
    assert tuple(pair[0] for pair in downloads_after) == tuple(pair[0] for pair in downloads_before)
