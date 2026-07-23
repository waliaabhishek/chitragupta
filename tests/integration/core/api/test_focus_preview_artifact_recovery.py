from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from core.config.models import StorageConfig
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request, _snapshot
from tests.unit.core.preview.test_persistence_profiles_v5 import _stored_package
from tests.unit.core.preview.test_revision_models import _candidate, _package
from tests.unit.core.preview.test_service import (
    ControlledExecutor,
    _aggregate,
    _allocation,
    _runtime,
    _seed,
    _source,
    _tenant_config,
)


def _backend(connection_string: str) -> SQLModelBackend:
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    return backend


def _ready_request(
    backend: SQLModelBackend,
    *,
    request_id: str,
    tenant_id: str,
    storage_key: str,
) -> None:
    created_at = datetime(2026, 7, 3, tzinfo=UTC)
    with backend.create_preview_write_unit_of_work() as uow:
        uow.requests.create_queued(
            _request(
                status="queued",
                request_id=request_id,
                tenant_name="production",
                tenant_id=tenant_id,
            )
        )
        uow.commit()
    with backend.create_preview_write_unit_of_work() as uow:
        assert uow.requests.mark_running(request_id, created_at + timedelta(hours=1)) is not None
        uow.commit()
    with backend.create_preview_write_unit_of_work() as uow:
        assert uow.requests.mark_ready(
            request_id,
            created_at + timedelta(hours=2),
            created_at + timedelta(days=7, hours=2),
            _snapshot(),
            replace(_stored_package(), storage_key=storage_key),
        )
        uow.commit()


def _revision(
    backend: SQLModelBackend,
    *,
    revision_id: str,
    tenant_id: str,
    storage_key: str,
) -> None:
    with backend.create_preview_write_unit_of_work() as uow:
        uow.revisions.replace_current(
            candidate=_candidate(revision_id=revision_id, tenant_id=tenant_id),
            package=replace(_package(), storage_key=storage_key),
            expected_current_revision_id=None,
        )
        uow.commit()


def test_real_sqlite_recovery_isolates_same_provider_id_databases_and_preserves_all_references(
    tmp_path: Path,
) -> None:
    artifacts = preview_module("artifacts")
    fingerprint = __import__("core.config.fingerprint", fromlist=["storage_backend_fingerprint"])
    connection_a = f"sqlite:///{tmp_path / 'a.db'}"
    connection_b = f"sqlite:///{tmp_path / 'b.db'}"
    backend_a = _backend(connection_a)
    backend_b = _backend(connection_b)
    owner_a = artifacts.PreviewArtifactOwner(
        tenant_name="old-label",
        ecosystem="confluent_cloud",
        tenant_id="shared-provider-id",
        storage_backend_fingerprint=fingerprint.storage_backend_fingerprint(
            StorageConfig(connection_string=connection_a)
        ),
    )
    owner_a_renamed = replace(owner_a, tenant_name="new-label")
    owner_b = artifacts.PreviewArtifactOwner(
        tenant_name="database-b",
        ecosystem="confluent_cloud",
        tenant_id="shared-provider-id",
        storage_backend_fingerprint=fingerprint.storage_backend_fingerprint(
            StorageConfig(connection_string=connection_b)
        ),
    )
    token_a = artifacts.preview_storage_owner_token(owner_a)
    token_b = artifacts.preview_storage_owner_token(owner_b)
    assert artifacts.preview_storage_owner_token(owner_a_renamed) == token_a
    assert token_a != token_b

    request_a = f"v1-{token_a}-{'1' * 32}"
    revision_a = f"v1-{token_a}-{'2' * 32}"
    orphan_a = f"v1-{token_a}-{'3' * 32}"
    request_b = f"v1-{token_b}-{'4' * 32}"
    orphan_b = f"v1-{token_b}-{'5' * 32}"
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    for storage_key in (request_a, revision_a, orphan_a, request_b, orphan_b):
        package = artifact_root / storage_key
        package.mkdir()
        (package / "manifest.json").write_bytes(b"{}")

    _ready_request(
        backend_a,
        request_id="request-a",
        tenant_id="shared-provider-id",
        storage_key=request_a,
    )
    _revision(
        backend_a,
        revision_id="revision-a",
        tenant_id="shared-provider-id",
        storage_key=revision_a,
    )
    _ready_request(
        backend_b,
        request_id="request-b",
        tenant_id="shared-provider-id",
        storage_key=request_b,
    )
    store = artifacts.LocalPreviewArtifactStore(artifact_root)

    try:
        with backend_a.create_preview_metadata_read_unit_of_work() as read_uow:
            references_a = frozenset(
                read_uow.artifact_references.list_for_owner(
                    ecosystem="confluent_cloud",
                    tenant_id="shared-provider-id",
                )
            )
        assert references_a == frozenset({request_a, revision_a})

        def a_is_referenced(storage_key: str) -> bool:
            with backend_a.create_preview_metadata_read_unit_of_work() as read_uow:
                return read_uow.artifact_references.is_referenced(
                    ecosystem="confluent_cloud",
                    tenant_id="shared-provider-id",
                    storage_key=storage_key,
                )

        assert (
            store.reconcile_finalized(
                owner=owner_a_renamed,
                referenced_storage_keys=references_a,
                is_referenced=a_is_referenced,
            )
            == 1
        )
        assert not (artifact_root / orphan_a).exists()
        assert (artifact_root / request_a).is_dir()
        assert (artifact_root / revision_a).is_dir()
        assert (artifact_root / request_b).is_dir()
        assert (artifact_root / orphan_b).is_dir()

        with backend_b.create_preview_metadata_read_unit_of_work() as read_uow:
            references_b = frozenset(
                read_uow.artifact_references.list_for_owner(
                    ecosystem="confluent_cloud",
                    tenant_id="shared-provider-id",
                )
            )
        assert references_b == frozenset({request_b})
        assert (
            store.reconcile_finalized(
                owner=owner_b,
                referenced_storage_keys=references_b,
                is_referenced=lambda storage_key: storage_key in references_b,
            )
            == 1
        )
        assert not (artifact_root / orphan_b).exists()
        assert (artifact_root / request_b).is_dir()
        assert (artifact_root / request_a).is_dir()
        assert (artifact_root / revision_a).is_dir()
    finally:
        backend_a.dispose()
        backend_b.dispose()


def test_failed_ready_and_failure_recording_recover_request_row_and_finalized_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(f"sqlite:///{tmp_path / 'preview.db'}")
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    persistence = preview_module("persistence")
    artifacts = preview_module("artifacts")
    real_mark_failed = persistence.SQLModelPreviewRequestRepository.mark_failed
    calls = {"mark_failed": 0}

    def fail_ready(*_args: object, **_kwargs: object) -> bool:
        raise OSError("database unavailable after final rename")

    def fail_first_terminalization(*args: object, **kwargs: object) -> bool:
        calls["mark_failed"] += 1
        if calls["mark_failed"] == 1:
            raise OSError("failure recording interrupted")
        return real_mark_failed(*args, **kwargs)

    monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", fail_ready)
    monkeypatch.setattr(
        persistence.SQLModelPreviewRequestRepository,
        "mark_failed",
        fail_first_terminalization,
    )
    tenant_config = _tenant_config(backend._connection_string)
    owner = artifacts.preview_artifact_owner("production", tenant_config)

    try:
        queued = runtime.submit(
            tenant_name="production",
            tenant_config=tenant_config,
            backend=backend,
            start_date=_request(status="queued").start_date,
            end_date=_request(status="queued").end_date,
            grain="daily",
            column_profile="full",
            effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
        )
        executor.run_all()

        interrupted = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert interrupted.status.value == "running"
        assert interrupted.storage_key is None
        assert interrupted.package is None
        finalized_before_recovery = tuple(
            path.name for path in (tmp_path / "artifacts").iterdir() if path.name.startswith("v1-")
        )
        assert len(finalized_before_recovery) <= 1

        runtime.ensure_owner_recovered(backend=backend, owner=owner)

        terminal = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert terminal.status.value == "failed"
        assert terminal.storage_key is None
        assert terminal.package is None
        assert terminal.diagnostic.code == "preview_generation_failed"
        assert not tuple(path for path in (tmp_path / "artifacts").iterdir() if path.name.startswith("v1-"))
        with pytest.raises(FileNotFoundError):
            runtime.read_manifest_bytes(terminal)

        runtime.ensure_owner_recovered(backend=backend, owner=owner)
        engine = create_engine(backend._connection_string)
        with engine.connect() as connection:
            request_rows = (
                connection.execute(
                    text("SELECT status, storage_key FROM preview_requests WHERE request_id = :request_id"),
                    {"request_id": queued.request_id},
                )
                .mappings()
                .all()
            )
            revision_count = connection.execute(text("SELECT COUNT(*) FROM preview_revisions")).scalar_one()
        engine.dispose()
        assert request_rows == [{"status": "failed", "storage_key": None}]
        assert revision_count == 0
        assert calls["mark_failed"] == 2
    finally:
        runtime.close()
        backend.dispose()
