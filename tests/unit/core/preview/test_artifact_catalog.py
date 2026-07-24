from __future__ import annotations

import hashlib
import re
import tracemalloc
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import text

from core.preview.artifacts import LocalPreviewArtifactStore, find_preview_artifact_metadata
from core.preview.generator import PreviewPackageGenerator
from core.preview.persistence import PreviewPersistedArtifactMetadataCollection
from core.preview.revisions import PreviewRevisionService
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.test_focus_preview_revision_publication import _seed_month
from tests.unit.core.preview.test_revisions import _tenant_config
from tests.unit.core.preview.test_service import _ready_request


def _forbid_complete_artifact_catalog_json(monkeypatch: pytest.MonkeyPatch) -> None:
    persistence = __import__("core.preview.persistence", fromlist=["_canonical_json"])
    canonical_json = persistence._canonical_json

    def guarded(value: object) -> str:
        if (
            isinstance(value, list)
            and value
            and all(
                isinstance(item, dict) and {"name", "media_type", "size_bytes", "sha256", "order"} <= set(item)
                for item in value
            )
        ):
            raise AssertionError("complete artifact catalogs must not be serialized to legacy JSON")
        return canonical_json(value)

    monkeypatch.setattr(persistence, "_canonical_json", guarded)


def _parent_and_catalog(backend: object, request_id: str) -> tuple[object, list[object]]:
    with backend._engine.connect() as connection:  # type: ignore[attr-defined]
        parent = connection.execute(
            text(
                """
                SELECT data_files_json, artifact_file_count, artifact_file_catalog_sha256
                FROM preview_requests
                WHERE request_id = :request_id
                """
            ),
            {"request_id": request_id},
        ).one()
        catalog = connection.execute(
            text(
                """
                SELECT name, media_type, size_bytes, sha256, file_order
                FROM preview_artifact_files
                WHERE ecosystem = 'confluent_cloud'
                  AND tenant_id = 'tenant-1'
                  AND package_kind = 'requested'
                  AND package_id = :request_id
                ORDER BY file_order
                """
            ),
            {"request_id": request_id},
        ).all()
    return parent, list(catalog)


def test_new_requested_publication_uses_only_normalized_catalog_metadata(tmp_path: Path) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    try:
        parent, catalog = _parent_and_catalog(backend, ready.request_id)
    finally:
        runtime.close()
        backend.dispose()

    assert parent.data_files_json is None
    assert parent.artifact_file_count == len(ready.package.files)
    assert re.fullmatch(r"[0-9a-f]{64}", parent.artifact_file_catalog_sha256)
    assert [tuple(row) for row in catalog] == [
        (
            item.name,
            item.media_type,
            item.size_bytes,
            item.sha256,
            item.order,
        )
        for item in ready.package.files
    ]


def test_requested_publication_never_serializes_complete_catalog_to_legacy_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _forbid_complete_artifact_catalog_json(monkeypatch)

    runtime, ready, backend, _executor = _ready_request(tmp_path)
    try:
        assert ready.status.value == "ready", ready.diagnostic
        parent, catalog = _parent_and_catalog(backend, ready.request_id)
    finally:
        runtime.close()
        backend.dispose()

    assert parent.data_files_json is None
    assert len(catalog) == parent.artifact_file_count


@pytest.mark.parametrize("corruption", ["mixed", "missing-row", "wrong-count", "wrong-digest"])
def test_requested_loader_rejects_mixed_or_partial_normalized_catalogs(
    tmp_path: Path,
    corruption: str,
) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    with backend._engine.begin() as connection:
        if corruption == "mixed":
            connection.execute(
                text("UPDATE preview_requests SET data_files_json = '[]' WHERE request_id = :request_id"),
                {"request_id": ready.request_id},
            )
        elif corruption == "missing-row":
            connection.execute(
                text(
                    """
                    DELETE FROM preview_artifact_files
                    WHERE package_kind = 'requested' AND package_id = :request_id
                    """
                ),
                {"request_id": ready.request_id},
            )
        elif corruption == "wrong-count":
            connection.execute(
                text(
                    """
                    UPDATE preview_requests
                    SET artifact_file_count = artifact_file_count + 1
                    WHERE request_id = :request_id
                    """
                ),
                {"request_id": ready.request_id},
            )
        else:
            connection.execute(
                text(
                    """
                    UPDATE preview_requests
                    SET artifact_file_catalog_sha256 = :digest
                    WHERE request_id = :request_id
                    """
                ),
                {"request_id": ready.request_id, "digest": "0" * 64},
            )
    try:
        with pytest.raises(ValueError, match="artifact|catalog|representation"):
            runtime.get_request(
                backend=backend,
                request_id=ready.request_id,
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
            )
    finally:
        runtime.close()
        backend.dispose()


def test_requested_expiry_retains_normalized_catalog_after_storage_key_cleanup(
    tmp_path: Path,
) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    assert ready.expires_at is not None
    runtime._clock = lambda: ready.expires_at  # noqa: SLF001 - lifecycle boundary probe
    parent_before, catalog_before = _parent_and_catalog(backend, ready.request_id)
    try:
        runtime.reconcile_expiry(
            backend=backend,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            request_id=ready.request_id,
        )
        with backend._engine.connect() as connection:
            storage_key = connection.execute(
                text("SELECT storage_key FROM preview_requests WHERE request_id = :request_id"),
                {"request_id": ready.request_id},
            ).scalar_one()
        parent_after, catalog_after = _parent_and_catalog(backend, ready.request_id)
    finally:
        runtime.close()
        backend.dispose()

    assert storage_key is None
    assert tuple(parent_after) == tuple(parent_before)
    assert [tuple(row) for row in catalog_after] == [tuple(row) for row in catalog_before]


def test_new_revision_publication_uses_normalized_catalog_and_retention_deletes_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'revision.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    _seed_month(backend, billed_cost=Decimal("8"))
    tenant = _tenant_config(connection_string, cutoff_days=3).model_copy(update={"retention_days": 4})
    store = LocalPreviewArtifactStore(tmp_path / "artifacts")
    publisher = PreviewRevisionService(
        artifact_store=store,
        package_generator=PreviewPackageGenerator(
            max_csv_file_bytes=None,
            max_generation_spool_bytes=2_147_483_648,
            clock=lambda: datetime(2026, 8, 4, tzinfo=UTC),
        ),
        clock=lambda: datetime(2026, 8, 4, tzinfo=UTC),
        revision_id_factory=lambda: "revision-1",
    )
    _forbid_complete_artifact_catalog_json(monkeypatch)
    try:
        revisions = publisher.publish_eligible_months(
            tenant_name="production",
            tenant_config=tenant,
            backend=backend,
            now=datetime(2026, 8, 4, tzinfo=UTC),
        )
        assert [revision.revision_id for revision in revisions] == ["revision-1"]
        with backend._engine.connect() as connection:
            parent = connection.execute(
                text(
                    """
                    SELECT file_metadata_json, artifact_file_count, artifact_file_catalog_sha256
                    FROM preview_revisions WHERE revision_id = 'revision-1'
                    """
                )
            ).one()
            catalog_count = connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM preview_artifact_files
                    WHERE ecosystem = 'confluent_cloud'
                      AND tenant_id = 'tenant-1'
                      AND package_kind = 'revision'
                      AND package_id = 'revision-1'
                    """
                )
            ).scalar_one()
        assert parent.file_metadata_json is None
        assert parent.artifact_file_count == catalog_count == len(revisions[0].package.files)
        assert re.fullmatch(r"[0-9a-f]{64}", parent.artifact_file_catalog_sha256)

        unchanged = publisher.publish_eligible_months(
            tenant_name="production",
            tenant_config=tenant,
            backend=backend,
            now=datetime(2026, 8, 4, tzinfo=UTC),
        )
        assert unchanged == ()
        artifact_root = tmp_path / "artifacts"
        assert not tuple(artifact_root.rglob("*.workspace"))
        assert not tuple(artifact_root.rglob("*.staging"))
        assert len(tuple(path for path in artifact_root.iterdir() if path.name.startswith("v1-"))) == 1

        cleanup = publisher.cleanup_retention(
            tenant_name="production",
            tenant_config=tenant,
            backend=backend,
            now=datetime(2026, 8, 6, tzinfo=UTC),
        )
        assert cleanup.deleted_count == 1
        with backend._engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM preview_artifact_files
                        WHERE package_kind = 'revision' AND package_id = 'revision-1'
                        """
                    )
                ).scalar_one()
                == 0
            )
    finally:
        store.close()
        backend.dispose()


def test_large_normalized_catalog_file_lookup_is_indexed_and_constant_memory(
    tmp_path: Path,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'catalog-lookup.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    file_count = 10_000
    target_body = b"x" * (4 * 1024 * 1024)
    target_sha256 = hashlib.sha256(target_body).hexdigest()
    artifact_root = tmp_path / "artifacts"
    storage_key = "v1-large-catalog"
    target_path = artifact_root / storage_key / "part-10000.csv"
    target_path.parent.mkdir(parents=True)
    target_path.write_bytes(target_body)
    try:
        with backend._engine.begin() as connection:
            connection.execute(
                text(
                    """
                    WITH RECURSIVE sequence(file_order) AS (
                        SELECT 1
                        UNION ALL
                        SELECT file_order + 1 FROM sequence WHERE file_order < :file_count
                    )
                    INSERT INTO preview_artifact_files
                        (ecosystem, tenant_id, package_kind, package_id, file_order,
                         name, media_type, size_bytes, sha256)
                    SELECT
                        'confluent_cloud', 'tenant-1', 'requested', 'request-large',
                        file_order, printf('part-%05d.csv', file_order), 'text/csv',
                        :size_bytes, :sha256
                    FROM sequence
                    """
                ),
                {
                    "file_count": file_count,
                    "size_bytes": len(target_body),
                    "sha256": target_sha256,
                },
            )
            query_plan = connection.execute(
                text(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT * FROM preview_artifact_files
                    WHERE ecosystem = 'confluent_cloud'
                      AND tenant_id = 'tenant-1'
                      AND package_kind = 'requested'
                      AND package_id = 'request-large'
                      AND name = 'part-10000.csv'
                    """
                )
            ).all()
        files = PreviewPersistedArtifactMetadataCollection(
            backend._engine,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            package_kind="requested",
            package_id="request-large",
            expected_count=file_count,
            expected_digest="0" * 64,
        )

        tracemalloc.start()
        found = find_preview_artifact_metadata(files, "part-10000.csv")
        missing = find_preview_artifact_metadata(files, "missing.csv")
        streamed_digest = hashlib.sha256()
        streamed_size = 0
        assert found is not None
        with LocalPreviewArtifactStore(artifact_root).open_verified(storage_key, found) as stream:
            for chunk in stream.iter_chunks():
                streamed_digest.update(chunk)
                streamed_size += len(chunk)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    finally:
        backend.dispose()

    assert (found.name, found.order, found.size_bytes) == (
        "part-10000.csv",
        file_count,
        len(target_body),
    )
    assert missing is None
    assert streamed_size == len(target_body)
    assert streamed_digest.hexdigest() == target_sha256
    assert peak < 3 * 1024 * 1024
    assert any("INDEX" in " ".join(str(value) for value in row) for row in query_plan)
