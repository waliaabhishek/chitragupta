from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tests.unit.core.preview.conftest import preview_module


def _payload(*, manifest_sha: str | None = None) -> object:
    models = preview_module("models")
    csv_body = b"AllocatedMethodId,BilledCost\ndirect,8\n"
    csv_sha = hashlib.sha256(csv_body).hexdigest()
    manifest = {
        "schema_version": "chitragupta.preview-manifest.v1",
        "files": [
            {
                "name": "cost-and-usage.csv",
                "media_type": "text/csv",
                "size_bytes": len(csv_body),
                "sha256": manifest_sha or csv_sha,
                "order": 1,
            }
        ],
    }
    manifest_body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return models.PreviewPackagePayload(
        manifest_body=manifest_body,
        data_files=(
            models.PreviewArtifactPayload(
                name="cost-and-usage.csv",
                media_type="text/csv",
                order=1,
                body=csv_body,
            ),
        ),
    )


def test_finalize_package_is_atomic_opaque_and_returns_exact_metadata(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    payload = _payload()

    stored = store.finalize_package(request_id="public-request-id", package=payload)

    assert stored.storage_key != "public-request-id"
    assert "/" not in stored.storage_key
    assert "public-request-id" not in stored.storage_key
    assert stored.manifest.name == "manifest.json"
    assert stored.manifest.order is None
    assert stored.manifest.sha256 == hashlib.sha256(payload.manifest_body).hexdigest()
    assert stored.files[0].name == "cost-and-usage.csv"
    assert stored.files[0].sha256 == hashlib.sha256(payload.data_files[0].body).hexdigest()
    assert [path.name for path in preview_artifact_root.iterdir()] == [stored.storage_key]
    assert not list(preview_artifact_root.glob("*.staging*"))


def test_finalize_flushes_files_and_directories_then_atomically_renames(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    fsync_calls: list[int] = []
    renames: list[tuple[Path, Path]] = []
    real_fsync = artifacts.os.fsync
    real_rename = artifacts.Path.rename

    def fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    def rename(source: Path, target: Path) -> Path:
        renames.append((source, target))
        return real_rename(source, target)

    monkeypatch.setattr(artifacts.os, "fsync", fsync)
    monkeypatch.setattr(artifacts.Path, "rename", rename)

    stored = store.finalize_package(request_id="request-1", package=_payload())

    assert len(fsync_calls) == 4
    assert len(renames) == 1
    assert renames[0][0].name.endswith(".staging")
    assert renames[0][1] == preview_artifact_root / stored.storage_key


@pytest.mark.parametrize("failure_point", ["file_write", "fsync", "rename"])
def test_atomic_failure_after_staging_cleans_every_partial_path(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    if failure_point == "file_write":
        real_open = artifacts.Path.open

        def fail_data_write(path: Path, *args: object, **kwargs: object) -> object:
            if path.name == "cost-and-usage.csv":
                raise OSError("disk write failed")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(artifacts.Path, "open", fail_data_write)
    elif failure_point == "fsync":
        monkeypatch.setattr(artifacts.os, "fsync", lambda _fd: (_ for _ in ()).throw(OSError("fsync failed")))
    else:
        monkeypatch.setattr(
            artifacts.Path,
            "rename",
            lambda _source, _target: (_ for _ in ()).throw(OSError("rename failed")),
        )

    with pytest.raises(OSError, match="failed"):
        store.finalize_package(request_id="request-1", package=_payload())

    assert list(preview_artifact_root.iterdir()) == []


def test_artifact_reads_return_owned_bytes(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    payload = _payload()
    stored = store.finalize_package(request_id="request-1", package=payload)

    manifest = store.read_manifest(stored.storage_key)
    data = store.read_file(stored.storage_key, "cost-and-usage.csv")

    assert isinstance(manifest, bytes)
    assert isinstance(data, bytes)
    assert manifest == payload.manifest_body
    assert data == payload.data_files[0].body


@pytest.mark.parametrize(
    ("storage_key", "file_name"),
    [
        ("../outside", "cost-and-usage.csv"),
        ("opaque", "../secret"),
        ("opaque", "/etc/passwd"),
        ("opaque", "nested/file.csv"),
    ],
)
def test_artifact_store_rejects_path_traversal(
    preview_artifact_root: Path,
    storage_key: str,
    file_name: str,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)

    with pytest.raises(ValueError):
        store.read_file(storage_key, file_name)


def test_manifest_metadata_mismatch_cleans_staging_without_finalizing(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)

    with pytest.raises(ValueError):
        store.finalize_package(request_id="request-1", package=_payload(manifest_sha="0" * 64))

    assert not list(preview_artifact_root.iterdir())


def test_duplicate_request_finalization_never_overwrites_existing_package(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    first = store.finalize_package(request_id="request-1", package=_payload())
    first_manifest = store.read_manifest(first.storage_key)

    second = store.finalize_package(request_id="request-1", package=_payload())

    assert second.storage_key != first.storage_key
    assert store.read_manifest(first.storage_key) == first_manifest
    assert store.read_manifest(second.storage_key) == first_manifest


def test_missing_finalized_bytes_fail_explicitly(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)

    with pytest.raises(FileNotFoundError):
        store.read_manifest("opaque-but-absent")
