from __future__ import annotations

import hashlib
import io
import json
import multiprocessing
import os
import zipfile
from pathlib import Path
from threading import Event, Thread
from typing import Any

import pytest

from tests.unit.core.preview.conftest import preview_module


def _owner() -> object:
    artifacts = preview_module("artifacts")
    return artifacts.PreviewArtifactOwner(
        "production",
        "confluent_cloud",
        "tenant-1",
        storage_backend_fingerprint="a" * 64,
    )


def _cross_process_stage_worker(root: str, connection: Any, mode: str = "normal") -> None:
    from core.preview import artifacts as artifacts_module
    from core.preview.artifacts import LocalPreviewArtifactStore
    from core.preview.models import PreviewArtifactPayload

    files = (
        PreviewArtifactPayload("part-1.csv", "text/csv", 1, b"name,cost\na,1\n"),
        PreviewArtifactPayload("part-2.csv", "text/csv", 2, b"name,cost\nb,2\n"),
    )
    if mode == "pause-before-lock":
        original_acquire = artifacts_module._acquire_stage_lock

        def pause_then_acquire(handle: Any) -> None:
            connection.send("lock-visible-before-acquire")
            if connection.recv() != "acquire-stage-lock":
                raise AssertionError("parent did not release stage-lock acquisition")
            original_acquire(handle)

        artifacts_module._acquire_stage_lock = pause_then_acquire
    elif mode == "die-after-lock":
        original_acquire = artifacts_module._acquire_stage_lock

        def die_after_acquire(handle: Any) -> None:
            original_acquire(handle)
            connection.send("lock-acquired-before-staging")
            if connection.recv() != "die-before-staging":
                raise AssertionError("parent did not trigger hard death")
            os._exit(0)

        artifacts_module._acquire_stage_lock = die_after_acquire
    store = LocalPreviewArtifactStore(Path(root))
    with store.stage_data_files(owner=_owner(), request_id="process-a-request", data_files=files) as staged:
        connection.send("staged")
        command = connection.recv()
        if command == "die":
            os._exit(0)
        if command != "publish":
            raise AssertionError(f"unexpected staging command: {command}")
        stored = staged.publish(manifest_body=_manifest(files))
        connection.send(("published", stored.storage_key))
    store.close()
    connection.close()


def _data_files() -> tuple[object, ...]:
    models = preview_module("models")
    return (
        models.PreviewArtifactPayload("part-1.csv", "text/csv", 1, b"name,cost\na,1\n"),
        models.PreviewArtifactPayload("part-2.csv", "text/csv", 2, b"name,cost\nb,2\n"),
    )


def _declared(files: tuple[object, ...]) -> tuple[object, ...]:
    models = preview_module("models")
    return tuple(
        models.PreviewArtifactMetadata(
            item.name,
            item.media_type,
            len(item.body),
            hashlib.sha256(item.body).hexdigest(),
            item.order,
        )
        for item in files
    )


def _manifest(files: tuple[object, ...], *, mutate_sha: bool = False) -> bytes:
    declared = [
        {
            "name": item.name,
            "media_type": item.media_type,
            "size_bytes": item.size_bytes,
            "sha256": "0" * 64 if mutate_sha and index == 0 else item.sha256,
            "order": item.order,
        }
        for index, item in enumerate(_declared(files))
    ]
    return (
        json.dumps(
            {"schema_version": "chitragupta.preview-manifest.v2", "files": declared},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def _publish(store: object, files: tuple[object, ...] | None = None) -> tuple[object, bytes]:
    files = _data_files() if files is None else files
    manifest = _manifest(files)
    with store.stage_data_files(owner=_owner(), request_id="public-request-id", data_files=files) as staged:
        assert staged.files == _declared(files)
        stored = staged.publish(manifest_body=manifest)
    return stored, manifest


def _staging_paths(root: Path) -> tuple[Path, ...]:
    return tuple(path for path in root.rglob("*.staging") if path != root / ".staging")


def _lock_paths(root: Path) -> tuple[Path, ...]:
    return tuple(root.rglob("*.lock"))


def test_staged_publication_is_atomic_opaque_and_returns_exact_metadata(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, manifest = _publish(store)

    assert stored.storage_key != "public-request-id"
    assert "/" not in stored.storage_key
    assert "public-request-id" not in stored.storage_key
    assert stored.manifest.name == "manifest.json"
    assert stored.manifest.order is None
    assert stored.manifest.sha256 == hashlib.sha256(manifest).hexdigest()
    assert stored.files == _declared(_data_files())
    assert [path.name for path in preview_artifact_root.iterdir()] == [stored.storage_key]
    assert not list(preview_artifact_root.glob("*.staging*"))


def test_staging_fsync_order_is_data_then_directory_then_manifest_then_directory_rename_root(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    events: list[str] = []
    real_fsync = artifacts.os.fsync
    real_rename = artifacts.Path.rename

    def fsync(fd: int) -> None:
        target = Path(f"/proc/self/fd/{fd}").resolve()
        if target == preview_artifact_root:
            events.append("root-dir-fsync")
        elif target.name.endswith(".staging"):
            events.append("staging-dir-fsync")
        else:
            events.append(f"file-fsync:{target.name}")
        real_fsync(fd)

    def rename(source: Path, target: Path) -> Path:
        events.append("rename")
        return real_rename(source, target)

    monkeypatch.setattr(artifacts.os, "fsync", fsync)
    monkeypatch.setattr(artifacts.Path, "rename", rename)

    files = _data_files()
    with store.stage_data_files(owner=_owner(), request_id="request-1", data_files=files) as staged:
        assert events == [
            "file-fsync:part-1.csv",
            "file-fsync:part-2.csv",
            "staging-dir-fsync",
        ]
        staged.publish(manifest_body=_manifest(files))

    assert events == [
        "file-fsync:part-1.csv",
        "file-fsync:part-2.csv",
        "staging-dir-fsync",
        "file-fsync:manifest.json",
        "staging-dir-fsync",
        "rename",
        "root-dir-fsync",
    ]


@pytest.mark.parametrize("failure_point", ["data_write", "fsync", "publish"])
def test_unpublished_staging_is_removed_on_every_failure(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    files = _data_files()
    if failure_point == "data_write":
        real_open = artifacts.Path.open

        def fail_data_write(path: Path, *args: object, **kwargs: object) -> object:
            if path.name == "part-2.csv":
                raise OSError("data write failed")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(artifacts.Path, "open", fail_data_write)
    elif failure_point == "fsync":
        monkeypatch.setattr(artifacts.os, "fsync", lambda _fd: (_ for _ in ()).throw(OSError("fsync failed")))

    if failure_point in {"data_write", "fsync"}:
        with pytest.raises(OSError, match="failed"):
            store.stage_data_files(owner=_owner(), request_id="request-1", data_files=files)
    else:
        with (
            pytest.raises(ValueError),
            store.stage_data_files(owner=_owner(), request_id="request-1", data_files=files) as staged,
        ):
            staged.publish(manifest_body=_manifest(files, mutate_sha=True))

    assert list(preview_artifact_root.iterdir()) == []


def test_publish_rejects_manifest_metadata_drift_before_rename(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    files = _data_files()

    with (
        pytest.raises(ValueError),
        store.stage_data_files(owner=_owner(), request_id="request-1", data_files=files) as staged,
    ):
        staged.publish(manifest_body=_manifest(files, mutate_sha=True))

    assert not list(preview_artifact_root.iterdir())


@pytest.mark.parametrize("manifest_value", [None, [], "manifest", 1, True])
def test_publish_rejects_non_object_json_manifest_and_removes_staging(
    preview_artifact_root: Path,
    manifest_value: object,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    files = _data_files()
    manifest_body = (json.dumps(manifest_value) + "\n").encode()

    with (
        pytest.raises(ValueError, match="manifest"),
        store.stage_data_files(owner=_owner(), request_id="request-1", data_files=files) as staged,
    ):
        staged.publish(manifest_body=manifest_body)

    assert not list(preview_artifact_root.iterdir())


def test_verified_reads_reject_size_and_checksum_corruption(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _manifest_body = _publish(store)
    target = preview_artifact_root / stored.storage_key / stored.files[0].name

    assert store.read_file(stored.storage_key, stored.files[0]) == _data_files()[0].body
    target.write_bytes(b"corrupt")
    with pytest.raises(artifacts.PreviewArtifactIntegrityError):
        store.read_file(stored.storage_key, stored.files[0])

    manifest_path = preview_artifact_root / stored.storage_key / "manifest.json"
    manifest_path.write_bytes(b"{}\n")
    with pytest.raises(artifacts.PreviewArtifactIntegrityError):
        store.read_manifest(stored.storage_key, stored.manifest)


def test_cleanup_staging_removes_only_owned_staging_names_and_is_idempotent(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner_root = preview_artifact_root / ".staging" / artifacts.preview_owner_token(_owner())
    owner_root.mkdir(parents=True)
    owned = owner_root / f".{('a' * 32)}.staging"
    final = preview_artifact_root / ("b" * 32)
    unrelated = owner_root / ".notes.staging"
    owned.mkdir()
    final.mkdir()
    unrelated.mkdir()

    assert store.cleanup_staging(_owner()) == 1
    assert not owned.exists()
    assert final.exists()
    assert unrelated.exists()
    assert store.cleanup_staging(_owner()) == 0


def test_shared_root_startup_cleanup_skips_live_cross_process_stage_and_it_publishes(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_cross_process_stage_worker,
        args=(str(preview_artifact_root), child),
    )
    process.start()
    child.close()
    assert parent.recv() == "staged"
    staging = list(_staging_paths(preview_artifact_root))
    locks = list(_lock_paths(preview_artifact_root))
    assert len(staging) == 1
    assert len(locks) == 1

    store_b = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    try:
        store_b.cleanup_staging(_owner())
        assert staging[0].is_dir()
        assert locks[0].is_file()

        parent.send("publish")
        result = parent.recv()
        assert result[0] == "published"
        storage_key = result[1]
        process.join(5)
        assert process.exitcode == 0
        final_directories = [path for path in preview_artifact_root.iterdir() if path.is_dir()]
        assert [path.name for path in final_directories] == [storage_key]
        assert (final_directories[0] / "manifest.json").read_bytes() == _manifest(_data_files())
        assert (final_directories[0] / "part-1.csv").read_bytes() == b"name,cost\na,1\n"
        assert (final_directories[0] / "part-2.csv").read_bytes() == b"name,cost\nb,2\n"
        assert _staging_paths(preview_artifact_root) == ()
        assert _lock_paths(preview_artifact_root) == ()
    finally:
        parent.close()
        store_b.close()
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_root_serialization_prevents_cleanup_race_before_stage_lock_acquisition(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_cross_process_stage_worker,
        args=(str(preview_artifact_root), child, "pause-before-lock"),
    )
    process.start()
    child.close()
    assert parent.poll(5)
    assert parent.recv() == "lock-visible-before-acquire"
    assert _staging_paths(preview_artifact_root) == ()
    stage_locks = list(_lock_paths(preview_artifact_root))
    assert len(stage_locks) == 1

    store_b = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    cleanup_started = Event()
    cleanup_finished = Event()
    cleanup_errors: list[BaseException] = []

    def cleanup_from_process_b() -> None:
        cleanup_started.set()
        try:
            store_b.cleanup_staging(_owner())
        except BaseException as exc:
            cleanup_errors.append(exc)
        finally:
            cleanup_finished.set()

    cleanup_thread = Thread(target=cleanup_from_process_b)
    cleanup_thread.start()
    try:
        assert cleanup_started.wait(1)
        assert cleanup_finished.wait(0.1) is False
        assert stage_locks[0].is_file()

        parent.send("acquire-stage-lock")
        assert parent.poll(5)
        assert parent.recv() == "staged"
        assert cleanup_finished.wait(5)
        assert cleanup_errors == []
        assert len(_staging_paths(preview_artifact_root)) == 1
        assert stage_locks[0].is_file()

        parent.send("publish")
        assert parent.poll(5)
        result = parent.recv()
        assert result[0] == "published"
        process.join(5)
        assert process.exitcode == 0
        assert (preview_artifact_root / result[1] / "manifest.json").read_bytes() == _manifest(_data_files())
        assert _staging_paths(preview_artifact_root) == ()
        assert _lock_paths(preview_artifact_root) == ()
    finally:
        parent.close()
        store_b.close()
        cleanup_thread.join(5)
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_shared_root_startup_cleanup_reclaims_stage_after_owner_process_dies(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_cross_process_stage_worker,
        args=(str(preview_artifact_root), child),
    )
    process.start()
    child.close()
    assert parent.recv() == "staged"
    assert len(_staging_paths(preview_artifact_root)) == 1
    assert len(_lock_paths(preview_artifact_root)) == 1

    parent.send("die")
    process.join(5)
    assert process.exitcode == 0
    store_b = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    try:
        store_b.cleanup_staging(_owner())
        assert list(preview_artifact_root.iterdir()) == []
        assert store_b.cleanup_staging(_owner()) == 0
    finally:
        parent.close()
        store_b.close()
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_cleanup_reclaims_orphan_lock_after_hard_death_before_staging_directory(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_cross_process_stage_worker,
        args=(str(preview_artifact_root), child, "die-after-lock"),
    )
    process.start()
    child.close()
    assert parent.poll(5)
    assert parent.recv() == "lock-acquired-before-staging"
    orphan_locks = list(_lock_paths(preview_artifact_root))
    assert len(orphan_locks) == 1
    assert _staging_paths(preview_artifact_root) == ()

    parent.send("die-before-staging")
    process.join(5)
    assert process.exitcode == 0

    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    try:
        assert store.cleanup_staging(_owner()) == 0
        assert list(preview_artifact_root.iterdir()) == []
        stored, manifest = _publish(store)
        assert (preview_artifact_root / stored.storage_key / "manifest.json").read_bytes() == manifest
        assert _staging_paths(preview_artifact_root) == ()
        assert _lock_paths(preview_artifact_root) == ()
    finally:
        parent.close()
        store.close()
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_staging_setup_failure_releases_and_removes_cross_process_fence(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    real_mkdir = artifacts.Path.mkdir

    def fail_staging_mkdir(path: Path, *args: object, **kwargs: object) -> None:
        if path.name.endswith(".staging"):
            raise OSError("staging mkdir failed")
        real_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(artifacts.Path, "mkdir", fail_staging_mkdir)
    with pytest.raises(OSError, match="staging mkdir failed"):
        store.stage_data_files(owner=_owner(), request_id="request-1", data_files=_data_files())
    assert list(preview_artifact_root.iterdir()) == []

    monkeypatch.setattr(artifacts.Path, "mkdir", real_mkdir)
    stored, manifest = _publish(store)
    assert (preview_artifact_root / stored.storage_key / "manifest.json").read_bytes() == manifest
    assert _staging_paths(preview_artifact_root) == ()
    assert _lock_paths(preview_artifact_root) == ()


def test_delete_package_is_exact_idempotent_and_rejects_traversal(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _ = _publish(store)

    assert store.delete_package(storage_key=stored.storage_key) is True
    assert store.delete_package(storage_key=stored.storage_key) is False
    with pytest.raises(ValueError):
        store.delete_package(storage_key="../outside")


def test_delete_package_propagates_failure_for_retry(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _ = _publish(store)
    monkeypatch.setattr(artifacts.shutil, "rmtree", lambda _path: (_ for _ in ()).throw(OSError("busy")))

    with pytest.raises(OSError, match="busy"):
        store.delete_package(storage_key=stored.storage_key)


def test_archive_is_byte_deterministic_and_has_canonical_metadata(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, manifest = _publish(store)

    def archive_bytes() -> bytes:
        with store.open_archive(
            storage_key=stored.storage_key,
            manifest=stored.manifest,
            files=stored.files,
        ) as archive:
            assert archive.size_bytes > len(manifest)
            return b"".join(archive.iter_chunks(chunk_size=7))

    first = archive_bytes()
    assert first == archive_bytes()
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        infos = archive.infolist()
        assert [item.filename for item in infos] == ["manifest.json", "part-1.csv", "part-2.csv"]
        assert [item.date_time for item in infos] == [(1980, 1, 1, 0, 0, 0)] * 3
        assert [item.compress_type for item in infos] == [zipfile.ZIP_STORED] * 3
        assert [item.create_system for item in infos] == [3, 3, 3]
        assert [item.external_attr >> 16 for item in infos] == [0o100644] * 3
        assert all(item.extra == b"" and item.comment == b"" for item in infos)
        assert archive.comment == b""
        assert archive.read("manifest.json") == manifest
        assert archive.read("part-1.csv") == _data_files()[0].body


def test_archive_reads_verified_sources_incrementally_without_path_read_bytes(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _ = _publish(store)

    def forbidden_read_bytes(_path: Path) -> bytes:
        raise AssertionError("archive sources must be copied incrementally")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    with store.open_archive(storage_key=stored.storage_key, manifest=stored.manifest, files=stored.files) as archive:
        archive_bytes = b"".join(archive.iter_chunks(chunk_size=5))

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as package:
        assert package.namelist() == ["manifest.json", "part-1.csv", "part-2.csv"]


def test_archive_forces_spool_rollover_and_closes_idempotently(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = preview_module("artifacts")
    created: list[object] = []
    real_spool = artifacts.tempfile.SpooledTemporaryFile

    def tiny_spool(*args: object, **kwargs: object) -> object:
        kwargs["max_size"] = 1
        spool = real_spool(*args, **kwargs)
        created.append(spool)
        return spool

    monkeypatch.setattr(artifacts.tempfile, "SpooledTemporaryFile", tiny_spool)
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    stored, _ = _publish(store)
    archive = store.open_archive(storage_key=stored.storage_key, manifest=stored.manifest, files=stored.files)

    assert len(created) == 1
    assert created[0]._rolled is True  # type: ignore[attr-defined]
    assert b"".join(archive.iter_chunks(chunk_size=3)).startswith(b"PK")
    archive.close()
    archive.close()
    with pytest.raises(ValueError):
        tuple(archive.iter_chunks())

    with (
        pytest.raises(RuntimeError, match="consumer failed"),
        store.open_archive(
            storage_key=stored.storage_key,
            manifest=stored.manifest,
            files=stored.files,
        ),
    ):
        interrupted_spool = created[-1]
        raise RuntimeError("consumer failed")
    assert interrupted_spool.closed is True  # type: ignore[attr-defined]


def test_artifact_store_rejects_storage_key_traversal(preview_artifact_root: Path) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    metadata = preview_module("models").PreviewArtifactMetadata(
        "empty.csv", "text/csv", 0, hashlib.sha256(b"").hexdigest(), 1
    )

    with pytest.raises(ValueError):
        store.read_file("../outside", metadata)


def test_v1_publication_uses_one_stable_lock_until_metadata_transition_scope_closes(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner = _owner()
    owner_token = artifacts.preview_storage_owner_token(owner)
    staged = store.stage_data_files(
        owner=owner,
        request_id="request-1",
        data_files=_data_files(),
    )

    staging_paths = tuple((preview_artifact_root / ".staging" / "v1" / owner_token).glob("*.staging"))
    lock_paths = tuple((preview_artifact_root / ".locks" / "v1" / owner_token).glob("*.lock"))
    assert len(staging_paths) == 1
    assert len(lock_paths) == 1
    package_id = staging_paths[0].name.removesuffix(".staging")
    assert lock_paths[0].name == f"{package_id}.lock"

    stored = staged.publish(manifest_body=_manifest(_data_files()))

    assert stored.storage_key == f"v1-{owner_token}-{package_id}"
    assert not staging_paths[0].exists()
    assert lock_paths[0].is_file()
    assert (preview_artifact_root / stored.storage_key / "manifest.json").is_file()

    with lock_paths[0].open("a+b") as competing_handle, pytest.raises(BlockingIOError):
        artifacts.fcntl.flock(
            competing_handle.fileno(),
            artifacts.fcntl.LOCK_EX | artifacts.fcntl.LOCK_NB,
        )

    staged.close()
    assert not lock_paths[0].exists()


def test_finalized_recovery_preserves_every_reference_and_ignores_unverifiable_paths(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    publishing_owner = _owner()
    recovery_owner = artifacts.PreviewArtifactOwner(
        "renamed-production",
        publishing_owner.ecosystem,
        publishing_owner.tenant_id,
        storage_backend_fingerprint=publishing_owner.storage_backend_fingerprint,
    )
    owner_token = artifacts.preview_storage_owner_token(publishing_owner)
    referenced_key = f"v1-{owner_token}-{'1' * 32}"
    expired_with_key = f"v1-{owner_token}-{'2' * 32}"
    orphan_key = f"v1-{owner_token}-{'3' * 32}"
    foreign_key = f"v1-{'b' * 64}-{'4' * 32}"
    legacy_key = "5" * 32
    malformed_key = f"v1-{owner_token}-not-hex"
    for storage_key in (referenced_key, expired_with_key, orphan_key, foreign_key, legacy_key, malformed_key):
        package = preview_artifact_root / storage_key
        package.mkdir()
        (package / "manifest.json").write_bytes(b"{}")
    symlink_key = f"v1-{owner_token}-{'6' * 32}"
    (preview_artifact_root / symlink_key).symlink_to(preview_artifact_root / foreign_key, target_is_directory=True)
    file_key = f"v1-{owner_token}-{'7' * 32}"
    (preview_artifact_root / file_key).write_bytes(b"not a package")
    callbacks: list[str] = []

    removed = store.reconcile_finalized(
        owner=recovery_owner,
        referenced_storage_keys=frozenset({referenced_key, expired_with_key}),
        is_referenced=lambda storage_key: callbacks.append(storage_key) is None and False,
    )

    assert removed == 1
    assert not (preview_artifact_root / orphan_key).exists()
    assert (preview_artifact_root / referenced_key).is_dir()
    assert (preview_artifact_root / expired_with_key).is_dir()
    assert (preview_artifact_root / foreign_key).is_dir()
    assert (preview_artifact_root / legacy_key).is_dir()
    assert (preview_artifact_root / malformed_key).is_dir()
    assert (preview_artifact_root / symlink_key).is_symlink()
    assert (preview_artifact_root / file_key).is_file()
    assert callbacks == [orphan_key]


def test_finalized_recovery_rechecks_snapshot_miss_under_stable_lock(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    publishing_owner = _owner()
    recovery_owner = artifacts.PreviewArtifactOwner(
        "renamed-production",
        publishing_owner.ecosystem,
        publishing_owner.tenant_id,
        storage_backend_fingerprint=publishing_owner.storage_backend_fingerprint,
    )
    owner_token = artifacts.preview_storage_owner_token(publishing_owner)
    storage_key = f"v1-{owner_token}-{'8' * 32}"
    package = preview_artifact_root / storage_key
    package.mkdir()
    (package / "manifest.json").write_bytes(b"{}")
    callbacks: list[str] = []

    removed = store.reconcile_finalized(
        owner=recovery_owner,
        referenced_storage_keys=frozenset(),
        is_referenced=lambda candidate: callbacks.append(candidate) is None,
    )

    assert removed == 0
    assert callbacks == [storage_key]
    assert package.is_dir()


def test_finalized_recovery_preserves_candidate_and_fails_for_retry_when_reference_check_fails(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner = _owner()
    owner_token = artifacts.preview_storage_owner_token(owner)
    storage_key = f"v1-{owner_token}-{'9' * 32}"
    package = preview_artifact_root / storage_key
    package.mkdir()
    (package / "manifest.json").write_bytes(b"{}")
    calls = 0

    def fail_once(_candidate: str) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("reference read failed")
        return False

    with pytest.raises(RuntimeError, match="reference read failed"):
        store.reconcile_finalized(
            owner=owner,
            referenced_storage_keys=frozenset(),
            is_referenced=fail_once,
        )

    assert package.is_dir()
    assert (
        store.reconcile_finalized(
            owner=owner,
            referenced_storage_keys=frozenset(),
            is_referenced=fail_once,
        )
        == 1
    )
    assert calls == 2
    assert not package.exists()


@pytest.mark.parametrize("failure_point", ["delete", "fsync"])
def test_finalized_recovery_interruption_is_visible_and_retry_converges_without_touching_references(
    preview_artifact_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    owner = _owner()
    owner_token = artifacts.preview_storage_owner_token(owner)
    referenced_id = "a" * 32
    orphan_id = "b" * 32
    referenced_key = f"v1-{owner_token}-{referenced_id}"
    orphan_key = f"v1-{owner_token}-{orphan_id}"
    referenced_package = preview_artifact_root / referenced_key
    orphan_package = preview_artifact_root / orphan_key
    for package in (referenced_package, orphan_package):
        package.mkdir()
        (package / "manifest.json").write_bytes(b"{}")
    referenced_lock = preview_artifact_root / ".locks" / "v1" / owner_token / f"{referenced_id}.lock"
    referenced_lock.parent.mkdir(parents=True)
    referenced_lock.write_bytes(b"")

    if failure_point == "delete":
        real_rmtree = artifacts.shutil.rmtree
        delete_calls = 0

        def fail_delete_once(path: Path) -> None:
            nonlocal delete_calls
            delete_calls += 1
            if delete_calls == 1:
                raise OSError("delete interrupted")
            real_rmtree(path)

        monkeypatch.setattr(artifacts.shutil, "rmtree", fail_delete_once)
        error = "delete interrupted"
    else:
        real_fsync_directory = artifacts._fsync_directory
        fsync_calls = 0

        def fail_fsync_once(path: Path) -> None:
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 1:
                raise OSError("fsync interrupted")
            real_fsync_directory(path)

        monkeypatch.setattr(artifacts, "_fsync_directory", fail_fsync_once)
        error = "fsync interrupted"

    with pytest.raises(OSError, match=error):
        store.reconcile_finalized(
            owner=owner,
            referenced_storage_keys=frozenset({referenced_key}),
            is_referenced=lambda _candidate: False,
        )

    if failure_point == "delete":
        assert orphan_package.is_dir()
    else:
        assert not orphan_package.exists()
    assert referenced_package.is_dir()
    assert referenced_lock.is_file()

    assert store.reconcile_finalized(
        owner=owner,
        referenced_storage_keys=frozenset({referenced_key}),
        is_referenced=lambda _candidate: False,
    ) == (1 if failure_point == "delete" else 0)
    assert not orphan_package.exists()
    assert referenced_package.is_dir()
    assert referenced_lock.is_file()


def test_finalized_recovery_skips_live_publisher_lock_then_reclaims_after_release(
    preview_artifact_root: Path,
) -> None:
    artifacts = preview_module("artifacts")
    store = artifacts.LocalPreviewArtifactStore(preview_artifact_root)
    publishing_owner = _owner()
    recovery_owner = artifacts.PreviewArtifactOwner(
        "renamed-production",
        publishing_owner.ecosystem,
        publishing_owner.tenant_id,
        storage_backend_fingerprint=publishing_owner.storage_backend_fingerprint,
    )
    owner_token = artifacts.preview_storage_owner_token(publishing_owner)
    package_id = "c" * 32
    storage_key = f"v1-{owner_token}-{package_id}"
    package = preview_artifact_root / storage_key
    package.mkdir()
    (package / "manifest.json").write_bytes(b"{}")
    lock_path = preview_artifact_root / ".locks" / "v1" / owner_token / f"{package_id}.lock"
    lock_path.parent.mkdir(parents=True)

    with lock_path.open("a+b") as publisher_handle:
        artifacts.fcntl.flock(publisher_handle.fileno(), artifacts.fcntl.LOCK_EX)
        assert (
            store.reconcile_finalized(
                owner=recovery_owner,
                referenced_storage_keys=frozenset(),
                is_referenced=lambda _candidate: False,
            )
            == 0
        )
        assert package.is_dir()

    assert (
        store.reconcile_finalized(
            owner=recovery_owner,
            referenced_storage_keys=frozenset(),
            is_referenced=lambda _candidate: False,
        )
        == 1
    )
    assert not package.exists()
    assert not lock_path.exists()
