from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import shutil
import tempfile
import uuid
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Protocol, Self, cast, runtime_checkable

from core.preview.models import PreviewArtifactMetadata, PreviewArtifactPayload, PreviewStoredPackage

logger = logging.getLogger(__name__)
_ARCHIVE_SPOOL_BYTES = 8 * 1024 * 1024


class PreviewArtifactIntegrityError(OSError):
    """Stored Preview bytes no longer match their immutable metadata."""


@runtime_checkable
class PreviewStagedPackage(Protocol):
    @property
    def files(self) -> tuple[PreviewArtifactMetadata, ...]: ...

    def publish(self, *, manifest_body: bytes) -> PreviewStoredPackage: ...

    def close(self) -> None: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...


@runtime_checkable
class PreviewArchiveStream(Protocol):
    @property
    def size_bytes(self) -> int: ...

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]: ...

    def close(self) -> None: ...

    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None: ...


@runtime_checkable
class PreviewArtifactStore(Protocol):
    def stage_data_files(
        self,
        *,
        request_id: str,
        data_files: tuple[PreviewArtifactPayload, ...],
    ) -> PreviewStagedPackage: ...

    def read_manifest(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes: ...

    def read_file(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes: ...

    def open_archive(
        self,
        *,
        storage_key: str,
        manifest: PreviewArtifactMetadata,
        files: tuple[PreviewArtifactMetadata, ...],
    ) -> PreviewArchiveStream: ...

    def delete_package(self, *, storage_key: str) -> bool: ...

    def cleanup_staging(self) -> int: ...

    def close(self) -> None: ...


def _safe_segment(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or "/" in value or "\\" in value:
        raise ValueError("artifact identifiers must be safe basenames")
    return value


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@contextmanager
def _exclusive_root_lock(root: Path) -> Iterator[None]:
    root_fd = os.open(root, os.O_RDONLY)
    try:
        fcntl.flock(root_fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(root_fd, fcntl.LOCK_UN)
    finally:
        os.close(root_fd)


def _acquire_stage_lock(handle: BinaryIO) -> None:
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)


class _LocalPreviewStagedPackage:
    def __init__(
        self,
        *,
        root: Path,
        staging: Path,
        lock_path: Path,
        lock_handle: BinaryIO,
        storage_key: str,
        files: tuple[PreviewArtifactMetadata, ...],
    ) -> None:
        self._root = root
        self._staging = staging
        self._lock_path = lock_path
        self._lock_handle: BinaryIO | None = lock_handle
        self._storage_key = storage_key
        self._files = files
        self._published = False
        self._closed = False

    @property
    def files(self) -> tuple[PreviewArtifactMetadata, ...]:
        return self._files

    def publish(self, *, manifest_body: bytes) -> PreviewStoredPackage:
        if self._closed or self._published:
            raise RuntimeError("staged package is no longer publishable")
        try:
            manifest = json.loads(manifest_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("manifest is not valid JSON") from exc
        if not isinstance(manifest, dict):
            raise ValueError("manifest is not valid JSON")
        declared = manifest.get("files")
        actual = [
            {
                "name": item.name,
                "media_type": item.media_type,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "order": item.order,
            }
            for item in self._files
        ]
        if declared != actual:
            raise ValueError("manifest file metadata does not match package bytes")
        manifest_path = self._staging / "manifest.json"
        with manifest_path.open("xb") as handle:
            handle.write(manifest_body)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(self._staging)
        target = self._root / self._storage_key
        self._staging.rename(target)
        _fsync_directory(self._root)
        self._published = True
        self._release_lock()
        manifest_metadata = PreviewArtifactMetadata(
            name="manifest.json",
            media_type="application/json",
            size_bytes=len(manifest_body),
            sha256=hashlib.sha256(manifest_body).hexdigest(),
            order=None,
        )
        return PreviewStoredPackage(
            storage_key=self._storage_key,
            manifest=manifest_metadata,
            files=self._files,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if not self._published and self._staging.exists():
                shutil.rmtree(self._staging)
        finally:
            self._release_lock()

    def _release_lock(self) -> None:
        handle = self._lock_handle
        if handle is None:
            return
        self._lock_handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
        self._lock_path.unlink(missing_ok=True)

    def __enter__(self) -> Self:
        if self._closed:
            raise RuntimeError("staged package is closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()


class _LocalPreviewArchiveStream:
    def __init__(self, spool: BinaryIO, size_bytes: int) -> None:
        self._spool = spool
        self._size_bytes = size_bytes
        self._closed = False

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if self._closed:
            raise ValueError("archive stream is closed")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        while chunk := self._spool.read(chunk_size):
            yield chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._spool.close()

    def __enter__(self) -> Self:
        if self._closed:
            raise ValueError("archive stream is closed")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()


class LocalPreviewArtifactStore:
    def __init__(self, artifact_root: Path) -> None:
        self._root = artifact_root
        self._root.mkdir(parents=True, exist_ok=True)

    def stage_data_files(
        self,
        *,
        request_id: str,
        data_files: tuple[PreviewArtifactPayload, ...],
    ) -> PreviewStagedPackage:
        del request_id
        storage_key = uuid.uuid4().hex
        staging = self._root / f".{storage_key}.staging"
        lock_path = self._root / f".{storage_key}.staging.lock"
        lock_handle: BinaryIO | None = None
        metadata: list[PreviewArtifactMetadata] = []
        try:
            with _exclusive_root_lock(self._root):
                lock_handle = lock_path.open("x+b")
                _acquire_stage_lock(lock_handle)
                staging.mkdir()
            for item in data_files:
                name = _safe_segment(item.name)
                item_metadata = PreviewArtifactMetadata(
                    name=name,
                    media_type=item.media_type,
                    size_bytes=len(item.body),
                    sha256=hashlib.sha256(item.body).hexdigest(),
                    order=item.order,
                )
                metadata.append(item_metadata)
                with (staging / name).open("xb") as handle:
                    handle.write(item.body)
                    handle.flush()
                    os.fsync(handle.fileno())
            if tuple(item.order for item in metadata) != tuple(range(1, len(metadata) + 1)):
                raise ValueError("package file order must be contiguous")
            if len({item.name for item in metadata}) != len(metadata):
                raise ValueError("package artifact names must be unique")
            _fsync_directory(staging)
        except Exception:
            try:
                if staging.exists():
                    shutil.rmtree(staging)
            finally:
                if lock_handle is not None:
                    try:
                        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        lock_handle.close()
                lock_path.unlink(missing_ok=True)
            raise
        assert lock_handle is not None
        return _LocalPreviewStagedPackage(
            root=self._root,
            staging=staging,
            lock_path=lock_path,
            lock_handle=lock_handle,
            storage_key=storage_key,
            files=tuple(metadata),
        )

    def read_manifest(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes:
        return self._read_verified(storage_key, metadata)

    def read_file(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes:
        return self._read_verified(storage_key, metadata)

    def _read_verified(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes:
        body = (self._root / _safe_segment(storage_key) / _safe_segment(metadata.name)).read_bytes()
        if len(body) != metadata.size_bytes or hashlib.sha256(body).hexdigest() != metadata.sha256:
            raise PreviewArtifactIntegrityError("stored preview artifact failed integrity verification")
        return body

    def _write_zip_entry(
        self,
        archive: zipfile.ZipFile,
        *,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
    ) -> None:
        path = self._root / _safe_segment(storage_key) / _safe_segment(metadata.name)
        info = zipfile.ZipInfo(metadata.name, date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_STORED
        info.create_system = 3
        info.external_attr = 0o100644 << 16
        info.extra = b""
        info.comment = b""
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source, archive.open(info, "w") as target:
            while chunk := source.read(64 * 1024):
                target.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if size != metadata.size_bytes or digest.hexdigest() != metadata.sha256:
            raise PreviewArtifactIntegrityError("stored preview artifact failed integrity verification")

    def open_archive(
        self,
        *,
        storage_key: str,
        manifest: PreviewArtifactMetadata,
        files: tuple[PreviewArtifactMetadata, ...],
    ) -> PreviewArchiveStream:
        _safe_segment(storage_key)
        spool = tempfile.SpooledTemporaryFile(max_size=_ARCHIVE_SPOOL_BYTES, mode="w+b")  # noqa: SIM115
        try:
            with zipfile.ZipFile(spool, mode="w", compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                archive.comment = b""
                self._write_zip_entry(archive, storage_key=storage_key, metadata=manifest)
                for metadata in files:
                    self._write_zip_entry(archive, storage_key=storage_key, metadata=metadata)
            size_bytes = spool.tell()
            spool.seek(0)
            return _LocalPreviewArchiveStream(cast("BinaryIO", spool), size_bytes)
        except Exception:
            spool.close()
            raise

    def delete_package(self, *, storage_key: str) -> bool:
        target = self._root / _safe_segment(storage_key)
        if not target.exists():
            return False
        shutil.rmtree(target)
        _fsync_directory(self._root)
        return True

    def cleanup_staging(self) -> int:
        removed = 0
        changed = False
        with _exclusive_root_lock(self._root):
            for path in self._root.iterdir():
                name = path.name
                if (
                    path.is_dir()
                    and name.startswith(".")
                    and name.endswith(".staging")
                    and len(name) == 1 + 32 + len(".staging")
                    and all(character in "0123456789abcdef" for character in name[1:33])
                ):
                    lock_path = self._root / f"{name}.lock"
                    lock_handle = lock_path.open("a+b")
                    try:
                        try:
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                        if path.exists():
                            shutil.rmtree(path)
                            removed += 1
                            changed = True
                        lock_path.unlink(missing_ok=True)
                    finally:
                        lock_handle.close()
            for lock_path in self._root.iterdir():
                name = lock_path.name
                if (
                    lock_path.is_file()
                    and name.startswith(".")
                    and name.endswith(".staging.lock")
                    and len(name) == 1 + 32 + len(".staging.lock")
                    and all(character in "0123456789abcdef" for character in name[1:33])
                    and not (self._root / name.removesuffix(".lock")).exists()
                ):
                    lock_handle = lock_path.open("a+b")
                    try:
                        try:
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                        lock_path.unlink(missing_ok=True)
                        changed = True
                    finally:
                        lock_handle.close()
            if changed:
                _fsync_directory(self._root)
        return removed

    def close(self) -> None:
        return None
