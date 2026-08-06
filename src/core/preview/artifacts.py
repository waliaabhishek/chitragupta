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
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Literal,
    Protocol,
    Self,
    cast,
    overload,
    runtime_checkable,
)

from core.config.fingerprint import storage_backend_fingerprint
from core.preview.models import PreviewArtifactMetadata, PreviewArtifactPayload, PreviewStoredPackage
from core.preview.spooling import (
    PreviewGenerationWorkspace,
    PreviewSpooledArtifactCollection,
    PreviewSpooledBody,
    spooled_body_metadata,
)

if TYPE_CHECKING:
    from core.config.models import TenantConfig

logger = logging.getLogger(__name__)
_ARCHIVE_SPOOL_BYTES = 8 * 1024 * 1024


class PreviewArtifactIntegrityError(OSError):
    """Stored Preview bytes no longer match their immutable metadata."""


@dataclass(frozen=True)
class PreviewArtifactOwner:
    tenant_name: str
    ecosystem: str
    tenant_id: str
    storage_backend_fingerprint: str

    def __post_init__(self) -> None:
        if not self.tenant_name.strip() or not self.ecosystem.strip() or not self.tenant_id.strip():
            raise ValueError("artifact owner fields must not be blank")
        fingerprint = self.storage_backend_fingerprint
        if len(fingerprint) != 64 or any(character not in "0123456789abcdef" for character in fingerprint):
            raise ValueError("storage backend fingerprint must be 64 lowercase hexadecimal characters")


def preview_artifact_owner(tenant_name: str, tenant_config: TenantConfig) -> PreviewArtifactOwner:
    return PreviewArtifactOwner(
        tenant_name=tenant_name,
        ecosystem=tenant_config.ecosystem,
        tenant_id=tenant_config.tenant_id,
        storage_backend_fingerprint=storage_backend_fingerprint(tenant_config.storage),
    )


def preview_owner_token(owner: PreviewArtifactOwner) -> str:
    canonical = json.dumps(
        {"ecosystem": owner.ecosystem, "tenant_id": owner.tenant_id, "tenant_name": owner.tenant_name},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def preview_storage_owner_token(owner: PreviewArtifactOwner) -> str:
    canonical = json.dumps(
        {
            "ecosystem": owner.ecosystem,
            "storage_backend_fingerprint": owner.storage_backend_fingerprint,
            "tenant_id": owner.tenant_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


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
class PreviewGenerationPackage(PreviewStagedPackage, Protocol):
    @property
    def workspace(self) -> PreviewGenerationWorkspace: ...

    def stage_data_files(self, data_files: Sequence[PreviewArtifactPayload]) -> None: ...

    def stage_metadata_file(self, metadata_file: PreviewArtifactPayload) -> None: ...


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
class PreviewVerifiedArtifactStream(Protocol):
    @property
    def size_bytes(self) -> int: ...

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]: ...

    def rewind(self) -> None: ...

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
        owner: PreviewArtifactOwner,
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

    def cleanup_staging(self, owner: PreviewArtifactOwner) -> int: ...

    def reconcile_finalized(
        self,
        *,
        owner: PreviewArtifactOwner,
        referenced_storage_keys: frozenset[str],
        is_referenced: Callable[[str], bool],
    ) -> int: ...

    def close(self) -> None: ...


@runtime_checkable
class PreviewGenerationArtifactStore(PreviewArtifactStore, Protocol):
    def begin_generation(
        self,
        *,
        owner: PreviewArtifactOwner,
        request_id: str,
        max_spool_bytes: int,
    ) -> PreviewGenerationPackage: ...


@runtime_checkable
class PreviewStreamingArtifactStore(PreviewArtifactStore, Protocol):
    def open_verified(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
    ) -> PreviewVerifiedArtifactStream: ...


@runtime_checkable
class PreviewRuntimeArtifactStore(PreviewGenerationArtifactStore, PreviewStreamingArtifactStore, Protocol):
    pass


@runtime_checkable
class PreviewArtifactMetadataLookup(Protocol):
    def find_by_name(self, file_name: str) -> PreviewArtifactMetadata | None: ...


def find_preview_artifact_metadata(
    files: Sequence[PreviewArtifactMetadata],
    file_name: str,
) -> PreviewArtifactMetadata | None:
    if isinstance(files, PreviewArtifactMetadataLookup):
        return files.find_by_name(file_name)
    return next((item for item in files if item.name == file_name), None)


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


def _remove_empty_parents(root: Path, path: Path) -> None:
    current = path
    while current != root:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _remove_empty_staging_parents(root: Path, owner_root: Path) -> None:
    try:
        owner_root.rmdir()
    except OSError:
        return
    with suppress(OSError):
        (root / ".staging").rmdir()


def _package_id_from_storage_key(storage_key: str, owner_token: str) -> str | None:
    prefix = f"v1-{owner_token}-"
    if not storage_key.startswith(prefix):
        return None
    package_id = storage_key.removeprefix(prefix)
    if len(package_id) != 32 or any(character not in "0123456789abcdef" for character in package_id):
        return None
    return package_id


class _LocalPreviewStagedPackage:
    def __init__(
        self,
        *,
        root: Path,
        staging: Path,
        lock_path: Path,
        lock_handle: BinaryIO,
        storage_key: str,
        workspace: PreviewGenerationWorkspace,
    ) -> None:
        self._root = root
        self._staging = staging
        self._staging_owner_root = staging.parent
        self._lock_path = lock_path
        self._lock_handle: BinaryIO | None = lock_handle
        self._storage_key = storage_key
        self._workspace = workspace
        self._files: tuple[PreviewArtifactMetadata, ...] | None = None
        self._published = False
        self._closed = False

    @property
    def files(self) -> tuple[PreviewArtifactMetadata, ...]:
        if self._files is None:
            raise RuntimeError("generation data files have not been staged")
        return self._files

    @property
    def workspace(self) -> PreviewGenerationWorkspace:
        return self._workspace

    def stage_data_files(self, data_files: Sequence[PreviewArtifactPayload]) -> None:
        if self._closed or self._published or self._files is not None:
            raise RuntimeError("generation package is no longer stageable")
        bounded_metadata = data_files.metadata if isinstance(data_files, PreviewSpooledArtifactCollection) else None
        metadata: list[PreviewArtifactMetadata] = []
        for item in data_files:
            item_metadata = self._stage_file(item)
            if bounded_metadata is None:
                metadata.append(item_metadata)
        file_metadata = tuple(metadata) if bounded_metadata is None else bounded_metadata
        if any(item.order != index for index, item in enumerate(file_metadata, start=1)):
            raise ValueError("package file order must be contiguous")
        if bounded_metadata is None and len({item.name for item in metadata}) != len(metadata):
            raise ValueError("package artifact names must be unique")
        self._workspace.enforce_limit()
        _fsync_directory(self._staging)
        self._files = cast("tuple[PreviewArtifactMetadata, ...]", file_metadata)

    def stage_metadata_file(self, metadata_file: PreviewArtifactPayload) -> None:
        if self._closed or self._published:
            raise RuntimeError("generation package is no longer stageable")
        if self._files is None:
            raise RuntimeError("generation data files have not been staged")
        file_metadata = (*self._files, self._stage_file(metadata_file))
        if any(item.order != index for index, item in enumerate(file_metadata, start=1)):
            raise ValueError("package file order must be contiguous")
        if len({item.name for item in file_metadata}) != len(file_metadata):
            raise ValueError("package artifact names must be unique")
        self._workspace.enforce_limit()
        _fsync_directory(self._staging)
        self._files = file_metadata

    def _stage_file(self, item: PreviewArtifactPayload) -> PreviewArtifactMetadata:
        name = _safe_segment(item.name)
        size_bytes, sha256 = spooled_body_metadata(item.body)
        item_metadata = PreviewArtifactMetadata(
            name=name,
            media_type=item.media_type,
            size_bytes=size_bytes,
            sha256=sha256,
            order=item.order,
        )
        target = self._staging / name
        staged_hasher = hashlib.sha256()
        staged_size = 0
        if isinstance(item.body, PreviewSpooledBody):
            with item.body.open() as source:
                while chunk := source.read(64 * 1024):
                    staged_hasher.update(chunk)
                    staged_size += len(chunk)
            if (staged_size, staged_hasher.hexdigest()) != (size_bytes, sha256):
                raise PreviewArtifactIntegrityError("spooled preview artifact changed during staging")
            item.body.path.rename(target)
        else:
            self._workspace.record_write(len(item.body))
            with target.open("xb") as handle:
                handle.write(item.body)
                handle.flush()
                os.fsync(handle.fileno())
            staged_hasher.update(item.body)
            staged_size = len(item.body)
            if (staged_size, staged_hasher.hexdigest()) != (size_bytes, sha256):
                raise PreviewArtifactIntegrityError("preview artifact changed during staging")
        return item_metadata

    def publish(self, *, manifest_body: bytes) -> PreviewStoredPackage:
        if self._closed or self._published:
            raise RuntimeError("staged package is no longer publishable")
        files = self.files
        if not isinstance(manifest_body, PreviewSpooledBody):
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
                for item in files
            ]
            if declared != actual:
                raise ValueError("manifest file metadata does not match package bytes")
        manifest_path = self._staging / "manifest.json"
        if isinstance(manifest_body, PreviewSpooledBody):
            manifest_body.path.rename(manifest_path)
        else:
            self._workspace.record_write(len(manifest_body))
            with manifest_path.open("xb") as handle:
                handle.write(manifest_body)
                handle.flush()
                os.fsync(handle.fileno())
        self._workspace.enforce_limit()
        _fsync_directory(self._staging)
        target = self._root / self._storage_key
        self._staging.rename(target)
        _fsync_directory(self._root)
        self._published = True
        manifest_size, manifest_sha256 = spooled_body_metadata(manifest_body)
        manifest_metadata = PreviewArtifactMetadata(
            name="manifest.json",
            media_type="application/json",
            size_bytes=manifest_size,
            sha256=manifest_sha256,
            order=None,
        )
        return PreviewStoredPackage(
            storage_key=self._storage_key,
            manifest=manifest_metadata,
            files=files,
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if not self._published and self._staging.exists():
                shutil.rmtree(self._staging)
        finally:
            try:
                self._workspace.close()
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
        with _exclusive_root_lock(self._root):
            self._lock_path.unlink(missing_ok=True)
            _remove_empty_parents(self._root, self._staging_owner_root)
            _remove_empty_parents(self._root, self._lock_path.parent)

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


class _LocalPreviewVerifiedArtifactStream:
    def __init__(self, handle: BinaryIO, size_bytes: int) -> None:
        self._handle = handle
        self._size_bytes = size_bytes
        self._closed = False

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        if self._closed:
            raise ValueError("artifact stream is closed")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        while chunk := self._handle.read(chunk_size):
            yield chunk

    def rewind(self) -> None:
        if self._closed:
            raise ValueError("artifact stream is closed")
        self._handle.seek(0)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._handle.close()

    def __enter__(self) -> Self:
        if self._closed:
            raise ValueError("artifact stream is closed")
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

    def begin_generation(
        self,
        *,
        owner: PreviewArtifactOwner,
        request_id: str,
        max_spool_bytes: int,
    ) -> PreviewGenerationPackage:
        del request_id
        package_id = uuid.uuid4().hex
        owner_token = preview_storage_owner_token(owner)
        storage_key = f"v1-{owner_token}-{package_id}"
        owner_root = self._root / ".staging" / "v1" / owner_token
        lock_root = self._root / ".locks" / "v1" / owner_token
        staging = owner_root / f"{package_id}.staging"
        workspace_path = owner_root / f"{package_id}.workspace"
        lock_path = lock_root / f"{package_id}.lock"
        lock_handle: BinaryIO | None = None
        workspace: PreviewGenerationWorkspace | None = None
        try:
            with _exclusive_root_lock(self._root):
                owner_root.mkdir(parents=True, exist_ok=True)
                lock_root.mkdir(parents=True, exist_ok=True)
                lock_handle = lock_path.open("x+b")
                _acquire_stage_lock(lock_handle)
                staging.mkdir()
                workspace = PreviewGenerationWorkspace(
                    max_spool_bytes,
                    root=workspace_path,
                    accounting_roots=(staging,),
                )
        except Exception:
            try:
                if staging.exists():
                    shutil.rmtree(staging)
                if workspace_path.exists():
                    shutil.rmtree(workspace_path)
            finally:
                if lock_handle is not None:
                    try:
                        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                    finally:
                        lock_handle.close()
                with _exclusive_root_lock(self._root):
                    lock_path.unlink(missing_ok=True)
                    _remove_empty_parents(self._root, owner_root)
                    _remove_empty_parents(self._root, lock_root)
            raise
        assert lock_handle is not None
        assert workspace is not None
        return _LocalPreviewStagedPackage(
            root=self._root,
            staging=staging,
            lock_path=lock_path,
            lock_handle=lock_handle,
            storage_key=storage_key,
            workspace=workspace,
        )

    def stage_data_files(
        self,
        *,
        owner: PreviewArtifactOwner,
        request_id: str,
        data_files: tuple[PreviewArtifactPayload, ...],
    ) -> PreviewStagedPackage:
        staged = self.begin_generation(
            owner=owner,
            request_id=request_id,
            max_spool_bytes=max(1, sum(len(item.body) for item in data_files) * 2 + 1024 * 1024),
        )
        try:
            staged.stage_data_files(data_files)
            return staged
        except BaseException:
            staged.close()
            raise

    @overload
    def read_manifest(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes: ...

    @overload
    def read_manifest(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
        *,
        stream: Literal[True],
    ) -> PreviewVerifiedArtifactStream: ...

    def read_manifest(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
        *,
        stream: bool = False,
    ) -> bytes | PreviewVerifiedArtifactStream:
        opened = self.open_verified(storage_key, metadata)
        if stream:
            return opened
        with opened:
            return b"".join(opened.iter_chunks())

    @overload
    def read_file(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes: ...

    @overload
    def read_file(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
        *,
        stream: Literal[True],
    ) -> PreviewVerifiedArtifactStream: ...

    def read_file(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
        *,
        stream: bool = False,
    ) -> bytes | PreviewVerifiedArtifactStream:
        opened = self.open_verified(storage_key, metadata)
        if stream:
            return opened
        with opened:
            return b"".join(opened.iter_chunks())

    def _read_verified(self, storage_key: str, metadata: PreviewArtifactMetadata) -> bytes:
        result = self.read_file(storage_key, metadata)
        return result

    def open_verified(
        self,
        storage_key: str,
        metadata: PreviewArtifactMetadata,
    ) -> PreviewVerifiedArtifactStream:
        path = self._root / _safe_segment(storage_key) / _safe_segment(metadata.name)
        handle = path.open("rb")
        try:
            digest = hashlib.sha256()
            size = 0
            while chunk := handle.read(64 * 1024):
                digest.update(chunk)
                size += len(chunk)
            if size != metadata.size_bytes or digest.hexdigest() != metadata.sha256:
                raise PreviewArtifactIntegrityError("stored preview artifact failed integrity verification")
            handle.seek(0)
            return _LocalPreviewVerifiedArtifactStream(handle, size)
        except BaseException:
            handle.close()
            raise

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

    def cleanup_staging(self, owner: PreviewArtifactOwner) -> int:
        removed = self._cleanup_v1_staging(owner)
        removed += self._cleanup_legacy_staging(owner)
        return removed

    def _cleanup_v1_staging(self, owner: PreviewArtifactOwner) -> int:
        removed_packages: set[str] = set()
        changed = False
        owner_token = preview_storage_owner_token(owner)
        owner_root = self._root / ".staging" / "v1" / owner_token
        lock_root = self._root / ".locks" / "v1" / owner_token
        with _exclusive_root_lock(self._root):
            if not owner_root.exists():
                return 0
            for path in owner_root.iterdir():
                name = path.name
                if (
                    path.is_dir()
                    and (name.endswith(".staging") or name.endswith(".workspace"))
                    and len(name)
                    in {
                        32 + len(".staging"),
                        32 + len(".workspace"),
                    }
                    and all(character in "0123456789abcdef" for character in name[:32])
                ):
                    package_id = name.split(".", maxsplit=1)[0]
                    lock_path = lock_root / f"{package_id}.lock"
                    lock_path.parent.mkdir(parents=True, exist_ok=True)
                    lock_handle = lock_path.open("a+b")
                    try:
                        try:
                            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                        if path.exists():
                            shutil.rmtree(path)
                            removed_packages.add(package_id)
                            changed = True
                        lock_path.unlink(missing_ok=True)
                    finally:
                        lock_handle.close()
            for lock_path in lock_root.iterdir() if lock_root.exists() else ():
                name = lock_path.name
                if (
                    lock_path.is_file()
                    and name.endswith(".lock")
                    and len(name) == 32 + len(".lock")
                    and all(character in "0123456789abcdef" for character in name[:32])
                    and not (owner_root / f"{name.removesuffix('.lock')}.staging").exists()
                    and not (owner_root / f"{name.removesuffix('.lock')}.workspace").exists()
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
            _remove_empty_parents(self._root, owner_root)
            _remove_empty_parents(self._root, lock_root)
        return len(removed_packages)

    def _cleanup_legacy_staging(self, owner: PreviewArtifactOwner) -> int:
        removed = 0
        changed = False
        owner_root = self._root / ".staging" / preview_owner_token(owner)
        with _exclusive_root_lock(self._root):
            if not owner_root.exists():
                return 0
            for path in owner_root.iterdir():
                name = path.name
                if (
                    path.is_dir()
                    and name.startswith(".")
                    and name.endswith(".staging")
                    and len(name) == 1 + 32 + len(".staging")
                    and all(character in "0123456789abcdef" for character in name[1:33])
                ):
                    lock_path = owner_root / f"{name}.lock"
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
            for lock_path in owner_root.iterdir():
                name = lock_path.name
                if (
                    lock_path.is_file()
                    and name.startswith(".")
                    and name.endswith(".staging.lock")
                    and len(name) == 1 + 32 + len(".staging.lock")
                    and all(character in "0123456789abcdef" for character in name[1:33])
                    and not (owner_root / name.removesuffix(".lock")).exists()
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
            _remove_empty_staging_parents(self._root, owner_root)
        return removed

    def reconcile_finalized(
        self,
        *,
        owner: PreviewArtifactOwner,
        referenced_storage_keys: frozenset[str],
        is_referenced: Callable[[str], bool],
    ) -> int:
        owner_token = preview_storage_owner_token(owner)
        lock_root = self._root / ".locks" / "v1" / owner_token
        removed = 0
        candidates: list[tuple[Path, str]] = []
        for path in self._root.iterdir():
            if path.is_symlink() or not path.is_dir():
                continue
            package_id = _package_id_from_storage_key(path.name, owner_token)
            if package_id is None or path.name in referenced_storage_keys:
                continue
            candidates.append((path, package_id))
        for path, package_id in candidates:
            lock_path = lock_root / f"{package_id}.lock"
            with _exclusive_root_lock(self._root):
                lock_root.mkdir(parents=True, exist_ok=True)
                lock_handle = lock_path.open("a+b")
            acquired = False
            try:
                try:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                except BlockingIOError:
                    continue
                if is_referenced(path.name):
                    continue
                if path.is_symlink() or not path.is_dir():
                    continue
                shutil.rmtree(path)
                _fsync_directory(self._root)
                removed += 1
            finally:
                if acquired:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                lock_handle.close()
                if acquired:
                    with _exclusive_root_lock(self._root):
                        lock_path.unlink(missing_ok=True)
                        _remove_empty_parents(self._root, lock_root)
        workspace_root = self._root / ".staging" / "v1" / owner_token
        with _exclusive_root_lock(self._root):
            for storage_key in referenced_storage_keys:
                package_id = _package_id_from_storage_key(storage_key, owner_token)
                if package_id is None:
                    continue
                workspace = workspace_root / f"{package_id}.workspace"
                if workspace.exists():
                    shutil.rmtree(workspace)
            _remove_empty_parents(self._root, workspace_root)
        return removed

    def close(self) -> None:
        return None
