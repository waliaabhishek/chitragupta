from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import uuid
from pathlib import Path
from typing import Protocol, runtime_checkable

from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewPackagePayload,
    PreviewStoredPackage,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class PreviewArtifactStore(Protocol):
    def finalize_package(self, *, request_id: str, package: PreviewPackagePayload) -> PreviewStoredPackage: ...

    def read_manifest(self, storage_key: str) -> bytes: ...

    def read_file(self, storage_key: str, file_name: str) -> bytes: ...

    def close(self) -> None: ...


def _safe_segment(value: str) -> str:
    if not value or value in {".", ".."} or Path(value).name != value or "/" in value or "\\" in value:
        raise ValueError("artifact identifiers must be safe basenames")
    return value


class LocalPreviewArtifactStore:
    def __init__(self, artifact_root: Path) -> None:
        self._root = artifact_root
        self._root.mkdir(parents=True, exist_ok=True)

    def finalize_package(self, *, request_id: str, package: PreviewPackagePayload) -> PreviewStoredPackage:
        del request_id
        manifest = json.loads(package.manifest_body)
        declared = manifest.get("files")
        actual = []
        metadata: list[PreviewArtifactMetadata] = []
        for item in package.data_files:
            _safe_segment(item.name)
            digest = hashlib.sha256(item.body).hexdigest()
            row = {
                "name": item.name,
                "media_type": item.media_type,
                "size_bytes": len(item.body),
                "sha256": digest,
                "order": item.order,
            }
            actual.append(row)
            metadata.append(
                PreviewArtifactMetadata(
                    name=item.name,
                    media_type=item.media_type,
                    size_bytes=len(item.body),
                    sha256=digest,
                    order=item.order,
                )
            )
        if declared != actual:
            raise ValueError("manifest file metadata does not match package bytes")

        storage_key = uuid.uuid4().hex
        staging = self._root / f".{storage_key}.staging"
        target = self._root / storage_key
        try:
            staging.mkdir()
            for name, body in [
                ("manifest.json", package.manifest_body),
                *[(x.name, x.body) for x in package.data_files],
            ]:
                path = staging / name
                with path.open("xb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
            directory_fd = os.open(staging, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            staging.rename(target)
            root_fd = os.open(self._root, os.O_RDONLY)
            try:
                os.fsync(root_fd)
            finally:
                os.close(root_fd)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

        manifest_meta = PreviewArtifactMetadata(
            name="manifest.json",
            media_type="application/json",
            size_bytes=len(package.manifest_body),
            sha256=hashlib.sha256(package.manifest_body).hexdigest(),
            order=None,
        )
        return PreviewStoredPackage(storage_key=storage_key, manifest=manifest_meta, files=tuple(metadata))

    def read_manifest(self, storage_key: str) -> bytes:
        return self._read(storage_key, "manifest.json")

    def read_file(self, storage_key: str, file_name: str) -> bytes:
        return self._read(storage_key, file_name)

    def _read(self, storage_key: str, file_name: str) -> bytes:
        return (self._root / _safe_segment(storage_key) / _safe_segment(file_name)).read_bytes()

    def close(self) -> None:
        return None
