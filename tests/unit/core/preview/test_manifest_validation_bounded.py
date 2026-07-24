from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_revision_reader import _stored_revision
from tests.unit.core.preview.test_service import _ready_request


def _tampered_ready_request(
    ready: Any,
    manifest_path: Path,
    mutate: Callable[[dict[str, Any]], None],
) -> Any:
    manifest = json.loads(manifest_path.read_bytes())
    mutate(manifest)
    body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    manifest_path.write_bytes(body)
    metadata = replace(
        ready.package.manifest,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
    )
    return replace(ready, package=replace(ready.package, manifest=metadata))


@pytest.mark.parametrize(
    ("case", "mutate"),
    [
        ("tenant", lambda manifest: manifest.__setitem__("tenant_name", "other")),
        ("grain", lambda manifest: manifest.__setitem__("grain", "monthly")),
        ("interval", lambda manifest: manifest.__setitem__("start_date", "2026-06-30")),
        ("profile", lambda manifest: manifest.__setitem__("column_profile", "summary")),
        ("columns", lambda manifest: manifest.__setitem__("effective_columns", ["BilledCost"])),
        (
            "snapshot",
            lambda manifest: manifest["source_snapshot"].__setitem__(
                "source_through",
                "2026-07-04T00:00:00Z",
            ),
        ),
        (
            "validation-status",
            lambda manifest: manifest["validation"].__setitem__("status", "failed"),
        ),
        (
            "validation-integrity",
            lambda manifest: manifest["validation"].__setitem__(
                "artifact_integrity",
                "failed",
            ),
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_requested_manifest_rejects_every_semantic_correlation_before_delivery(
    tmp_path: Path,
    case: str,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    del case
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    assert ready.storage_key is not None
    manifest_path = tmp_path / "artifacts" / ready.storage_key / "manifest.json"
    tampered = _tampered_ready_request(ready, manifest_path, mutate)
    service = preview_module("service")
    try:
        with pytest.raises(service.PreviewArtifactUnavailable):
            runtime.read_manifest_bytes(tampered)
    finally:
        runtime.close()
        backend.dispose()


def test_manifest_and_file_delivery_do_not_call_path_read_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)

    def forbid_read_bytes(path: Path) -> bytes:
        raise AssertionError(f"whole-artifact read forbidden: {path}")

    monkeypatch.setattr(Path, "read_bytes", forbid_read_bytes)
    service = preview_module("service")
    monkeypatch.setattr(
        service,
        "json",
        SimpleNamespace(
            loads=lambda _body: (_ for _ in ()).throw(AssertionError("complete manifest JSON loading is forbidden"))
        ),
        raising=False,
    )
    try:
        manifest_stream = runtime.open_manifest_stream(ready)
        with manifest_stream:
            manifest_body = b"".join(manifest_stream.iter_chunks(chunk_size=17))
        file_stream = runtime.open_file_stream(ready, "cost-and-usage.csv")
        with file_stream:
            file_body = b"".join(file_stream.iter_chunks(chunk_size=19))
    finally:
        runtime.close()
        backend.dispose()

    manifest = json.loads(manifest_body)
    assert manifest["request_id"] == ready.request_id
    assert hashlib.sha256(file_body).hexdigest() == ready.package.files[0].sha256


def test_revision_manifest_and_file_delivery_use_verified_streams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revision, expected_manifest, store = _stored_revision(tmp_path)

    def forbid_read_bytes(path: Path) -> bytes:
        raise AssertionError(f"whole-artifact read forbidden: {path}")

    monkeypatch.setattr(Path, "read_bytes", forbid_read_bytes)
    reader = preview_module("revisions").PreviewRevisionReadService(artifact_store=store)

    manifest_stream = reader.open_manifest_stream(revision=revision)
    with manifest_stream:
        manifest_body = b"".join(manifest_stream.iter_chunks(chunk_size=17))
    metadata, file_stream = reader.open_file_stream(
        revision=revision,
        file_name=revision.package.files[0].name,
    )
    with file_stream:
        file_body = b"".join(file_stream.iter_chunks(chunk_size=19))

    assert manifest_body == expected_manifest
    assert metadata == revision.package.files[0]
    assert len(file_body) == metadata.size_bytes
    assert hashlib.sha256(file_body).hexdigest() == metadata.sha256
