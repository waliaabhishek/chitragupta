from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from tests.unit.core.preview.test_revision_mapping import (
    _draft,
    _files,
    _settled_snapshot,
)


@dataclass
class _CorruptingStore:
    delegate: Any
    manifest_body: bytes
    file_reads: int = 0
    archive_opens: int = 0

    def stage_data_files(self, *, request_id: str, data_files: tuple[Any, ...]) -> Any:
        return self.delegate.stage_data_files(request_id=request_id, data_files=data_files)

    def read_manifest(self, storage_key: str, metadata: Any) -> bytes:
        del storage_key, metadata
        return self.manifest_body

    def read_file(self, storage_key: str, metadata: Any) -> bytes:
        self.file_reads += 1
        return self.delegate.read_file(storage_key, metadata)

    def open_archive(self, *, storage_key: str, manifest: Any, files: tuple[Any, ...]) -> Any:
        self.archive_opens += 1
        return self.delegate.open_archive(storage_key=storage_key, manifest=manifest, files=files)

    def delete_package(self, *, storage_key: str) -> bool:
        return self.delegate.delete_package(storage_key=storage_key)

    def cleanup_staging(self) -> int:
        return self.delegate.cleanup_staging()

    def close(self) -> None:
        self.delegate.close()


def _stored_revision(tmp_path: Path) -> tuple[Any, bytes, Any]:
    mapping = import_module("core.preview.mapping")
    models = import_module("core.preview.models")
    artifacts = import_module("core.preview.artifacts")
    draft = _draft()
    snapshot = _settled_snapshot()
    material = mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
    body = mapping.build_preview_revision_manifest(
        revision_id="revision-1",
        tenant_name_at_publication="old-label",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status="settled",
        material_sha256=material,
        supersedes_revision_id="revision-0",
        snapshot=snapshot,
        draft=draft,
        files=_files(draft),
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    store = artifacts.LocalPreviewArtifactStore(tmp_path)
    with store.stage_data_files(request_id="revision-1", data_files=draft.data_files) as staged:
        package = staged.publish(manifest_body=body)
    revision = models.PreviewRevision(
        revision_id="revision-1",
        tenant_name_at_publication="old-label",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status="settled",
        material_sha256=material,
        source_snapshot=snapshot,
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
        supersedes_revision_id="revision-0",
        superseded_by_revision_id=None,
        is_current=True,
        package=package,
    )
    return revision, body, store


def _mutate(body: bytes, field: str, *, rewrite_material: bool = False) -> bytes:
    mapping = import_module("core.preview.mapping")
    manifest = json.loads(body)
    if field == "effective_columns":
        manifest[field] = list(reversed(manifest[field]))
    elif field == "logical_data_sha256":
        manifest[field] = "e" * 64
    else:
        manifest[field] = f"{manifest[field]}-corrupt"
    if rewrite_material:
        manifest["material_sha256"] = mapping.preview_revision_content_sha256(
            mapping_profile_version=manifest["mapping_profile_version"],
            target_focus_version=manifest["target_focus_version"],
            column_profile=manifest["column_profile"],
            effective_columns=tuple(manifest["effective_columns"]),
            logical_data_sha256=manifest["logical_data_sha256"],
        )
    return json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()


def _assert_every_delivery_rejects(reader: Any, revision: Any, error_type: type[BaseException]) -> None:
    for operation in (
        lambda: reader.read_manifest(revision),
        lambda: reader.read_file(revision, "cost-and-usage.csv"),
        lambda: reader.open_archive(revision),
    ):
        with pytest.raises(error_type) as raised:
            operation()
        assert "tenant-1" not in str(raised.value)
        assert revision.package.storage_key not in str(raised.value)


@pytest.mark.parametrize(
    "field",
    [
        "mapping_profile_version",
        "target_focus_version",
        "column_profile",
        "effective_columns",
        "logical_data_sha256",
    ],
)
def test_reader_recomputes_each_material_preimage_field_before_all_delivery(tmp_path: Path, field: str) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    corrupting = _CorruptingStore(store, _mutate(body, field))
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize(
    "field",
    [
        "mapping_profile_version",
        "target_focus_version",
        "column_profile",
        "effective_columns",
        "logical_data_sha256",
    ],
)
def test_reader_rejects_self_consistent_corrupt_manifest_against_persisted_material(tmp_path: Path, field: str) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    corrupting = _CorruptingStore(store, _mutate(body, field, rewrite_material=True))
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("revision_id", "revision-other"),
        ("tenant_name", "new-label"),
        ("grain", "daily"),
        ("month", "2026-08"),
        ("start_date", "2026-07-02"),
        ("end_date", "2026-07-31"),
        ("monthly_status", "provisional"),
        ("material_sha256", "f" * 64),
        ("supersedes_revision_id", None),
        ("published_at", "2026-08-04T00:00:00.000001Z"),
    ],
)
def test_reader_rejects_persisted_manifest_identity_mismatch_on_every_delivery(
    tmp_path: Path, field: str, value: object
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    manifest[field] = value
    corrupting = _CorruptingStore(store, json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest["source_snapshot"].__setitem__("calculation_timestamp", "2026-08-03T00:00:00Z"),
        lambda manifest: manifest["source_snapshot"]["calculation_coverage"][0].__setitem__(
            "tracking_date", "2026-07-02"
        ),
        lambda manifest: manifest["source_snapshot"]["calculation_coverage"][0].__setitem__(
            "calculation_id", "calculation-corrupt"
        ),
        lambda manifest: manifest["source_snapshot"]["calculation_coverage"][0].__setitem__(
            "calculation_completed_at", "2026-08-05T00:00:00Z"
        ),
        lambda manifest: manifest["source_snapshot"]["calculation_coverage"][0].__setitem__("calculation_run_id", 999),
        lambda manifest: manifest["source_snapshot"].__setitem__("source_through", "2026-07-31T23:59:59Z"),
        lambda manifest: manifest["source_snapshot"].__setitem__("effective_coverage_start_date", "2026-07-02"),
        lambda manifest: manifest["source_snapshot"].__setitem__("effective_coverage_end_date", "2026-07-31"),
        lambda manifest: manifest["source_snapshot"].__setitem__("availability_cutoff_end_date", "2026-07-31"),
        lambda manifest: manifest["source_snapshot"].__setitem__("monthly_status", "provisional"),
        lambda manifest: manifest["files"][0].__setitem__("name", "other.csv"),
        lambda manifest: manifest["files"][0].__setitem__("media_type", "application/octet-stream"),
        lambda manifest: manifest["files"][0].__setitem__("size_bytes", 999),
        lambda manifest: manifest["files"][0].__setitem__("sha256", "f" * 64),
        lambda manifest: manifest["files"][0].__setitem__("order", 2),
        lambda manifest: manifest.__setitem__("files", []),
        lambda manifest: manifest["files"].append(dict(manifest["files"][0])),
    ],
    ids=(
        "calculation-timestamp",
        "coverage-tracking-date",
        "calculation-coverage",
        "coverage-completed-at",
        "coverage-run-id",
        "source-through",
        "effective-start",
        "effective-end",
        "availability-cutoff",
        "monthly-status",
        "file-name",
        "file-media-type",
        "file-size",
        "file-hash",
        "file-order",
        "missing-file",
        "extra-file",
    ),
)
def test_reader_rejects_complete_source_snapshot_or_ordered_file_declaration_mismatch(
    tmp_path: Path, mutate: Any
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    mutate(manifest)
    corrupting = _CorruptingStore(store, json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)
    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize(
    "manifest_body",
    [
        b"not-json",
        b"[]",
        json.dumps({"mapping_profile_version": 7}).encode(),
    ],
)
def test_reader_maps_malformed_manifest_to_one_redacted_artifact_error(tmp_path: Path, manifest_body: bytes) -> None:
    revisions = import_module("core.preview.revisions")
    revision, _body, store = _stored_revision(tmp_path)
    corrupting = _CorruptingStore(store, manifest_body)
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("mapping_profile_version", 7),
        ("target_focus_version", None),
        ("column_profile", []),
        ("effective_columns", "BilledCost"),
        ("logical_data_sha256", 64),
        ("material_sha256", True),
    ],
)
@pytest.mark.parametrize("mode", ["missing", "wrong-type"])
def test_reader_rejects_missing_or_wrong_type_material_fields_before_every_delivery(
    tmp_path: Path,
    field: str,
    wrong_value: object,
    mode: str,
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    if mode == "missing":
        manifest.pop(field)
    else:
        manifest[field] = wrong_value
    corrupting = _CorruptingStore(
        store,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
    )
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize("field", ["logical_data_sha256", "material_sha256"])
@pytest.mark.parametrize("value", ["A" * 64, "a" * 63, "g" * 64], ids=["uppercase", "short", "non-hex"])
def test_reader_rejects_noncanonical_digest_shapes_before_every_delivery(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    manifest[field] = value
    corrupting = _CorruptingStore(
        store,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
    )
    reader = revisions.PreviewRevisionReadService(artifact_store=corrupting)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)
    assert corrupting.file_reads == 0
    assert corrupting.archive_opens == 0


@pytest.mark.parametrize("damage", ["delete", "corrupt"])
def test_real_missing_or_corrupt_data_file_is_redacted_for_file_and_archive_delivery(
    tmp_path: Path,
    damage: str,
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, _body, store = _stored_revision(tmp_path)
    data_path = tmp_path / revision.package.storage_key / revision.package.files[0].name
    if damage == "delete":
        data_path.unlink()
    else:
        data_path.write_bytes(b"private corrupt tenant-1 bytes")
    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    for operation in (
        lambda: reader.read_file(revision, revision.package.files[0].name),
        lambda: reader.open_archive(revision),
    ):
        with pytest.raises(revisions.PreviewRevisionArtifactUnavailableError) as raised:
            operation()
        assert "tenant-1" not in str(raised.value)
        assert str(data_path) not in str(raised.value)


def test_real_missing_manifest_is_redacted_before_every_delivery(tmp_path: Path) -> None:
    revisions = import_module("core.preview.revisions")
    revision, _body, store = _stored_revision(tmp_path)
    manifest_path = tmp_path / revision.package.storage_key / revision.package.manifest.name
    manifest_path.unlink()
    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    _assert_every_delivery_rejects(reader, revision, revisions.PreviewRevisionArtifactUnavailableError)


def test_archive_open_failure_is_redacted_without_leaking_storage_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    revisions = import_module("core.preview.revisions")
    revision, _body, store = _stored_revision(tmp_path)

    def fail_open(**kwargs: object) -> object:
        del kwargs
        raise OSError(f"private {revision.package.storage_key} tenant-1")

    monkeypatch.setattr(store, "open_archive", fail_open)
    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    with pytest.raises(revisions.PreviewRevisionArtifactUnavailableError) as raised:
        reader.open_archive(revision)
    assert revision.package.storage_key not in str(raised.value)
    assert "tenant-1" not in str(raised.value)


@pytest.mark.parametrize("operation", ["manifest", "file", "archive"])
def test_every_delivery_calls_shared_revision_invariant_before_artifact_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    revisions = import_module("core.preview.revisions")
    models = import_module("core.preview.models")
    revision, body, store = _stored_revision(tmp_path)
    calls: list[str] = []
    original = models.validate_preview_revision_invariant

    def capture(**kwargs: object) -> None:
        calls.append(str(kwargs["month"]))
        original(**kwargs)

    monkeypatch.setattr(revisions, "validate_preview_revision_invariant", capture)
    reader = revisions.PreviewRevisionReadService(artifact_store=store)
    if operation == "manifest":
        assert reader.read_manifest(revision) == body
    elif operation == "file":
        reader.read_file(revision, "cost-and-usage.csv")
    else:
        archive = reader.open_archive(revision)
        archive.close()

    assert calls == ["2026-07"]


def test_reader_validates_then_returns_exact_manifest_file_and_archive(tmp_path: Path) -> None:
    revisions = import_module("core.preview.revisions")
    revision, body, store = _stored_revision(tmp_path)
    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    assert reader.read_manifest(revision) == body
    metadata, file_body = reader.read_file(revision, "cost-and-usage.csv")
    assert metadata == revision.package.files[0]
    assert file_body == store.read_file(revision.package.storage_key, metadata)
    archive = reader.open_archive(revision)
    try:
        assert b"".join(archive.iter_chunks())
    finally:
        archive.close()


def test_reader_is_a_borrower_and_never_closes_artifact_store(tmp_path: Path) -> None:
    revisions = import_module("core.preview.revisions")
    _revision, _body, store = _stored_revision(tmp_path)

    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    assert not hasattr(reader, "close")
    assert store.cleanup_staging() == 0
