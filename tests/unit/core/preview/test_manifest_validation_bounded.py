from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_revision_mapping import EXPECTED_PUBLIC_KNOWN_GAPS
from tests.unit.core.preview.test_revision_reader import _stored_revision, _TestArtifactStream
from tests.unit.core.preview.test_service import _ready_request


def _tampered_ready_request(
    ready: Any,
    manifest_path: Path,
    mutate: Callable[[dict[str, Any]], None],
) -> Any:
    manifest = json.loads(manifest_path.read_bytes())
    manifest["known_gaps"] = deepcopy(EXPECTED_PUBLIC_KNOWN_GAPS)
    mutate(manifest)
    body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    manifest_path.write_bytes(body)
    metadata = replace(
        ready.package.manifest,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
    )
    return replace(ready, package=replace(ready.package, manifest=metadata))


def _tampered_revision(
    tmp_path: Path,
    mutate: Callable[[dict[str, Any]], None],
) -> tuple[Any, Any]:
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    manifest["known_gaps"] = deepcopy(EXPECTED_PUBLIC_KNOWN_GAPS)
    mutate(manifest)
    tampered_body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    manifest_path = tmp_path / revision.package.storage_key / "manifest.json"
    manifest_path.write_bytes(tampered_body)
    metadata = replace(
        revision.package.manifest,
        size_bytes=len(tampered_body),
        sha256=hashlib.sha256(tampered_body).hexdigest(),
    )
    return (
        replace(revision, package=replace(revision.package, manifest=metadata)),
        store,
    )


def _remove_known_gaps(manifest: dict[str, Any]) -> None:
    manifest.pop("known_gaps")


def _reverse_known_gaps(manifest: dict[str, Any]) -> None:
    manifest["known_gaps"].reverse()


def _alter_known_gap_columns(manifest: dict[str, Any]) -> None:
    manifest["known_gaps"][-1]["columns"].reverse()


def _add_known_gap_owner(manifest: dict[str, Any]) -> None:
    manifest["known_gaps"][0]["owner_task"] = "TASK-254.03"


_KNOWN_GAP_TAMPERS = (
    ("omitted", _remove_known_gaps),
    ("reordered", _reverse_known_gaps),
    ("altered-columns", _alter_known_gap_columns),
    ("owner-bearing", _add_known_gap_owner),
)


def _set_manifest_path(
    manifest: dict[str, Any],
    path: tuple[str | int, ...],
    value: object,
) -> None:
    target: Any = manifest
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value


_LEGACY_SELF_CLAIM_BYPASS_TAMPERS = (
    ("schema", ("schema_version",), "chitragupta.preview-manifest.v2"),
    ("package-identity", ("package_type",), "other"),
    ("request-identity", ("request_id",), "other"),
    ("tenant", ("tenant_name",), "other"),
    ("interval", ("start_date",), "2026-06-30"),
    ("effective-columns", ("effective_columns",), ["BilledCost"]),
    ("target", ("target_focus_version",), "1.3"),
    ("status", ("conformance_status",), "conforming"),
    ("gaps", ("known_gaps",), []),
    ("snapshot", ("source_snapshot",), {}),
    ("evidence-coverage", ("evidence_coverage",), {}),
    ("lifecycle", ("lifecycle",), {}),
    ("validation", ("validation",), {}),
    ("reconciliation", ("reconciliation",), {}),
    ("file-order", ("files", 0, "order"), 9),
    ("file-checksum", ("files", 0, "sha256"), "0" * 64),
)


@pytest.mark.parametrize(
    "value",
    (
        "1e100000",
        "01",
        "1.0",
        "+1",
    ),
)
def test_reconciliation_rejects_noncanonical_decimal_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    validation = preview_module("manifest_validation")
    monkeypatch.setattr(
        validation,
        "Decimal",
        lambda _value: pytest.fail("invalid reconciliation decimal reached Decimal construction"),
    )

    with pytest.raises(validation.PreviewManifestValidationError):
        validation._canonical_decimal(value)


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
        (
            "profile-not-applicable",
            lambda manifest: manifest.__setitem__("profile_not_applicable_columns", []),
        ),
        (
            "evidence-coverage",
            lambda manifest: manifest.__setitem__("evidence_coverage", {}),
        ),
        (
            "validation-negative-count",
            lambda manifest: manifest["validation"].__setitem__("source_records", -1),
        ),
        (
            "reconciliation",
            lambda manifest: manifest["reconciliation"].__setitem__("difference", "1"),
        ),
        ("lifecycle", lambda manifest: manifest.__setitem__("lifecycle", {})),
        (
            "generated-at",
            lambda manifest: manifest.__setitem__("generated_at", "2026-07-05T00:00:00Z"),
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


@pytest.mark.parametrize(("case", "mutate"), _KNOWN_GAP_TAMPERS)
def test_requested_manifest_rejects_noncanonical_public_gap_contract(
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


def test_legacy_v4_self_claim_cannot_bypass_any_current_requested_manifest_check(
    tmp_path: Path,
) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    assert ready.storage_key is not None
    manifest_path = tmp_path / "artifacts" / ready.storage_key / "manifest.json"
    original = json.loads(manifest_path.read_bytes())
    validation = preview_module("manifest_validation")
    try:
        for case, path, value in _LEGACY_SELF_CLAIM_BYPASS_TAMPERS:
            manifest = deepcopy(original)
            manifest["mapping_profile_version"] = "focus-1.4-preview-v4"
            _set_manifest_path(manifest, path, value)
            body = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
            try:
                validation.validate_requested_manifest(_TestArtifactStream(body), ready)
            except validation.PreviewManifestValidationError:
                continue
            pytest.fail(f"legacy v4 self-claim bypassed current requested-manifest check: {case}")
    finally:
        runtime.close()
        backend.dispose()


def test_incomplete_internal_schema_v1_collision_fails_complete_current_validation(
    tmp_path: Path,
) -> None:
    runtime, ready, backend, _executor = _ready_request(tmp_path)
    assert ready.storage_key is not None
    manifest_path = tmp_path / "artifacts" / ready.storage_key / "manifest.json"
    incomplete = json.loads(manifest_path.read_bytes())
    incomplete["schema_version"] = "chitragupta.preview-manifest.v1"
    incomplete.pop("effective_columns")
    body = (json.dumps(incomplete, sort_keys=True, separators=(",", ":")) + "\n").encode()
    validation = preview_module("manifest_validation")
    try:
        with pytest.raises(validation.PreviewManifestValidationError):
            validation.validate_requested_manifest(_TestArtifactStream(body), ready)
    finally:
        runtime.close()
        backend.dispose()


def test_requested_and_revision_builders_share_exact_first_release_authorities(
    tmp_path: Path,
) -> None:
    requested_root = tmp_path / "requested"
    requested_root.mkdir()
    runtime, ready, backend, _executor = _ready_request(requested_root)
    revision, revision_body, store = _stored_revision(tmp_path / "revision")
    del revision
    assert ready.storage_key is not None
    requested_path = requested_root / "artifacts" / ready.storage_key / "manifest.json"
    requested = json.loads(requested_path.read_bytes())
    revision_manifest = json.loads(revision_body)
    mapping = preview_module("mapping")
    try:
        assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-preview-v1"
        assert mapping.PREVIEW_MANIFEST_SCHEMA_VERSION == "chitragupta.preview-manifest.v1"
        expected_capability = {
            "mapping_profile_version": "focus-1.4-preview-v1",
            "target_focus_version": "1.4",
            "conformance_status": "non_conforming",
            "known_gaps": EXPECTED_PUBLIC_KNOWN_GAPS,
        }
        assert {field: requested[field] for field in expected_capability} == expected_capability
        assert {field: revision_manifest[field] for field in expected_capability} == expected_capability
        assert requested["schema_version"] == mapping.PREVIEW_MANIFEST_SCHEMA_VERSION
        assert revision_manifest["schema_version"] == mapping.PREVIEW_MANIFEST_SCHEMA_VERSION
    finally:
        runtime.close()
        backend.dispose()
        store.close()


def test_revision_validation_rejects_obsolete_schema_even_when_manifest_is_otherwise_current(
    tmp_path: Path,
) -> None:
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    manifest["schema_version"] = "chitragupta.preview-manifest.v2"
    obsolete = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    validation = preview_module("manifest_validation")
    try:
        with pytest.raises(validation.PreviewManifestValidationError):
            validation.validate_revision_manifest(_TestArtifactStream(obsolete), revision)
    finally:
        store.close()


@pytest.mark.parametrize(
    ("field", "obsolete_value"),
    [
        ("mapping_profile_version", "focus-1.4-preview-v5"),
        ("target_focus_version", "1.3"),
        ("conformance_status", "conforming"),
        ("column_profile", "summary"),
        ("effective_columns", ["BilledCost"]),
    ],
)
def test_revision_validation_rejects_self_consistent_obsolete_material_authority(
    tmp_path: Path,
    field: str,
    obsolete_value: object,
) -> None:
    revision, body, store = _stored_revision(tmp_path)
    manifest = json.loads(body)
    manifest[field] = obsolete_value
    if field == "mapping_profile_version":
        manifest["validation"]["mapping_profile_version"] = obsolete_value
    mapping = preview_module("mapping")
    obsolete_material = (
        mapping.preview_revision_content_sha256(
            mapping_profile_version=manifest["mapping_profile_version"],
            target_focus_version=manifest["target_focus_version"],
            column_profile=manifest["column_profile"],
            effective_columns=tuple(manifest["effective_columns"]),
            logical_data_sha256=manifest["logical_data_sha256"],
        )
        if field != "conformance_status"
        else manifest["material_sha256"]
    )
    manifest["material_sha256"] = obsolete_material
    obsolete_revision = replace(revision, material_sha256=obsolete_material)
    obsolete = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    validation = preview_module("manifest_validation")
    try:
        with pytest.raises(validation.PreviewManifestValidationError):
            validation.validate_revision_manifest(_TestArtifactStream(obsolete), obsolete_revision)
    finally:
        store.close()


@pytest.mark.parametrize(("case", "mutate"), _KNOWN_GAP_TAMPERS)
def test_revision_manifest_rejects_noncanonical_public_gap_contract(
    tmp_path: Path,
    case: str,
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    del case
    revision, store = _tampered_revision(tmp_path, mutate)
    revisions = preview_module("revisions")
    reader = revisions.PreviewRevisionReadService(artifact_store=store)

    with pytest.raises(revisions.PreviewRevisionArtifactUnavailableError):
        reader.read_manifest(revision=revision)


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
