from __future__ import annotations

import json
import logging
import re
import tempfile
from collections.abc import Iterator, Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from io import TextIOWrapper
from typing import Any, Literal, Protocol, cast

from core.preview.capability import (
    FOCUS_PREVIEW_CAPABILITY,
    MAPPING_PROFILE_VERSION,
    preview_manifest_known_gaps,
)
from core.preview.mapping import (
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    PREVIEW_MANIFEST_SCHEMA_VERSION,
    PROFILE_NOT_APPLICABLE_COLUMNS,
    preview_decimal_text,
    preview_revision_content_sha256,
    preview_revision_source_snapshot,
    preview_subtract_decimals,
    preview_utc_text,
)
from core.preview.models import (
    PreviewColumnProfile,
    PreviewRequest,
    PreviewRevision,
    PreviewRevisionValidationSummary,
    preview_month,
)

logger = logging.getLogger(__name__)
_CANONICAL_DECIMAL_PATTERN = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?")


class RewindableArtifactStream(Protocol):
    def iter_chunks(self, *, chunk_size: int = 64 * 1024) -> Iterator[bytes]: ...

    def rewind(self) -> None: ...


class PreviewManifestValidationError(ValueError):
    """A stored manifest no longer matches its persisted package metadata."""


class _JsonReader:
    def __init__(self, handle: TextIOWrapper) -> None:
        self._handle = handle
        self._pending: str | None = None

    def _take(self) -> str:
        if self._pending is not None:
            value = self._pending
            self._pending = None
            return value
        return self._handle.read(1)

    def _peek(self) -> str:
        if self._pending is None:
            self._pending = self._handle.read(1)
        return self._pending

    def _skip_whitespace(self) -> None:
        while self._peek() in {" ", "\t", "\r", "\n"}:
            self._take()

    def expect(self, expected: str) -> None:
        self._skip_whitespace()
        actual = self._take()
        if actual != expected:
            raise PreviewManifestValidationError("stored preview manifest is malformed")

    def string(self) -> str:
        self._skip_whitespace()
        if self._take() != '"':
            raise PreviewManifestValidationError("stored preview manifest is malformed")
        encoded = ['"']
        escaped = False
        while True:
            character = self._take()
            if not character:
                raise PreviewManifestValidationError("stored preview manifest is malformed")
            encoded.append(character)
            if escaped:
                escaped = False
                continue
            if character == "\\":
                escaped = True
                continue
            if character == '"':
                break
        try:
            value = json.loads("".join(encoded))
        except json.JSONDecodeError as exc:
            raise PreviewManifestValidationError("stored preview manifest is malformed") from exc
        if not isinstance(value, str):
            raise PreviewManifestValidationError("stored preview manifest is malformed")
        return value

    def value(self) -> Any:
        self._skip_whitespace()
        leading = self._peek()
        if leading == '"':
            return self.string()
        if leading == "{":
            return self.parse_object()
        if leading == "[":
            return self.parse_array()
        token: list[str] = []
        while self._peek() and self._peek() not in {",", "]", "}", " ", "\t", "\r", "\n"}:
            token.append(self._take())
        encoded = "".join(token)
        if (
            not encoded
            or re.fullmatch(r"(?:true|false|null|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?)", encoded) is None
        ):
            raise PreviewManifestValidationError("stored preview manifest is malformed")
        try:
            return json.loads(encoded)
        except json.JSONDecodeError as exc:
            raise PreviewManifestValidationError("stored preview manifest is malformed") from exc

    def parse_object(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        self.expect("{")
        self._skip_whitespace()
        if self._peek() == "}":
            self._take()
            return result
        while True:
            key = self.string()
            if key in result:
                raise PreviewManifestValidationError("stored preview manifest contains duplicate keys")
            self.expect(":")
            result[key] = self.value()
            self._skip_whitespace()
            delimiter = self._take()
            if delimiter == "}":
                return result
            if delimiter != ",":
                raise PreviewManifestValidationError("stored preview manifest is malformed")

    def parse_array(self) -> list[Any]:
        result: list[Any] = []
        self.expect("[")
        self._skip_whitespace()
        if self._peek() == "]":
            self._take()
            return result
        while True:
            result.append(self.value())
            self._skip_whitespace()
            delimiter = self._take()
            if delimiter == "]":
                return result
            if delimiter != ",":
                raise PreviewManifestValidationError("stored preview manifest is malformed")

    def manifest(self, expected_files: Iterator[dict[str, object]]) -> dict[str, Any]:
        manifest: dict[str, Any] = {}
        files_seen = False
        self.expect("{")
        self._skip_whitespace()
        if self._peek() == "}":
            self._take()
            return manifest
        while True:
            key = self.string()
            if key in manifest or (key == "files" and files_seen):
                raise PreviewManifestValidationError("stored preview manifest contains duplicate keys")
            self.expect(":")
            if key == "files":
                files_seen = True
                self._validate_files(expected_files)
            else:
                manifest[key] = self.value()
            self._skip_whitespace()
            delimiter = self._take()
            if delimiter == "}":
                break
            if delimiter != ",":
                raise PreviewManifestValidationError("stored preview manifest is malformed")
        self._skip_whitespace()
        if self._take():
            raise PreviewManifestValidationError("stored preview manifest has trailing content")
        if not files_seen:
            raise PreviewManifestValidationError("stored preview manifest file declarations are inconsistent")
        return manifest

    def _validate_files(self, expected_files: Iterator[dict[str, object]]) -> None:
        self.expect("[")
        self._skip_whitespace()
        if self._peek() == "]":
            self._take()
            try:
                next(expected_files)
            except StopIteration:
                return
            raise PreviewManifestValidationError("stored preview manifest file declarations are inconsistent")
        while True:
            declaration = self.value()
            try:
                expected = next(expected_files)
            except StopIteration:
                raise PreviewManifestValidationError(
                    "stored preview manifest file declarations are inconsistent"
                ) from None
            if declaration != expected:
                raise PreviewManifestValidationError("stored preview manifest file declarations are inconsistent")
            self._skip_whitespace()
            delimiter = self._take()
            if delimiter == "]":
                break
            if delimiter != ",":
                raise PreviewManifestValidationError("stored preview manifest is malformed")
        try:
            next(expected_files)
        except StopIteration:
            return
        raise PreviewManifestValidationError("stored preview manifest file declarations are inconsistent")


def _artifact_declaration(item: Any) -> dict[str, object]:
    return {
        "name": item.name,
        "media_type": item.media_type,
        "size_bytes": item.size_bytes,
        "sha256": item.sha256,
        "order": item.order,
    }


def _require_equal(manifest: Mapping[str, Any], field: str, expected: object) -> None:
    if manifest.get(field) != expected:
        raise PreviewManifestValidationError(f"stored preview manifest {field} is inconsistent")


def _canonical_decimal(value: object) -> Decimal:
    if not isinstance(value, str) or _CANONICAL_DECIMAL_PATTERN.fullmatch(value) is None:
        raise PreviewManifestValidationError("stored preview manifest reconciliation is inconsistent")
    try:
        decimal = Decimal(value)
    except InvalidOperation as exc:
        raise PreviewManifestValidationError("stored preview manifest reconciliation is inconsistent") from exc
    if not decimal.is_finite() or preview_decimal_text(decimal) != value:
        raise PreviewManifestValidationError("stored preview manifest reconciliation is inconsistent")
    return decimal


def _validate_reconciliation(manifest: Mapping[str, Any]) -> None:
    reconciliation = manifest.get("reconciliation")
    fields = {
        "source_cost",
        "allocated_cost",
        "difference",
        "source_quantity",
        "allocated_quantity",
        "quantity_difference",
    }
    if not isinstance(reconciliation, dict) or set(reconciliation) != fields:
        raise PreviewManifestValidationError("stored preview manifest reconciliation is inconsistent")
    source_cost = _canonical_decimal(reconciliation["source_cost"])
    allocated_cost = _canonical_decimal(reconciliation["allocated_cost"])
    difference = _canonical_decimal(reconciliation["difference"])
    source_quantity = _canonical_decimal(reconciliation["source_quantity"])
    allocated_quantity = _canonical_decimal(reconciliation["allocated_quantity"])
    quantity_difference = _canonical_decimal(reconciliation["quantity_difference"])
    if (
        reconciliation["difference"] != preview_decimal_text(preview_subtract_decimals(source_cost, allocated_cost))
        or reconciliation["quantity_difference"]
        != preview_decimal_text(preview_subtract_decimals(source_quantity, allocated_quantity))
        or difference != 0
        or quantity_difference != 0
    ):
        raise PreviewManifestValidationError("stored preview manifest reconciliation is inconsistent")


def _parse_manifest(
    stream: RewindableArtifactStream,
    expected_files: Iterator[dict[str, object]],
) -> dict[str, Any]:
    with tempfile.TemporaryFile(mode="w+b") as spool:
        for chunk in stream.iter_chunks(chunk_size=64 * 1024):
            spool.write(chunk)
        spool.seek(0)
        text = TextIOWrapper(spool, encoding="utf-8", newline="")
        try:
            return _JsonReader(text).manifest(expected_files)
        except UnicodeDecodeError as exc:
            raise PreviewManifestValidationError("stored preview manifest is invalid") from exc
        finally:
            text.detach()
            stream.rewind()


def validate_requested_manifest(
    stream: RewindableArtifactStream,
    request: PreviewRequest,
) -> None:
    """Validate a requested-package manifest before response bytes are exposed.

    The artifact is copied through a fixed read buffer to a disk-backed temporary
    handle. This keeps artifact I/O bounded and leaves the verified source handle
    positioned at byte zero for delivery.
    """

    expected_files = (
        _artifact_declaration(item) for item in (() if request.package is None else request.package.files)
    )
    manifest = _parse_manifest(stream, expected_files)
    if not isinstance(manifest, dict):
        raise PreviewManifestValidationError("stored preview manifest is invalid")

    _require_equal(manifest, "schema_version", PREVIEW_MANIFEST_SCHEMA_VERSION)
    _require_equal(manifest, "package_type", "requested_preview_package")
    _require_equal(manifest, "request_id", request.request_id)
    _require_equal(manifest, "tenant_name", request.tenant_name)
    _require_equal(manifest, "grain", request.grain)
    _require_equal(manifest, "start_date", request.start_date.isoformat())
    _require_equal(manifest, "end_date", request.end_date.isoformat())
    _require_equal(
        manifest,
        "month",
        preview_month(grain=request.grain, start_date=request.start_date, end_date=request.end_date),
    )
    _require_equal(manifest, "column_profile", request.column_profile)
    _require_equal(manifest, "effective_columns", list(request.effective_columns))
    _require_equal(manifest, "target_focus_version", FOCUS_PREVIEW_CAPABILITY.target_focus_version)
    _require_equal(manifest, "conformance_status", FOCUS_PREVIEW_CAPABILITY.conformance_status)
    _require_equal(manifest, "mapping_profile_version", MAPPING_PROFILE_VERSION)
    _require_equal(manifest, "known_gaps", preview_manifest_known_gaps())
    _require_equal(
        manifest,
        "profile_not_applicable_columns",
        list(PROFILE_NOT_APPLICABLE_COLUMNS),
    )

    snapshot = request.source_snapshot
    if snapshot is None:
        raise PreviewManifestValidationError("stored preview manifest source snapshot is inconsistent")
    source_snapshot = manifest.get("source_snapshot")
    expected_snapshot = {
        "calculation_timestamp": (
            None if snapshot.calculation_timestamp is None else preview_utc_text(snapshot.calculation_timestamp)
        ),
        "calculation_coverage": [
            {
                "tracking_date": entry.tracking_date.isoformat(),
                "calculation_id": entry.calculation_id,
                "calculation_completed_at": preview_utc_text(entry.calculation_completed_at),
                "calculation_run_id": entry.calculation_run_id,
            }
            for entry in snapshot.calculation_coverage
        ],
        "source_through": (None if snapshot.source_through is None else preview_utc_text(snapshot.source_through)),
    }
    if source_snapshot != expected_snapshot:
        raise PreviewManifestValidationError("stored preview manifest source snapshot is inconsistent")

    evidence_start = snapshot.effective_coverage_start_date
    evidence_end = snapshot.effective_coverage_end_date
    if evidence_start is None or evidence_end is None:
        raise PreviewManifestValidationError("stored preview manifest evidence coverage is inconsistent")
    evidence_through = None if evidence_start == evidence_end else evidence_end - date.resolution
    expected_evidence_coverage = {
        "start_date": evidence_start.isoformat(),
        "end_date": evidence_end.isoformat(),
        "end_exclusive": True,
        "evidence_through_date": None if evidence_through is None else evidence_through.isoformat(),
        "availability_cutoff_end_date": (
            None if snapshot.availability_cutoff_end_date is None else snapshot.availability_cutoff_end_date.isoformat()
        ),
    }
    if manifest.get("evidence_coverage") != expected_evidence_coverage:
        raise PreviewManifestValidationError("stored preview manifest evidence coverage is inconsistent")

    _require_equal(manifest, "monthly_status", snapshot.monthly_status)
    validation = manifest.get("validation")
    if not isinstance(validation, dict):
        raise PreviewManifestValidationError("stored preview manifest validation is inconsistent")
    if (
        validation.get("status") != "passed"
        or validation.get("mapping_profile_version") != MAPPING_PROFILE_VERSION
        or validation.get("mapping_errors") != 0
        or validation.get("artifact_integrity") != "passed"
        or not isinstance(validation.get("source_records"), int)
        or isinstance(validation.get("source_records"), bool)
        or validation["source_records"] < 0
        or not isinstance(validation.get("rows"), int)
        or isinstance(validation.get("rows"), bool)
        or validation["rows"] < 0
    ):
        raise PreviewManifestValidationError("stored preview manifest validation is inconsistent")

    _validate_reconciliation(manifest)

    if request.completed_at is None or request.expires_at is None:
        raise PreviewManifestValidationError("stored preview manifest lifecycle is inconsistent")
    expected_lifecycle = {
        "ready_at": preview_utc_text(request.completed_at),
        "expires_at": preview_utc_text(request.expires_at),
        "retention_days": 7,
    }
    if manifest.get("lifecycle") != expected_lifecycle:
        raise PreviewManifestValidationError("stored preview manifest lifecycle is inconsistent")
    _require_equal(manifest, "generated_at", preview_utc_text(request.completed_at))

    if request.package is None:
        raise PreviewManifestValidationError("stored preview manifest package is inconsistent")


def validate_revision_manifest(
    stream: RewindableArtifactStream,
    revision: PreviewRevision,
) -> PreviewRevisionValidationSummary:
    """Incrementally validate a revision manifest and rewind it for delivery."""

    manifest = _parse_manifest(
        stream,
        (_artifact_declaration(item) for item in revision.package.files),
    )

    _require_equal(manifest, "schema_version", PREVIEW_MANIFEST_SCHEMA_VERSION)
    _require_equal(manifest, "package_type", "published_preview_revision")
    _require_equal(manifest, "revision_id", revision.revision_id)
    _require_equal(manifest, "tenant_name", revision.tenant_name_at_publication)
    _require_equal(manifest, "grain", "monthly")
    _require_equal(manifest, "month", revision.month)
    _require_equal(manifest, "start_date", revision.start_date.isoformat())
    _require_equal(manifest, "end_date", revision.end_date.isoformat())
    _require_equal(manifest, "monthly_status", revision.monthly_status)
    _require_equal(manifest, "supersedes_revision_id", revision.supersedes_revision_id)
    _require_equal(manifest, "published_at", preview_utc_text(revision.published_at))
    _require_equal(
        manifest,
        "source_snapshot",
        preview_revision_source_snapshot(revision.source_snapshot),
    )
    _require_equal(manifest, "conformance_status", FOCUS_PREVIEW_CAPABILITY.conformance_status)
    _require_equal(manifest, "known_gaps", preview_manifest_known_gaps())
    _require_equal(manifest, "mapping_profile_version", MAPPING_PROFILE_VERSION)
    _require_equal(manifest, "target_focus_version", FOCUS_PREVIEW_CAPABILITY.target_focus_version)
    _require_equal(manifest, "column_profile", "full")
    _require_equal(manifest, "effective_columns", list(FOCUS_1_4_FULL_PROFILE_COLUMNS))

    mapping_profile_version = manifest.get("mapping_profile_version")
    target_focus_version = manifest.get("target_focus_version")
    column_profile = manifest.get("column_profile")
    effective_columns = manifest.get("effective_columns")
    logical_data_sha256 = manifest.get("logical_data_sha256")
    material_sha256 = manifest.get("material_sha256")
    if (
        not isinstance(mapping_profile_version, str)
        or not isinstance(target_focus_version, str)
        or not isinstance(column_profile, str)
        or column_profile != "full"
        or effective_columns != list(FOCUS_1_4_FULL_PROFILE_COLUMNS)
        or not isinstance(logical_data_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", logical_data_sha256) is None
        or not isinstance(material_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", material_sha256) is None
    ):
        raise PreviewManifestValidationError("stored preview manifest material preimage is invalid")
    recomputed_material = preview_revision_content_sha256(
        mapping_profile_version=mapping_profile_version,
        target_focus_version=target_focus_version,
        column_profile=cast("PreviewColumnProfile", column_profile),
        effective_columns=tuple(effective_columns),
        logical_data_sha256=logical_data_sha256,
    )
    if recomputed_material != material_sha256 or recomputed_material != revision.material_sha256:
        raise PreviewManifestValidationError("stored preview manifest material digest is inconsistent")

    validation = manifest.get("validation")
    if not isinstance(validation, dict):
        raise PreviewManifestValidationError("stored preview manifest validation is inconsistent")
    source_records = validation.get("source_records")
    rows = validation.get("rows")
    mapping_errors = validation.get("mapping_errors")
    if (
        validation.get("status") != "passed"
        or validation.get("mapping_profile_version") != mapping_profile_version
        or not isinstance(source_records, int)
        or isinstance(source_records, bool)
        or not isinstance(rows, int)
        or isinstance(rows, bool)
        or type(mapping_errors) is not int
        or mapping_errors != 0
        or validation.get("artifact_integrity") != "passed"
    ):
        raise PreviewManifestValidationError("stored preview manifest validation is inconsistent")
    return PreviewRevisionValidationSummary(
        status="passed",
        mapping_profile_version=mapping_profile_version,
        source_records=source_records,
        rows=rows,
        mapping_errors=cast("Literal[0]", mapping_errors),
        artifact_integrity="passed",
    )
