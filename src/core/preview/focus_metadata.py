from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from uuid import UUID, uuid5

from core.preview.capability import FOCUS_PREVIEW_CAPABILITY
from core.preview.mapping import (
    CUSTOM_EVIDENCE_RULES,
    FOCUS_1_4_COLUMN_RULES,
    FOCUS_1_4_FULL_PROFILE_COLUMNS,
    CustomEvidenceRule,
    FocusColumnRule,
    PreviewDataPackageDraft,
    PreviewMappingError,
    PreviewValidatorKind,
    preview_canonical_json,
    preview_revision_content_sha256,
    preview_utc_text,
)
from core.preview.models import (
    PreviewArtifactMetadata,
    PreviewArtifactPayload,
    PreviewMonthlyStatus,
    PreviewRequest,
    PreviewSourceSnapshot,
    preview_month,
    validate_preview_revision_invariant,
)
from core.preview.spooling import PreviewSpooledBody, spooled_body_metadata

logger = logging.getLogger(__name__)

FOCUS_METADATA_FILE_NAME = "focus-metadata.json"
FOCUS_METADATA_MEDIA_TYPE = "application/json"
FOCUS_METADATA_CONTRACT_VERSION = "chitragupta.focus-metadata.v1"
FOCUS_PREVIEW_SCHEMA_CONTRACT_VERSION = "chitragupta.focus-preview-schema.v1"
FOCUS_DATA_GENERATOR = "Chitragupta"
FOCUS_DATASET_ID = "CostAndUsage"
FOCUS_METADATA_NAMESPACE = UUID("5f1e6f56-9b72-4c47-91e6-c21f51d4826a")

_DATASET_INSTANCE_NAME = "Chitragupta FOCUS Mapping Preview Cost and Usage"
_MANIFEST_AUTHORITIES = (
    "checksums",
    "file_sizes",
    "expiry",
    "revisions",
    "known_gaps",
    "package_lifecycle",
)
_RULES_BY_COLUMN: dict[str, FocusColumnRule | CustomEvidenceRule] = {
    **{rule.column: rule for rule in FOCUS_1_4_COLUMN_RULES},
    **{rule.column: rule for rule in CUSTOM_EVIDENCE_RULES},
}
_STRING_VALIDATORS = {
    PreviewValidatorKind.ENUM,
    PreviewValidatorKind.IDENTIFIER,
    PreviewValidatorKind.TEXT,
}


def _require_aware(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise PreviewMappingError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _identity(preimage: Mapping[str, object]) -> str:
    return str(uuid5(FOCUS_METADATA_NAMESPACE, preview_canonical_json(preimage)))


def _time_sector_bounds(start_date: date, end_date: date) -> tuple[str, str]:
    return (
        preview_utc_text(datetime.combine(start_date, time.min, tzinfo=UTC)),
        preview_utc_text(datetime.combine(end_date, time.min, tzinfo=UTC)),
    )


def _dataset_instance_last_updated(
    *,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    lifecycle_at: datetime,
) -> datetime:
    if snapshot.source_through is not None:
        return snapshot.source_through
    if snapshot.calculation_timestamp is not None:
        return snapshot.calculation_timestamp
    coverage_latest = max(
        (entry.calculation_completed_at for entry in snapshot.calculation_coverage),
        default=None,
    )
    if coverage_latest is not None:
        return coverage_latest
    if draft.source_records == 0 and draft.rows == 0:
        return lifecycle_at
    raise PreviewMappingError("metadata dataset freshness is unavailable")


def _column_metadata(effective_columns: tuple[str, ...]) -> list[dict[str, object]]:
    columns: list[dict[str, object]] = []
    for column_name in effective_columns:
        rule = _RULES_BY_COLUMN.get(column_name)
        if rule is None:
            raise PreviewMappingError(f"metadata schema rule is unavailable for column {column_name!r}")
        data_type = {
            PreviewValidatorKind.DECIMAL: "DECIMAL",
            PreviewValidatorKind.DATETIME: "DATETIME",
            PreviewValidatorKind.JSON: "JSON",
        }.get(rule.validator, "STRING")
        column: dict[str, object] = {
            "column_name": column_name,
            "data_type": data_type,
            "allows_null": rule.allows_null,
            "applicability": rule.applicability.value,
            "source": rule.source,
            "transformation": rule.transformation,
        }
        if rule.validator is PreviewValidatorKind.DECIMAL:
            column["numeric_precision"] = 38
        if rule.validator in _STRING_VALIDATORS:
            column["string_encoding"] = "UTF-8"
        if rule.allowed_values is not None:
            column["allowed_values"] = list(rule.allowed_values)
        if rule.gap_code is not None:
            column["gap_code"] = rule.gap_code
        if column_name == "Tags":
            column["provider_tag_prefixes"] = []
        columns.append(column)
    return columns


def _body_bytes(payload: PreviewArtifactPayload) -> bytes:
    if isinstance(payload.body, bytes):
        return payload.body
    if isinstance(payload.body, PreviewSpooledBody):
        with payload.body.open() as handle:
            return handle.read()
    raise PreviewMappingError("metadata artifact body is unsupported")


def _build_document(
    *,
    package_type: str,
    tenant_scope: Mapping[str, str],
    grain: str,
    start_date: date,
    end_date: date,
    month: str | None,
    column_profile: str,
    effective_columns: tuple[str, ...],
    logical_data_sha256: str,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    data_files: tuple[PreviewArtifactPayload, ...],
    lifecycle_at: datetime,
    delivery: Mapping[str, str],
    time_sector_complete: bool,
) -> dict[str, object]:
    lifecycle_at = _require_aware(lifecycle_at, "metadata lifecycle timestamp")
    freshness = _dataset_instance_last_updated(snapshot=snapshot, draft=draft, lifecycle_at=lifecycle_at)
    start_at, end_at = _time_sector_bounds(start_date, end_date)
    mapping_profile_version = FOCUS_PREVIEW_CAPABILITY.mapping_profile_version
    target_focus_version = FOCUS_PREVIEW_CAPABILITY.target_focus_version
    dataset_instance_id = _identity(
        {
            "package_type": package_type,
            "tenant_scope": dict(tenant_scope),
            "grain": grain,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "month": month,
            "column_profile": column_profile,
            "effective_columns": list(effective_columns),
            "target_focus_version": target_focus_version,
            "mapping_profile_version": mapping_profile_version,
            "logical_data_sha256": logical_data_sha256,
        }
    )
    schema_id = _identity(
        {
            "target_focus_version": target_focus_version,
            "mapping_profile_version": mapping_profile_version,
            "column_profile": column_profile,
            "effective_columns": list(effective_columns),
            "schema_contract_version": FOCUS_PREVIEW_SCHEMA_CONTRACT_VERSION,
        }
    )
    recency_id = _identity(
        {
            "dataset_instance_id": dataset_instance_id,
            "time_sector_start": start_at,
            "time_sector_end": end_at,
            "time_sector_complete": time_sector_complete,
            "package_type": package_type,
            "grain": grain,
            "tenant_scope": dict(tenant_scope),
        }
    )
    freshness_text = preview_utc_text(freshness)
    lifecycle_text = preview_utc_text(lifecycle_at)
    return {
        "x_ChitraguptaPreviewMetadata": {
            "contract_version": FOCUS_METADATA_CONTRACT_VERSION,
            "metadata_conformance_status": "non_conforming_preview_metadata",
            "target_focus_version": target_focus_version,
            "conformance_status": FOCUS_PREVIEW_CAPABILITY.conformance_status,
            "mapping_profile_version": mapping_profile_version,
            "known_gaps_authority": {"manifest": "manifest.json", "field": "known_gaps"},
            "chitragupta_manifest": {
                "name": "manifest.json",
                "authority": list(_MANIFEST_AUTHORITIES),
            },
            "x_ChitraguptaPreviewDataGenerator": {
                "data_generator": FOCUS_DATA_GENERATOR,
                "data_generator_version": mapping_profile_version,
            },
            "x_ChitraguptaPreviewDatasetInstance": {
                "dataset_instance_id": dataset_instance_id,
                "dataset_instance_name": _DATASET_INSTANCE_NAME,
                "focus_dataset_id": FOCUS_DATASET_ID,
                "dataset_instance_last_updated": freshness_text,
                "package_type": package_type,
                "grain": grain,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "end_exclusive": True,
            },
            "x_ChitraguptaPreviewRecency": {
                "recency_id": recency_id,
                "dataset_instance_id": dataset_instance_id,
                "dataset_instance_last_updated": freshness_text,
                "recency_last_updated": lifecycle_text,
                "time_sectors": [
                    {
                        "time_sector_start": start_at,
                        "time_sector_end": end_at,
                        "time_sector_last_updated": freshness_text,
                        "time_sector_complete": time_sector_complete,
                    }
                ],
            },
            "x_ChitraguptaPreviewSchema": {
                "schema_id": schema_id,
                "dataset_instance_id": dataset_instance_id,
                "schema_contract_version": FOCUS_PREVIEW_SCHEMA_CONTRACT_VERSION,
                "target_focus_version": target_focus_version,
                "schema_semantics": "target_vocabulary_import_metadata_not_focus_schema_conformance",
                "columns": _column_metadata(effective_columns),
            },
            "dataset_artifacts": [
                {
                    "name": item.name,
                    "media_type": item.media_type,
                    "order": item.order,
                    "dataset_instance_id": dataset_instance_id,
                    "schema_id": schema_id,
                }
                for item in data_files
            ],
            "delivery": dict(delivery),
        }
    }


def _metadata_payload(document: Mapping[str, object], *, order: int) -> PreviewArtifactPayload:
    return PreviewArtifactPayload(
        name=FOCUS_METADATA_FILE_NAME,
        media_type=FOCUS_METADATA_MEDIA_TYPE,
        order=order,
        body=(preview_canonical_json(document) + "\n").encode(),
    )


def build_requested_focus_metadata_artifact(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    data_files: tuple[PreviewArtifactPayload, ...],
    ready_at: datetime,
    expires_at: datetime,
) -> PreviewArtifactPayload:
    ready_at = _require_aware(ready_at, "ready_at")
    expires_at = _require_aware(expires_at, "expires_at")
    if expires_at != ready_at + timedelta(days=7):
        raise PreviewMappingError("expires_at must be exactly seven days after ready_at")
    document = _build_document(
        package_type="requested_preview_package",
        tenant_scope={
            "ecosystem": request.ecosystem,
            "tenant_id": request.tenant_id,
        },
        grain=request.grain,
        start_date=request.start_date,
        end_date=request.end_date,
        month=preview_month(grain=request.grain, start_date=request.start_date, end_date=request.end_date),
        column_profile=request.column_profile,
        effective_columns=request.effective_columns,
        logical_data_sha256=draft.logical_data_sha256,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        lifecycle_at=ready_at,
        delivery={
            "delivery_handling": "one_off_download",
            "correction_handling": "not_a_correction_series",
            "snapshot": "complete_requested_snapshot",
            "consumer_action": "consume_as_immutable_requested_package",
        },
        time_sector_complete=snapshot.monthly_status == "settled",
    )
    return _metadata_payload(document, order=len(data_files) + 1)


def build_revision_focus_metadata_artifact(
    *,
    revision_id: str,
    tenant_name_at_publication: str,
    month: str,
    start_date: date,
    end_date: date,
    monthly_status: PreviewMonthlyStatus,
    material_sha256: str,
    supersedes_revision_id: str | None,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    data_files: tuple[PreviewArtifactPayload, ...],
    published_at: datetime,
) -> PreviewArtifactPayload:
    del revision_id, supersedes_revision_id
    validate_preview_revision_invariant(
        month=month,
        start_date=start_date,
        end_date=end_date,
        monthly_status=monthly_status,
        source_snapshot=snapshot,
    )
    recomputed_material = preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
    if material_sha256 != recomputed_material:
        raise PreviewMappingError("revision material digest does not match canonical preimage")
    document = _build_document(
        package_type="published_preview_revision",
        tenant_scope={"tenant_name_at_publication": tenant_name_at_publication},
        grain="monthly",
        start_date=start_date,
        end_date=end_date,
        month=month,
        column_profile="full",
        effective_columns=FOCUS_1_4_FULL_PROFILE_COLUMNS,
        logical_data_sha256=draft.logical_data_sha256,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        lifecycle_at=published_at,
        delivery={
            "delivery_handling": "Overwrite",
            "correction_handling": "Replacement",
            "snapshot": "complete",
            "consumer_action": "replace_do_not_aggregate",
        },
        time_sector_complete=True,
    )
    return _metadata_payload(document, order=len(data_files) + 1)


def compose_package_artifacts(
    *,
    data_files: Sequence[PreviewArtifactPayload],
    focus_metadata: PreviewArtifactPayload,
) -> tuple[PreviewArtifactPayload, ...]:
    rendered = tuple(data_files)
    if tuple(item.order for item in rendered) != tuple(range(1, len(rendered) + 1)):
        raise PreviewMappingError("data artifact order must be contiguous")
    if any(item.media_type != "text/csv" for item in rendered):
        raise PreviewMappingError("data artifacts must be CSV files")
    if focus_metadata.name != FOCUS_METADATA_FILE_NAME or focus_metadata.media_type != FOCUS_METADATA_MEDIA_TYPE:
        raise PreviewMappingError("focus metadata artifact identity is invalid")
    if focus_metadata.order != len(rendered) + 1:
        raise PreviewMappingError("focus metadata artifact order is invalid")
    package_files = (*rendered, focus_metadata)
    if len({item.name for item in package_files}) != len(package_files):
        raise PreviewMappingError("package artifact names must be unique")
    return package_files


def _validate_metadata_artifact(
    *,
    package_files: tuple[PreviewArtifactPayload, ...],
    staged_files: tuple[PreviewArtifactMetadata, ...],
    expected: PreviewArtifactPayload,
) -> None:
    matching = tuple(item for item in package_files if item.name == FOCUS_METADATA_FILE_NAME)
    if len(matching) != 1 or package_files[-1] is not matching[0]:
        raise PreviewMappingError("package must contain exactly one final focus metadata artifact")
    if tuple(item.order for item in package_files) != tuple(range(1, len(package_files) + 1)):
        raise PreviewMappingError("package artifact order must be contiguous")
    if len(staged_files) != len(package_files):
        raise PreviewMappingError("staged artifact metadata does not match package files")
    for payload, metadata in zip(package_files, staged_files, strict=True):
        size_bytes, sha256 = spooled_body_metadata(payload.body)
        if metadata != PreviewArtifactMetadata(
            name=payload.name,
            media_type=payload.media_type,
            size_bytes=size_bytes,
            sha256=sha256,
            order=payload.order,
        ):
            raise PreviewMappingError("staged artifact metadata does not match package files")
    actual_body = _body_bytes(matching[0])
    try:
        parsed = json.loads(actual_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PreviewMappingError("focus metadata artifact is malformed") from exc
    if not isinstance(parsed, dict) or (preview_canonical_json(parsed) + "\n").encode() != actual_body:
        raise PreviewMappingError("focus metadata artifact is not canonical JSON")
    if actual_body != _body_bytes(expected):
        raise PreviewMappingError("focus metadata artifact is stale or internally inconsistent")


def validate_requested_focus_metadata_artifact(
    *,
    request: PreviewRequest,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    package_files: tuple[PreviewArtifactPayload, ...],
    staged_files: tuple[PreviewArtifactMetadata, ...],
    ready_at: datetime,
    expires_at: datetime,
) -> None:
    data_files = tuple(item for item in package_files if item.name != FOCUS_METADATA_FILE_NAME)
    expected = build_requested_focus_metadata_artifact(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=ready_at,
        expires_at=expires_at,
    )
    _validate_metadata_artifact(package_files=package_files, staged_files=staged_files, expected=expected)


def validate_revision_focus_metadata_artifact(
    *,
    revision_id: str,
    tenant_name_at_publication: str,
    month: str,
    start_date: date,
    end_date: date,
    monthly_status: PreviewMonthlyStatus,
    material_sha256: str,
    supersedes_revision_id: str | None,
    snapshot: PreviewSourceSnapshot,
    draft: PreviewDataPackageDraft,
    package_files: tuple[PreviewArtifactPayload, ...],
    staged_files: tuple[PreviewArtifactMetadata, ...],
    published_at: datetime,
) -> None:
    data_files = tuple(item for item in package_files if item.name != FOCUS_METADATA_FILE_NAME)
    expected = build_revision_focus_metadata_artifact(
        revision_id=revision_id,
        tenant_name_at_publication=tenant_name_at_publication,
        month=month,
        start_date=start_date,
        end_date=end_date,
        monthly_status=monthly_status,
        material_sha256=material_sha256,
        supersedes_revision_id=supersedes_revision_id,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        published_at=published_at,
    )
    _validate_metadata_artifact(package_files=package_files, staged_files=staged_files, expected=expected)
