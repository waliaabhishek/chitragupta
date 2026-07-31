from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request, _snapshot
from tests.unit.core.preview.test_monthly_v5 import _row

EXPECTED_PUBLIC_KNOWN_GAPS: list[dict[str, object]] = [
    {
        "code": "invoice_identity_unavailable",
        "description": "Post-issuance invoice identity is unavailable.",
        "columns": ["InvoiceDetailId", "InvoiceId"],
    },
    {
        "code": "invoice_issuer_name_unavailable",
        "description": "Provider legal invoice-issuer evidence is unavailable.",
        "columns": ["InvoiceIssuerName"],
    },
    {
        "code": "provider_host_display_name_unavailable",
        "description": "HostProviderName contains the raw provider cloud code, not a provider display name.",
        "columns": ["HostProviderName"],
    },
    {
        "code": "provider_region_display_name_unavailable",
        "description": "Confluent inventory does not provide a distinct region display name.",
        "columns": ["RegionName"],
    },
    {
        "code": "derived_sku_identity_not_provider_authoritative",
        "description": ("SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers."),
        "columns": [
            "SkuId",
            "SkuMeter",
            "SkuPriceDetails",
            "SkuPriceId",
            "x_ChitraguptaSkuComponents",
        ],
    },
]


def assert_public_known_gaps(manifest: dict[str, Any]) -> None:
    assert manifest["known_gaps"] == EXPECTED_PUBLIC_KNOWN_GAPS
    manifest_text = json.dumps(manifest, sort_keys=True)
    assert "owner_task" not in manifest_text
    assert '"owner":' not in manifest_text
    assert "TASK-" not in manifest_text
    assert "reviewer" not in manifest_text.lower()
    assert "implementation chronology" not in manifest_text.lower()
    assert "delivery-process" not in manifest_text.lower()


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _models() -> Any:
    return import_module("core.preview.models")


def _settled_snapshot() -> Any:
    return _snapshot(
        start=date(2026, 7, 1),
        end=date(2026, 8, 1),
        monthly_status="settled",
        cutoff=date(2026, 8, 1),
        source_through=datetime(2026, 8, 1, tzinfo=UTC),
    )


def _monthly_request() -> Any:
    return _request(
        grain="monthly",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
        started_at=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
    )


def _draft(*, max_csv_file_bytes: int | None = None, rows: tuple[Any, ...] | None = None) -> Any:
    mapping = _mapping()
    selected = rows if rows is not None else (_row(day=1), _row(day=2, AllocatedResourceId="sa-2"))
    snapshot = _settled_snapshot()
    if not selected:
        snapshot = replace(snapshot, source_through=None)
    return mapping.build_preview_data_package(
        request=_monthly_request(),
        snapshot=snapshot,
        full_rows=selected,
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=len(selected),
            source_cost=Decimal("8") * len(selected),
            allocated_cost=Decimal("8") * len(selected),
            source_quantity=Decimal("5") * len(selected),
            allocated_quantity=Decimal("5") * len(selected),
        ),
        max_csv_file_bytes=max_csv_file_bytes,
    )


def _files(draft: Any) -> tuple[Any, ...]:
    models = _models()
    return tuple(
        models.PreviewArtifactMetadata(
            item.name,
            item.media_type,
            len(item.body),
            hashlib.sha256(item.body).hexdigest(),
            item.order,
        )
        for item in draft.data_files
    )


def _all_object_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value} | {
            nested_key for nested in value.values() for nested_key in _all_object_keys(nested)
        }
    if isinstance(value, list):
        return {nested_key for nested in value for nested_key in _all_object_keys(nested)}
    return set()


def test_logical_digest_is_independent_of_physical_partitioning() -> None:
    mapping = _mapping()
    unpartitioned = _draft()
    lines = unpartitioned.data_files[0].body.splitlines(keepends=True)
    header_size = len(lines[0])
    largest_record_size = max(len(line) for line in lines[1:])
    partitioned = _draft(max_csv_file_bytes=header_size + largest_record_size)

    assert len(unpartitioned.data_files) == 1
    assert len(partitioned.data_files) == 2
    assert partitioned.logical_data_sha256 == unpartitioned.logical_data_sha256
    assert mapping.preview_revision_content_sha256(
        logical_data_sha256=partitioned.logical_data_sha256
    ) == mapping.preview_revision_content_sha256(logical_data_sha256=unpartitioned.logical_data_sha256)


def test_header_only_month_has_stable_logical_and_material_identity() -> None:
    mapping = _mapping()
    first = _draft(rows=())
    second = _draft(rows=())

    assert first.logical_data_sha256 == hashlib.sha256(first.data_files[0].body).hexdigest()
    assert second.logical_data_sha256 == first.logical_data_sha256
    assert mapping.preview_revision_content_sha256(
        logical_data_sha256=first.logical_data_sha256
    ) == mapping.preview_revision_content_sha256(logical_data_sha256=second.logical_data_sha256)


def test_requested_and_revision_manifests_do_not_change_for_fallback_classification() -> None:
    mapping = _mapping()
    baseline = _draft(
        rows=(
            _row(
                x_ConfluentProduct="KAFKA",
                x_ConfluentLineType="KAFKA_STORAGE",
                ServiceCategory="Integration",
                ServiceName="Confluent Cloud Apache Kafka",
                ServiceSubcategory="Messaging",
            ),
        )
    )
    fallback = _draft(
        rows=(
            _row(
                x_ConfluentProduct="Provider Product / β",
                x_ConfluentLineType="Future Usage / β",
                ServiceCategory="Other",
                ServiceName="Provider Product / β",
                ServiceSubcategory="Other (Other)",
            ),
        )
    )
    request = _monthly_request()
    snapshot = _settled_snapshot()
    ready_at = datetime(2026, 8, 4, tzinfo=UTC)

    def requested(draft: Any) -> dict[str, Any]:
        return json.loads(
            mapping.build_requested_preview_manifest(
                request=request,
                snapshot=snapshot,
                draft=draft,
                files=_files(draft),
                ready_at=ready_at,
                expires_at=ready_at + timedelta(days=7),
            )
        )

    def revision(draft: Any) -> dict[str, Any]:
        material = mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
        return json.loads(
            mapping.build_preview_revision_manifest(
                revision_id="revision-1",
                tenant_name_at_publication="production",
                month="2026-07",
                start_date=date(2026, 7, 1),
                end_date=date(2026, 8, 1),
                monthly_status="settled",
                material_sha256=material,
                supersedes_revision_id=None,
                snapshot=snapshot,
                draft=draft,
                files=_files(draft),
                published_at=ready_at,
            )
        )

    baseline_requested = requested(baseline)
    fallback_requested = requested(fallback)
    baseline_revision = revision(baseline)
    fallback_revision = revision(fallback)

    assert set(fallback_requested) == set(baseline_requested)
    assert set(fallback_revision) == set(baseline_revision)
    assert set(fallback_requested["validation"]) == set(baseline_requested["validation"])
    assert set(fallback_revision["validation"]) == set(baseline_revision["validation"])
    assert fallback_requested["known_gaps"] == baseline_requested["known_gaps"]
    assert fallback_revision["known_gaps"] == baseline_revision["known_gaps"]
    assert_public_known_gaps(fallback_requested)
    assert_public_known_gaps(fallback_revision)
    assert all("fallback" not in key.casefold() for key in _all_object_keys(fallback_requested))
    assert all("fallback" not in key.casefold() for key in _all_object_keys(fallback_revision))


def test_material_digest_uses_exactly_the_five_declared_semantic_fields() -> None:
    mapping = _mapping()
    logical_digest = "1" * 64
    baseline = mapping.preview_revision_content_sha256(logical_data_sha256=logical_digest)

    changes = (
        {"mapping_profile_version": "mapping-v-next"},
        {"target_focus_version": "1.5"},
        {"column_profile": "custom"},
        {"effective_columns": ("BilledCost",)},
        {"logical_data_sha256": "2" * 64},
    )
    assert all(
        mapping.preview_revision_content_sha256(**{"logical_data_sha256": logical_digest, **change}) != baseline
        for change in changes
    )


def test_normalization_against_obsolete_baseline_changes_only_version_cells_and_derived_identity(
    tmp_path: Path,
) -> None:
    mapping = _mapping()
    models = _models()
    artifacts = import_module("core.preview.artifacts")
    assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-preview-v1"
    current = _draft(
        rows=(
            _row(
                day=1,
                x_ChitraguptaMappingProfileVersion=mapping.MAPPING_PROFILE_VERSION,
            ),
            _row(
                day=2,
                AllocatedResourceId="sa-2",
                x_ChitraguptaMappingProfileVersion=mapping.MAPPING_PROFILE_VERSION,
            ),
        )
    )
    current_csv = current.data_files[0].body
    assert b"focus-1.4-preview-v1" in current_csv
    baseline_csv = current_csv.replace(
        b"focus-1.4-preview-v1",
        b"focus-1.4-preview-v5",
    )
    assert baseline_csv != current_csv

    current_rows = list(csv.DictReader(io.StringIO(current_csv.decode(), newline="")))
    baseline_rows = list(csv.DictReader(io.StringIO(baseline_csv.decode(), newline="")))
    assert len(current_rows) == len(baseline_rows)
    for current_row, baseline_row in zip(current_rows, baseline_rows, strict=True):
        assert current_row.pop("x_ChitraguptaMappingProfileVersion") == "focus-1.4-preview-v1"
        assert baseline_row.pop("x_ChitraguptaMappingProfileVersion") == "focus-1.4-preview-v5"
        assert current_row == baseline_row

    baseline_logical_sha256 = hashlib.sha256(baseline_csv).hexdigest()
    baseline = mapping.PreviewDataPackageDraft(
        data_files=(
            models.PreviewArtifactPayload(
                current.data_files[0].name,
                current.data_files[0].media_type,
                current.data_files[0].order,
                baseline_csv,
            ),
        ),
        source_records=current.source_records,
        rows=current.rows,
        reconciliation=current.reconciliation,
        logical_data_sha256=baseline_logical_sha256,
    )
    current_files = _files(current)
    baseline_files = _files(baseline)
    assert current.logical_data_sha256 == hashlib.sha256(current_csv).hexdigest()
    assert current.logical_data_sha256 != baseline.logical_data_sha256
    assert current_files[0].sha256 != baseline_files[0].sha256

    request = _monthly_request()
    snapshot = _settled_snapshot()
    ready_at = datetime(2026, 8, 4, 0, 2, tzinfo=UTC)
    current_requested_body = mapping.build_requested_preview_manifest(
        request=request,
        snapshot=snapshot,
        draft=current,
        files=current_files,
        ready_at=ready_at,
        expires_at=ready_at + timedelta(days=7),
    )
    current_requested = json.loads(current_requested_body)
    baseline_requested = json.loads(current_requested_body)
    baseline_requested["schema_version"] = "chitragupta.preview-manifest.v2"
    baseline_requested["mapping_profile_version"] = "focus-1.4-preview-v5"
    baseline_requested["validation"]["mapping_profile_version"] = "focus-1.4-preview-v5"
    baseline_requested["files"][0]["sha256"] = baseline_files[0].sha256
    baseline_requested_body = (mapping.preview_canonical_json(baseline_requested) + "\n").encode()
    assert current_requested["schema_version"] == "chitragupta.preview-manifest.v1"
    assert current_requested["mapping_profile_version"] == "focus-1.4-preview-v1"
    assert current_requested["files"][0]["sha256"] == current_files[0].sha256
    assert current_requested_body != baseline_requested_body
    for field in (
        "known_gaps",
        "profile_not_applicable_columns",
        "source_snapshot",
        "evidence_coverage",
        "monthly_status",
        "reconciliation",
        "lifecycle",
        "generated_at",
        "conformance_status",
    ):
        assert current_requested[field] == baseline_requested[field]
    assert {
        key: value for key, value in current_requested["validation"].items() if key != "mapping_profile_version"
    } == {key: value for key, value in baseline_requested["validation"].items() if key != "mapping_profile_version"}

    current_material = mapping.preview_revision_content_sha256(logical_data_sha256=current.logical_data_sha256)
    baseline_material = mapping.preview_revision_content_sha256(
        mapping_profile_version="focus-1.4-preview-v5",
        logical_data_sha256=baseline.logical_data_sha256,
    )
    assert current_material != baseline_material
    current_revision_body = mapping.build_preview_revision_manifest(
        revision_id="revision-current",
        tenant_name_at_publication="production",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status="settled",
        material_sha256=current_material,
        supersedes_revision_id=None,
        snapshot=snapshot,
        draft=current,
        files=current_files,
        published_at=ready_at,
    )
    current_revision = json.loads(current_revision_body)
    baseline_revision = json.loads(current_revision_body)
    baseline_revision["schema_version"] = "chitragupta.preview-manifest.v2"
    baseline_revision["mapping_profile_version"] = "focus-1.4-preview-v5"
    baseline_revision["validation"]["mapping_profile_version"] = "focus-1.4-preview-v5"
    baseline_revision["logical_data_sha256"] = baseline.logical_data_sha256
    baseline_revision["material_sha256"] = baseline_material
    baseline_revision["files"][0]["sha256"] = baseline_files[0].sha256
    baseline_revision_body = (mapping.preview_canonical_json(baseline_revision) + "\n").encode()
    assert current_revision["material_sha256"] == current_material
    assert current_revision["files"][0]["sha256"] == current_files[0].sha256
    assert current_revision_body != baseline_revision_body
    for field in (
        "known_gaps",
        "source_snapshot",
        "monthly_status",
        "reconciliation",
        "conformance_status",
        "published_at",
    ):
        assert current_revision[field] == baseline_revision[field]
    assert {
        key: value for key, value in current_revision["validation"].items() if key != "mapping_profile_version"
    } == {key: value for key, value in baseline_revision["validation"].items() if key != "mapping_profile_version"}

    store = artifacts.LocalPreviewArtifactStore(tmp_path)
    owner = artifacts.PreviewArtifactOwner(
        "production",
        "confluent_cloud",
        "tenant-1",
        storage_backend_fingerprint="a" * 64,
    )
    with store.stage_data_files(
        owner=owner,
        request_id="current-package",
        data_files=current.data_files,
    ) as staged:
        current_package = staged.publish(manifest_body=current_requested_body)
    with store.stage_data_files(
        owner=owner,
        request_id="baseline-package",
        data_files=baseline.data_files,
    ) as staged:
        baseline_package = staged.publish(manifest_body=baseline_requested_body)
    with store.open_archive(
        storage_key=current_package.storage_key,
        manifest=current_package.manifest,
        files=current_package.files,
    ) as archive:
        current_archive = b"".join(archive.iter_chunks())
    with store.open_archive(
        storage_key=baseline_package.storage_key,
        manifest=baseline_package.manifest,
        files=baseline_package.files,
    ) as archive:
        baseline_archive = b"".join(archive.iter_chunks())
    assert current_archive != baseline_archive
    store.close()


@pytest.mark.parametrize("logical_digest", ["A" * 64, "short", "g" * 64])
def test_material_digest_rejects_noncanonical_logical_digest(logical_digest: str) -> None:
    mapping = _mapping()

    with pytest.raises(mapping.PreviewMappingError):
        mapping.preview_revision_content_sha256(logical_data_sha256=logical_digest)


def test_revision_manifest_recomputes_material_preimage_before_serialization() -> None:
    mapping = _mapping()
    draft = _draft(rows=())
    snapshot = replace(_settled_snapshot(), source_through=None)

    with pytest.raises(mapping.PreviewMappingError, match="material digest"):
        mapping.build_preview_revision_manifest(
            revision_id="revision-1",
            tenant_name_at_publication="production",
            month="2026-07",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
            monthly_status="settled",
            material_sha256="f" * 64,
            supersedes_revision_id=None,
            snapshot=snapshot,
            draft=draft,
            files=_files(draft),
            published_at=datetime(2026, 8, 4, tzinfo=UTC),
        )


def test_revision_manifest_serializes_the_exact_verified_material_preimage() -> None:
    mapping = _mapping()
    draft = _draft(rows=())
    snapshot = replace(_settled_snapshot(), source_through=None)
    material = mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)

    body = mapping.build_preview_revision_manifest(
        revision_id="revision-1",
        tenant_name_at_publication="production",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status="settled",
        material_sha256=material,
        supersedes_revision_id=None,
        snapshot=snapshot,
        draft=draft,
        files=_files(draft),
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    manifest = json.loads(body)

    assert {
        key: manifest[key]
        for key in (
            "mapping_profile_version",
            "target_focus_version",
            "column_profile",
            "effective_columns",
            "logical_data_sha256",
            "material_sha256",
        )
    } == {
        "mapping_profile_version": mapping.MAPPING_PROFILE_VERSION,
        "target_focus_version": "1.4",
        "column_profile": "full",
        "effective_columns": list(mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS),
        "logical_data_sha256": draft.logical_data_sha256,
        "material_sha256": material,
    }
    assert_public_known_gaps(manifest)


def test_counts_reconciliation_snapshot_and_file_layout_do_not_enter_material_identity() -> None:
    mapping = _mapping()
    draft = _draft(rows=())
    changed_diagnostics = replace(
        draft,
        source_records=99,
        rows=37,
        reconciliation=mapping.PreviewPackageReconciliation(
            99, Decimal("42"), Decimal("41"), Decimal("8"), Decimal("7")
        ),
    )

    assert mapping.preview_revision_content_sha256(
        logical_data_sha256=draft.logical_data_sha256
    ) == mapping.preview_revision_content_sha256(logical_data_sha256=changed_diagnostics.logical_data_sha256)


def test_requested_manifest_contract_keeps_seven_day_expiry_and_package_type() -> None:
    mapping = _mapping()
    request = _request()
    snapshot = _snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC))
    draft = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=(_row(),),
        reconciliation=mapping.PreviewPackageReconciliation(1, Decimal("8"), Decimal("8"), Decimal("5"), Decimal("5")),
        max_csv_file_bytes=None,
    )
    ready_at = datetime(2026, 7, 3, tzinfo=UTC)
    body = mapping.build_requested_preview_manifest(
        request=request,
        snapshot=snapshot,
        draft=draft,
        files=_files(draft),
        ready_at=ready_at,
        expires_at=datetime(2026, 7, 10, tzinfo=UTC),
    )

    manifest = json.loads(body)
    assert manifest["package_type"] == "requested_preview_package"
    assert manifest["lifecycle"] == {
        "ready_at": "2026-07-03T00:00:00Z",
        "expires_at": "2026-07-10T00:00:00Z",
        "retention_days": 7,
    }
    assert "expires_at" not in manifest
    assert_public_known_gaps(manifest)


def test_revision_manifest_invokes_shared_revision_invariant(monkeypatch: pytest.MonkeyPatch) -> None:
    mapping = _mapping()
    models = _models()
    draft = _draft(rows=())
    snapshot = replace(_settled_snapshot(), source_through=None)
    material = mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256)
    calls: list[str] = []
    original = models.validate_preview_revision_invariant

    def capture(**kwargs: object) -> None:
        calls.append(str(kwargs["month"]))
        original(**kwargs)

    monkeypatch.setattr(models, "validate_preview_revision_invariant", capture)
    mapping.build_preview_revision_manifest(
        revision_id="revision-1",
        tenant_name_at_publication="production",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status="settled",
        material_sha256=material,
        supersedes_revision_id=None,
        snapshot=snapshot,
        draft=draft,
        files=_files(draft),
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
    )

    assert calls == ["2026-07"]
