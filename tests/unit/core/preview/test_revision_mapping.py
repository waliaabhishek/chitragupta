from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request, _snapshot
from tests.unit.core.preview.test_monthly_v5 import _row


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
