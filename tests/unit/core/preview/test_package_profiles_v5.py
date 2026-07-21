from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from importlib import import_module
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request, _snapshot
from tests.unit.core.preview.test_monthly_v5 import _row


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _draft(
    profile: str,
    columns: tuple[str, ...],
    rows: tuple[Any, ...],
    *,
    max_csv_file_bytes: int | None = None,
) -> Any:
    mapping = _mapping()
    request = _request(
        column_profile=profile,
        effective_columns=columns,
    )
    snapshot = _snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC))
    return mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=len(rows),
            source_cost=Decimal("8") * len(rows),
            allocated_cost=Decimal("8") * len(rows),
            source_quantity=Decimal("5") * len(rows),
            allocated_quantity=Decimal("5") * len(rows),
        ),
        max_csv_file_bytes=max_csv_file_bytes,
    )


def _manifest(
    profile: str,
    columns: tuple[str, ...],
    draft: Any,
    *,
    request: Any | None = None,
    snapshot: Any | None = None,
) -> dict[str, Any]:
    mapping = _mapping()
    request = request or _request(column_profile=profile, effective_columns=columns)
    snapshot = snapshot or _snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC))
    files = tuple(
        import_module("core.preview.models").PreviewArtifactMetadata(
            item.name,
            item.media_type,
            len(item.body),
            hashlib.sha256(item.body).hexdigest(),
            item.order,
        )
        for item in draft.data_files
    )
    body = mapping.build_requested_preview_manifest(
        request=request,
        snapshot=snapshot,
        draft=draft,
        files=files,
        ready_at=datetime(2026, 7, 3, tzinfo=UTC),
        expires_at=datetime(2026, 7, 10, tzinfo=UTC),
    )
    return json.loads(body)


@pytest.mark.parametrize(
    ("profile", "columns"),
    [
        ("full", None),
        ("summary", None),
        ("custom", ("Tags", "BilledCost", "AllocatedResourceId")),
    ],
)
def test_package_csv_header_exactly_matches_manifest_effective_columns(
    monkeypatch: pytest.MonkeyPatch,
    profile: str,
    columns: tuple[str, ...] | None,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    effective = columns
    if effective is None:
        effective = mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS if profile == "full" else mapping.FOCUS_1_4_SUMMARY_COLUMNS

    draft = _draft(profile, effective, (_row(AllocatedResourceId="sa-1", Tags='{"team":"a"}'),))
    manifest = _manifest(profile, effective, draft)
    csv_rows = list(csv.reader(io.StringIO(draft.data_files[0].body.decode())))

    assert tuple(manifest["effective_columns"]) == effective
    assert tuple(csv_rows[0]) == effective
    assert len(csv_rows) == 2


def test_full_summary_and_custom_project_the_same_canonical_hidden_row_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    rows = (
        _row(day=1, AllocatedResourceId="sa-b", Tags='{"team":"b"}'),
        _row(day=2, AllocatedResourceId="sa-a", Tags='{"team":"a"}'),
    )
    profiles = {
        "full": mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        "summary": mapping.FOCUS_1_4_SUMMARY_COLUMNS,
        "custom": ("AllocatedResourceId", "Tags", "BilledCost"),
    }
    rendered = {name: _draft(name, columns, rows) for name, columns in profiles.items()}

    for name, draft in rendered.items():
        manifest = _manifest(name, profiles[name], draft)
        assert manifest["validation"]["rows"] == 2
        assert manifest["reconciliation"] == {
            "source_cost": "16",
            "allocated_cost": "16",
            "difference": "0",
            "source_quantity": "10",
            "allocated_quantity": "10",
            "quantity_difference": "0",
        }
    custom_rows = list(csv.DictReader(io.StringIO(rendered["custom"].data_files[0].body.decode())))
    assert [row["AllocatedResourceId"] for row in custom_rows] == ["sa-a", "sa-b"]


@pytest.mark.parametrize(
    ("source_records", "source_cost", "allocated_cost", "row_count"),
    [
        (0, Decimal("1"), Decimal(0), 0),
        (0, Decimal(0), Decimal("1"), 0),
        (0, Decimal(0), Decimal(0), 1),
        (1, Decimal("8"), Decimal("8"), 0),
    ],
)
def test_package_rejects_inconsistent_zero_source_or_positive_source_state(
    monkeypatch: pytest.MonkeyPatch,
    source_records: int,
    source_cost: Decimal,
    allocated_cost: Decimal,
    row_count: int,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    request = _request()
    snapshot = _snapshot(source_through=None if source_records == 0 else datetime(2026, 7, 2, tzinfo=UTC))

    with pytest.raises(mapping.PreviewMappingError):
        mapping.build_preview_data_package(
            request=request,
            snapshot=snapshot,
            full_rows=(_row(),) if row_count else (),
            reconciliation=mapping.PreviewPackageReconciliation(
                source_records=source_records,
                source_cost=source_cost,
                allocated_cost=allocated_cost,
                source_quantity=Decimal(0),
                allocated_quantity=Decimal(0),
            ),
            max_csv_file_bytes=None,
        )


def test_header_only_package_allows_nonempty_calculation_coverage_with_zero_sources() -> None:
    mapping = _mapping()
    request = _request()
    snapshot = _snapshot(source_through=None)

    draft = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=(),
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=0,
            source_cost=Decimal(0),
            allocated_cost=Decimal(0),
            source_quantity=Decimal(0),
            allocated_quantity=Decimal(0),
        ),
        max_csv_file_bytes=None,
    )
    manifest = _manifest("full", mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS, draft, snapshot=snapshot)
    csv_rows = list(csv.reader(io.StringIO(draft.data_files[0].body.decode())))

    assert len(csv_rows) == 1
    assert manifest["source_snapshot"]["calculation_timestamp"] is not None
    assert manifest["source_snapshot"]["source_through"] is None
    assert manifest["validation"]["source_records"] == 0


def test_empty_provisional_month_serializes_nullable_timestamps_without_formatter_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = _mapping()
    request = _request(
        grain="monthly",
        end_date=date(2026, 8, 1),
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
        started_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
    )
    snapshot = _snapshot(
        end=date(2026, 7, 1),
        monthly_status="provisional",
        cutoff=date(2026, 7, 1),
        source_through=None,
    )
    original = mapping.preview_utc_text

    def reject_none(value: datetime) -> str:
        assert value is not None
        return original(value)

    monkeypatch.setattr(mapping, "preview_utc_text", reject_none)
    draft = mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=(),
        reconciliation=mapping.PreviewPackageReconciliation(0, Decimal(0), Decimal(0), Decimal(0), Decimal(0)),
        max_csv_file_bytes=None,
    )
    manifest = _manifest(
        "monthly",
        mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
        draft,
        request=request,
        snapshot=snapshot,
    )

    assert manifest["month"] == "2026-07"
    assert manifest["monthly_status"] == "provisional"
    assert manifest["source_snapshot"]["calculation_timestamp"] is None
    assert manifest["source_snapshot"]["source_through"] is None
    assert manifest["evidence_coverage"]["evidence_through_date"] is None


def test_package_validates_effective_columns_before_snapshot_or_rendering(monkeypatch: pytest.MonkeyPatch) -> None:
    mapping = _mapping()
    order: list[str] = []
    original_columns = mapping.validate_preview_effective_columns
    original_snapshot = mapping.validate_preview_request_snapshot

    def columns(profile: str, effective: tuple[str, ...]) -> None:
        order.append("columns")
        original_columns(profile, effective)

    def snapshot(**kwargs: object) -> None:
        order.append("snapshot")
        original_snapshot(**kwargs)

    monkeypatch.setattr(mapping, "validate_preview_effective_columns", columns)
    monkeypatch.setattr(mapping, "validate_preview_request_snapshot", snapshot)
    _draft("custom", ("BilledCost",), (_row(),))

    assert order[:2] == ["columns", "snapshot"]


def test_partitioning_uses_utf8_record_bytes_repeats_headers_and_preserves_every_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    columns = ("AllocatedResourceId", "ResourceName", "BilledCost")
    rows = (
        _row(AllocatedResourceId="sa-c", ResourceName="東京", BilledCost=Decimal("3")),
        _row(AllocatedResourceId="sa-a", ResourceName='quoted,"name"', BilledCost=Decimal("1")),
        _row(AllocatedResourceId="sa-b", ResourceName="plain", BilledCost=Decimal("2")),
    )
    baseline = _draft("custom", columns, rows)
    body = baseline.data_files[0].body
    header = body.splitlines(keepends=True)[0]
    records = body.splitlines(keepends=True)[1:]
    limit = len(header) + max(len(record) for record in records)

    partitioned = _draft("custom", columns, rows, max_csv_file_bytes=limit)

    assert len(partitioned.data_files) > 1
    assert [item.order for item in partitioned.data_files] == list(range(1, len(partitioned.data_files) + 1))
    total = len(partitioned.data_files)
    assert [item.name for item in partitioned.data_files] == [
        f"cost-and-usage-part-{index:05d}-of-{total:05d}.csv" for index in range(1, total + 1)
    ]
    parsed = []
    for item in partitioned.data_files:
        assert len(item.body) <= limit
        assert item.body.startswith(header)
        parsed.extend(list(csv.reader(io.StringIO(item.body.decode())))[1:])
    assert parsed == list(csv.reader(io.StringIO(body.decode())))[1:]


def test_exact_fit_stays_in_part_and_one_byte_less_splits(monkeypatch: pytest.MonkeyPatch) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    columns = ("AllocatedResourceId", "BilledCost")
    baseline = _draft("custom", columns, (_row(AllocatedResourceId="sa-a"), _row(AllocatedResourceId="sa-b")))
    exact_size = len(baseline.data_files[0].body)

    assert (
        len(
            _draft(
                "custom",
                columns,
                (_row(AllocatedResourceId="sa-a"), _row(AllocatedResourceId="sa-b")),
                max_csv_file_bytes=exact_size,
            ).data_files
        )
        == 1
    )
    assert (
        len(
            _draft(
                "custom",
                columns,
                (_row(AllocatedResourceId="sa-a"), _row(AllocatedResourceId="sa-b")),
                max_csv_file_bytes=exact_size - 1,
            ).data_files
        )
        == 2
    )


def test_disabled_and_oversized_limits_return_identical_canonical_one_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    columns = ("AllocatedResourceId", "BilledCost")
    rows = (_row(AllocatedResourceId="sa-b"), _row(AllocatedResourceId="sa-a"))
    disabled = _draft("custom", columns, rows)
    oversized = _draft("custom", columns, tuple(reversed(rows)), max_csv_file_bytes=10_000_000)

    assert disabled.data_files == oversized.data_files
    assert disabled.data_files[0].name == "cost-and-usage.csv"


def test_header_or_single_row_overflow_fails_without_partial_draft(monkeypatch: pytest.MonkeyPatch) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)

    with pytest.raises(mapping.PreviewMappingError, match="file-size limit"):
        _draft("custom", ("AllocatedResourceId",), (), max_csv_file_bytes=1)
    with pytest.raises(mapping.PreviewMappingError, match="file-size limit"):
        _draft(
            "custom",
            ("AllocatedResourceId",),
            (_row(AllocatedResourceId="x" * 100),),
            max_csv_file_bytes=32,
        )


def test_manifest_v2_is_canonical_nonrecursive_and_has_exact_lifecycle_and_integrity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = _mapping()
    monkeypatch.setattr(mapping, "validate_preview_row", lambda **_kwargs: None)
    columns = ("AllocatedResourceId", "BilledCost")
    draft = _draft("custom", columns, (_row(AllocatedResourceId="sa-a"),))
    manifest = _manifest("custom", columns, draft)

    assert manifest["schema_version"] == "chitragupta.preview-manifest.v2"
    assert manifest["generated_at"] == "2026-07-03T00:00:00Z"
    assert manifest["lifecycle"] == {
        "ready_at": "2026-07-03T00:00:00Z",
        "expires_at": "2026-07-10T00:00:00Z",
        "retention_days": 7,
    }
    assert manifest["validation"]["artifact_integrity"] == "passed"
    assert [entry["name"] for entry in manifest["files"]] == [item.name for item in draft.data_files]
    assert "manifest.json" not in [entry["name"] for entry in manifest["files"]]
