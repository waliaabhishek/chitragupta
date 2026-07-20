from __future__ import annotations

import csv
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


def _package(profile: str, columns: tuple[str, ...], rows: tuple[Any, ...]) -> Any:
    mapping = _mapping()
    request = _request(
        column_profile=profile,
        effective_columns=columns,
    )
    snapshot = _snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC))
    return mapping.build_preview_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=len(rows),
            source_cost=Decimal("8") * len(rows),
            allocated_cost=Decimal("8") * len(rows),
        ),
        generated_at=datetime(2026, 7, 3, tzinfo=UTC),
    )


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

    package = _package(profile, effective, (_row(AllocatedResourceId="sa-1", Tags='{"team":"a"}'),))
    manifest = json.loads(package.manifest_body)
    csv_rows = list(csv.reader(io.StringIO(package.data_files[0].body.decode())))

    assert tuple(manifest["effective_columns"]) == effective
    assert tuple(csv_rows[0]) == effective
    assert len(csv_rows) == 2


def test_full_summary_and_custom_project_the_same_hidden_rows_and_order(monkeypatch: pytest.MonkeyPatch) -> None:
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
    rendered = {name: _package(name, columns, rows) for name, columns in profiles.items()}

    for package in rendered.values():
        manifest = json.loads(package.manifest_body)
        assert manifest["validation"]["rows"] == 2
        assert manifest["reconciliation"] == {
            "source_cost": "16",
            "allocated_cost": "16",
            "difference": "0",
        }
    custom_rows = list(csv.DictReader(io.StringIO(rendered["custom"].data_files[0].body.decode())))
    assert [row["AllocatedResourceId"] for row in custom_rows] == ["sa-b", "sa-a"]


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
        mapping.build_preview_package(
            request=request,
            snapshot=snapshot,
            full_rows=(_row(),) if row_count else (),
            reconciliation=mapping.PreviewPackageReconciliation(
                source_records=source_records,
                source_cost=source_cost,
                allocated_cost=allocated_cost,
            ),
            generated_at=datetime(2026, 7, 3, tzinfo=UTC),
        )


def test_header_only_package_allows_nonempty_calculation_coverage_with_zero_sources() -> None:
    mapping = _mapping()
    request = _request()
    snapshot = _snapshot(source_through=None)

    package = mapping.build_preview_package(
        request=request,
        snapshot=snapshot,
        full_rows=(),
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=0,
            source_cost=Decimal(0),
            allocated_cost=Decimal(0),
        ),
        generated_at=datetime(2026, 7, 3, tzinfo=UTC),
    )
    manifest = json.loads(package.manifest_body)
    csv_rows = list(csv.reader(io.StringIO(package.data_files[0].body.decode())))

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
    package = mapping.build_preview_package(
        request=request,
        snapshot=snapshot,
        full_rows=(),
        reconciliation=mapping.PreviewPackageReconciliation(0, Decimal(0), Decimal(0)),
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    manifest = json.loads(package.manifest_body)

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
    _package("custom", ("BilledCost",), (_row(),))

    assert order[:2] == ["columns", "snapshot"]
