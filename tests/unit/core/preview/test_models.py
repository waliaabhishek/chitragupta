from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime

import pytest

from tests.unit.core.preview.conftest import preview_module


def _queued_request(**overrides: object) -> object:
    mapping = preview_module("mapping")
    models = preview_module("models")
    values: dict[str, object] = {
        "request_id": "request-1",
        "tenant_name": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "grain": "daily",
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 2),
        "column_profile": "full",
        "status": models.PreviewRequestStatus.QUEUED,
        "created_at": datetime(2026, 7, 3, tzinfo=UTC),
        "started_at": None,
        "completed_at": None,
        "source_snapshot": None,
        "diagnostic": None,
        "storage_key": None,
        "package": None,
        "effective_columns": mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS,
    }
    values.update(overrides)
    return models.PreviewRequest(**values)


def test_preview_request_status_values_are_stable() -> None:
    models = preview_module("models")

    assert [status.value for status in models.PreviewRequestStatus] == [
        "queued",
        "running",
        "ready",
        "failed",
        "expired",
    ]


@pytest.mark.parametrize(
    "field",
    ["request_id", "tenant_name", "ecosystem", "tenant_id"],
)
def test_preview_request_rejects_blank_owner_identity(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        _queued_request(**{field: "  "})


def test_preview_request_rejects_unsupported_runtime_status() -> None:
    with pytest.raises(ValueError, match="(?i)status"):
        _queued_request(status="paused")


def test_calculation_coverage_entry_is_immutable_and_requires_usable_metadata() -> None:
    models = preview_module("models")
    entry = models.PreviewCalculationCoverageEntry(
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-a",
        calculation_completed_at=datetime(2026, 7, 3, 1, tzinfo=UTC),
        calculation_run_id=None,
    )

    assert entry.calculation_id == "calculation-a"
    with pytest.raises(FrozenInstanceError):
        entry.calculation_id = "changed"

    with pytest.raises(ValueError):
        models.PreviewCalculationCoverageEntry(
            tracking_date=date(2026, 7, 1),
            calculation_id="",
            calculation_completed_at=datetime(2026, 7, 3, 1, tzinfo=UTC),
            calculation_run_id=None,
        )

    with pytest.raises(ValueError):
        models.PreviewCalculationCoverageEntry(
            tracking_date=date(2026, 7, 1),
            calculation_id="calculation-a",
            calculation_completed_at=datetime(2026, 7, 3, 1),
            calculation_run_id=None,
        )


def test_artifact_metadata_distinguishes_manifest_from_ordered_files() -> None:
    models = preview_module("models")

    manifest = models.PreviewArtifactMetadata(
        name="manifest.json",
        media_type="application/json",
        size_bytes=12,
        sha256="a" * 64,
        order=None,
    )
    data = models.PreviewArtifactMetadata(
        name="cost-and-usage.csv",
        media_type="text/csv",
        size_bytes=13,
        sha256="b" * 64,
        order=1,
    )

    assert manifest.order is None
    assert data.order == 1


def test_source_snapshot_uses_date_ordered_coverage_and_maximum_timestamp() -> None:
    models = preview_module("models")
    early = models.PreviewCalculationCoverageEntry(
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-a",
        calculation_completed_at=datetime(2026, 7, 3, 1, tzinfo=UTC),
        calculation_run_id=10,
    )
    late = models.PreviewCalculationCoverageEntry(
        tracking_date=date(2026, 7, 2),
        calculation_id="calculation-b",
        calculation_completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        calculation_run_id=11,
    )
    snapshot = models.PreviewSourceSnapshot(
        calculation_timestamp=late.calculation_completed_at,
        calculation_coverage=(early, late),
        source_through=datetime(2026, 7, 4, tzinfo=UTC),
        effective_coverage_start_date=date(2026, 7, 1),
        effective_coverage_end_date=date(2026, 7, 3),
        availability_cutoff_end_date=None,
        monthly_status=None,
    )

    assert tuple(item.tracking_date for item in snapshot.calculation_coverage) == (
        date(2026, 7, 1),
        date(2026, 7, 2),
    )
    assert snapshot.calculation_timestamp == max(
        item.calculation_completed_at for item in snapshot.calculation_coverage
    )


@pytest.mark.parametrize(
    "invalid_order",
    [
        (date(2026, 7, 2), date(2026, 7, 1)),
        (date(2026, 7, 1), date(2026, 7, 1)),
    ],
)
def test_source_snapshot_rejects_duplicate_or_out_of_order_coverage(
    invalid_order: tuple[date, date],
) -> None:
    models = preview_module("models")
    entries = tuple(
        models.PreviewCalculationCoverageEntry(
            tracking_date=tracking_date,
            calculation_id=f"calculation-{index}",
            calculation_completed_at=datetime(2026, 7, 3, index + 1, tzinfo=UTC),
            calculation_run_id=None,
        )
        for index, tracking_date in enumerate(invalid_order)
    )

    with pytest.raises(ValueError):
        models.PreviewSourceSnapshot(
            calculation_timestamp=max(item.calculation_completed_at for item in entries),
            calculation_coverage=entries,
            source_through=datetime(2026, 7, 4, tzinfo=UTC),
            effective_coverage_start_date=date(2026, 7, 1),
            effective_coverage_end_date=date(2026, 7, 3),
            availability_cutoff_end_date=None,
            monthly_status=None,
        )
