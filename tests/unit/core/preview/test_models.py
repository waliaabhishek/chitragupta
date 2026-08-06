from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, date, datetime, timedelta

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
        "expires_at": None,
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


@pytest.mark.parametrize(
    "changes",
    [
        {"name": "../cost.csv"},
        {"name": "nested/cost.csv"},
        {"media_type": "  "},
        {"size_bytes": -1},
        {"sha256": "A" * 64},
        {"sha256": "a" * 63},
        {"order": 0},
    ],
)
def test_artifact_metadata_rejects_unsafe_or_noncanonical_values(changes: dict[str, object]) -> None:
    models = preview_module("models")
    values: dict[str, object] = {
        "name": "cost-and-usage.csv",
        "media_type": "text/csv",
        "size_bytes": 4,
        "sha256": "a" * 64,
        "order": 1,
    }
    values.update(changes)

    with pytest.raises(ValueError):
        models.PreviewArtifactMetadata(**values)


def test_package_metadata_requires_unique_names_and_contiguous_file_order() -> None:
    models = preview_module("models")
    manifest = models.PreviewArtifactMetadata("manifest.json", "application/json", 2, "a" * 64, None)
    first = models.PreviewArtifactMetadata("part-1.csv", "text/csv", 3, "b" * 64, 1)
    duplicate_name = models.PreviewArtifactMetadata("part-1.csv", "text/csv", 3, "c" * 64, 2)
    third = models.PreviewArtifactMetadata("part-3.csv", "text/csv", 3, "d" * 64, 3)

    with pytest.raises(ValueError):
        models.PreviewPackageMetadata(manifest=manifest, files=(first, duplicate_name))
    with pytest.raises(ValueError):
        models.PreviewPackageMetadata(manifest=manifest, files=(first, third))


def test_ready_and_expired_request_require_exact_seven_day_expiry() -> None:
    models = preview_module("models")
    completed = datetime(2026, 7, 3, 2, tzinfo=UTC)
    snapshot_module = __import__(
        "tests.unit.core.preview.test_lifecycle_snapshot_v5", fromlist=["_package", "_snapshot"]
    )
    values = {
        "status": models.PreviewRequestStatus.READY,
        "started_at": datetime(2026, 7, 3, 1, tzinfo=UTC),
        "completed_at": completed,
        "expires_at": completed + timedelta(days=7),
        "source_snapshot": snapshot_module._snapshot(),
        "storage_key": "opaque-key",
        "package": snapshot_module._package(),
    }

    ready = _queued_request(**values)
    assert ready.expires_at == completed + timedelta(days=7)

    with pytest.raises(ValueError, match="expires"):
        _queued_request(**{**values, "expires_at": completed + timedelta(days=6)})
    with pytest.raises(ValueError, match="expires"):
        _queued_request(**{**values, "expires_at": datetime(2026, 7, 10, 2)})

    expired = _queued_request(**{**values, "status": models.PreviewRequestStatus.EXPIRED, "storage_key": None})
    assert expired.storage_key is None
    assert expired.package == values["package"]


@pytest.mark.parametrize("status", ["queued", "running", "failed"])
def test_nonready_request_states_reject_expiry(status: str) -> None:
    models = preview_module("models")
    changes: dict[str, object] = {
        "status": models.PreviewRequestStatus(status),
        "expires_at": datetime(2026, 7, 10, tzinfo=UTC),
    }
    if status == "running":
        changes["started_at"] = datetime(2026, 7, 3, 1, tzinfo=UTC)
    if status == "failed":
        changes.update(
            started_at=datetime(2026, 7, 3, 1, tzinfo=UTC),
            completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
            diagnostic=models.PreviewDiagnostic("failed", "failed", False),
        )

    with pytest.raises(ValueError, match="expires"):
        _queued_request(**changes)


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
