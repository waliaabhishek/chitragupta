from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from typing import Any

import pytest


def _models() -> Any:
    return import_module("core.preview.models")


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _coverage(tracking_date: date = date(2026, 7, 1)) -> Any:
    models = _models()
    return models.PreviewCalculationCoverageEntry(
        tracking_date=tracking_date,
        calculation_id=f"calculation-{tracking_date.isoformat()}",
        calculation_completed_at=datetime.combine(tracking_date + timedelta(days=2), datetime.min.time(), tzinfo=UTC),
        calculation_run_id=17,
    )


def _snapshot(
    *,
    start: date = date(2026, 7, 1),
    end: date = date(2026, 7, 2),
    monthly_status: str | None = None,
    cutoff: date | None = None,
    source_through: datetime | None = datetime(2026, 7, 2, tzinfo=UTC),
) -> Any:
    models = _models()
    entries = tuple(_coverage(start + timedelta(days=offset)) for offset in range((end - start).days))
    timestamp = max((entry.calculation_completed_at for entry in entries), default=None)
    return models.PreviewSourceSnapshot(
        calculation_timestamp=timestamp,
        calculation_coverage=entries,
        source_through=source_through if entries else None,
        effective_coverage_start_date=start,
        effective_coverage_end_date=end,
        availability_cutoff_end_date=cutoff,
        monthly_status=monthly_status,
    )


def _package() -> Any:
    models = _models()
    manifest = models.PreviewArtifactMetadata(
        name="manifest.json",
        media_type="application/json",
        size_bytes=2,
        sha256="a" * 64,
        order=None,
    )
    data = models.PreviewArtifactMetadata(
        name="cost-and-usage.csv",
        media_type="text/csv",
        size_bytes=3,
        sha256="b" * 64,
        order=1,
    )
    return models.PreviewPackageMetadata(manifest=manifest, files=(data,))


def _request(*, status: str = "running", grain: str = "daily", **overrides: object) -> Any:
    models = _models()
    start = date(2026, 7, 1)
    end = date(2026, 8, 1) if grain == "monthly" else date(2026, 7, 2)
    status_value = models.PreviewRequestStatus(status)
    values: dict[str, object] = {
        "request_id": "request-1",
        "tenant_name": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "grain": grain,
        "start_date": start,
        "end_date": end,
        "column_profile": "full",
        "effective_columns": _mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        "status": status_value,
        "created_at": datetime(2026, 7, 3, tzinfo=UTC),
        "started_at": datetime(2026, 7, 3, 1, tzinfo=UTC) if status != "queued" else None,
        "completed_at": None,
        "expires_at": None,
        "source_snapshot": None,
        "diagnostic": None,
        "storage_key": None,
        "package": None,
    }
    values.update(overrides)
    return models.PreviewRequest(**values)


def test_snapshot_accepts_empty_monthly_evidence_with_nullable_timestamps() -> None:
    snapshot = _snapshot(
        end=date(2026, 7, 1),
        monthly_status="provisional",
        cutoff=date(2026, 7, 1),
        source_through=None,
    )

    assert snapshot.calculation_coverage == ()
    assert snapshot.calculation_timestamp is None
    assert snapshot.source_through is None


def test_snapshot_accepts_complete_calculation_coverage_with_zero_sources() -> None:
    snapshot = _snapshot(source_through=None)

    assert len(snapshot.calculation_coverage) == 1
    assert snapshot.calculation_timestamp == snapshot.calculation_coverage[0].calculation_completed_at
    assert snapshot.source_through is None


@pytest.mark.parametrize(
    "changes",
    [
        {"effective_coverage_end_date": date(2026, 7, 3)},
        {"calculation_timestamp": None},
        {"calculation_coverage": ()},
        {"calculation_timestamp": datetime(2026, 7, 9, tzinfo=UTC)},
    ],
)
def test_snapshot_rejects_coverage_not_equal_to_effective_interval(changes: dict[str, object]) -> None:
    snapshot = _snapshot()

    with pytest.raises(ValueError):
        replace(snapshot, **changes)


def test_candidate_ready_accepts_running_request_and_detached_proposed_snapshot() -> None:
    models = _models()
    request = _request()
    snapshot = _snapshot()

    models.validate_preview_request_snapshot(
        request=request,
        snapshot=snapshot,
        resulting_status=models.PreviewRequestStatus.READY,
        mode="candidate_ready",
    )


@pytest.mark.parametrize(
    ("grain", "start", "end", "message"),
    [
        ("daily", date(2026, 7, 2), date(2026, 7, 2), "daily request"),
        ("daily", date(2026, 7, 31), date(2026, 8, 2), "calendar month"),
        ("monthly", date(2026, 7, 2), date(2026, 8, 1), "month_start"),
    ],
)
def test_snapshotless_strict_validation_still_rejects_invalid_request_interval(
    grain: str,
    start: date,
    end: date,
    message: str,
) -> None:
    models = _models()
    request = _request(status="queued", grain=grain, start_date=start, end_date=end)

    with pytest.raises(ValueError, match=message):
        models.validate_preview_request_snapshot(
            request=request,
            snapshot=None,
            resulting_status=models.PreviewRequestStatus.QUEUED,
            mode="strict_materialized",
        )


@pytest.mark.parametrize("materialized_status", ["queued", "ready", "failed", "expired"])
def test_candidate_ready_rejects_every_materialized_state_except_running(materialized_status: str) -> None:
    models = _models()
    if materialized_status == "queued":
        request = _request(status="queued")
    elif materialized_status == "failed":
        request = _request(
            status="failed",
            completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
            diagnostic=models.PreviewDiagnostic("failed", "failed", False),
        )
    else:
        snapshot = _snapshot()
        request = _request(
            status=materialized_status,
            completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
            expires_at=datetime(2026, 7, 10, 2, tzinfo=UTC),
            source_snapshot=snapshot,
            storage_key=None if materialized_status == "expired" else "request-1",
            package=_package(),
        )

    with pytest.raises(ValueError):
        models.validate_preview_request_snapshot(
            request=request,
            snapshot=_snapshot(),
            resulting_status=models.PreviewRequestStatus.READY,
            mode="candidate_ready",
        )


def test_strict_materialized_requires_result_status_and_snapshot_identity() -> None:
    models = _models()
    snapshot = _snapshot()
    ready = _request(
        status="ready",
        completed_at=datetime(2026, 7, 3, 2, tzinfo=UTC),
        expires_at=datetime(2026, 7, 10, 2, tzinfo=UTC),
        source_snapshot=snapshot,
        storage_key="request-1",
        package=_package(),
    )

    models.validate_preview_request_snapshot(
        request=ready,
        snapshot=snapshot,
        resulting_status=models.PreviewRequestStatus.READY,
        mode="strict_materialized",
    )
    with pytest.raises(ValueError):
        models.validate_preview_request_snapshot(
            request=ready,
            snapshot=replace(snapshot, source_through=datetime(2026, 7, 2, 1, tzinfo=UTC)),
            resulting_status=models.PreviewRequestStatus.READY,
            mode="strict_materialized",
        )
    with pytest.raises(ValueError):
        models.validate_preview_request_snapshot(
            request=ready,
            snapshot=snapshot,
            resulting_status=models.PreviewRequestStatus.RUNNING,
            mode="strict_materialized",
        )


def test_daily_snapshot_rejects_monthly_cutoff_or_status() -> None:
    models = _models()
    request = _request()

    with pytest.raises(ValueError):
        models.validate_preview_request_snapshot(
            request=request,
            snapshot=_snapshot(monthly_status="provisional", cutoff=date(2026, 7, 2)),
            resulting_status=models.PreviewRequestStatus.READY,
            mode="candidate_ready",
        )


@pytest.mark.parametrize(
    ("created_at", "cutoff", "status", "effective_end"),
    [
        (datetime(2026, 7, 15, tzinfo=UTC), date(2026, 7, 14), "provisional", date(2026, 7, 14)),
        (datetime(2026, 8, 4, tzinfo=UTC), date(2026, 8, 1), "settled", date(2026, 8, 1)),
    ],
)
def test_monthly_snapshot_matches_frozen_submission_settlement(
    created_at: datetime,
    cutoff: date,
    status: str,
    effective_end: date,
) -> None:
    models = _models()
    request = _request(grain="monthly", created_at=created_at, started_at=created_at + timedelta(minutes=1))
    snapshot = _snapshot(
        end=effective_end,
        monthly_status=status,
        cutoff=cutoff,
        source_through=None,
    )

    models.validate_preview_request_snapshot(
        request=request,
        snapshot=snapshot,
        resulting_status=models.PreviewRequestStatus.READY,
        mode="candidate_ready",
    )


def test_monthly_snapshot_rejects_live_clock_reclassification() -> None:
    models = _models()
    request = _request(
        grain="monthly",
        created_at=datetime(2026, 7, 15, tzinfo=UTC),
        started_at=datetime(2026, 8, 10, tzinfo=UTC),
    )
    incorrectly_settled = _snapshot(
        end=date(2026, 8, 1),
        monthly_status="settled",
        cutoff=date(2026, 8, 1),
        source_through=None,
    )

    with pytest.raises(ValueError):
        models.validate_preview_request_snapshot(
            request=request,
            snapshot=incorrectly_settled,
            resulting_status=models.PreviewRequestStatus.READY,
            mode="candidate_ready",
        )
