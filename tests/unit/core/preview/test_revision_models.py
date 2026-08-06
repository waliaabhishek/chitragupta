from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from typing import Any

import pytest


def _models() -> Any:
    return import_module("core.preview.models")


def _coverage(start: date, end: date) -> tuple[Any, ...]:
    models = _models()
    return tuple(
        models.PreviewCalculationCoverageEntry(
            tracking_date=tracking_date,
            calculation_id=f"calculation-{tracking_date.isoformat()}",
            calculation_completed_at=datetime.combine(
                tracking_date + timedelta(days=2), datetime.min.time(), tzinfo=UTC
            ),
            calculation_run_id=17,
        )
        for tracking_date in (start + timedelta(days=offset) for offset in range((end - start).days))
    )


def _snapshot(
    *,
    status: str = "settled",
    start: date = date(2026, 7, 1),
    end: date = date(2026, 8, 1),
    cutoff: date | None = date(2026, 8, 1),
) -> Any:
    models = _models()
    coverage = _coverage(start, end)
    return models.PreviewSourceSnapshot(
        calculation_timestamp=max((item.calculation_completed_at for item in coverage), default=None),
        calculation_coverage=coverage,
        source_through=None if not coverage else datetime(2026, 8, 1, tzinfo=UTC),
        effective_coverage_start_date=start,
        effective_coverage_end_date=end,
        availability_cutoff_end_date=cutoff,
        monthly_status=status,
    )


def _package() -> Any:
    models = _models()
    return models.PreviewStoredPackage(
        storage_key="revision-1",
        manifest=models.PreviewArtifactMetadata("manifest.json", "application/json", 2, "a" * 64, None),
        files=(models.PreviewArtifactMetadata("cost-and-usage.csv", "text/csv", 8, "b" * 64, 1),),
    )


def _candidate(**overrides: object) -> Any:
    models = _models()
    values: dict[str, object] = {
        "revision_id": "revision-1",
        "tenant_name_at_publication": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "month": "2026-07",
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 8, 1),
        "monthly_status": "settled",
        "material_sha256": "c" * 64,
        "source_snapshot": _snapshot(),
        "published_at": datetime(2026, 8, 4, tzinfo=UTC),
        "supersedes_revision_id": None,
    }
    values.update(overrides)
    return models.PreviewRevisionCandidate(**values)


def _revision(**overrides: object) -> Any:
    models = _models()
    candidate = _candidate()
    values = {
        **candidate.__dict__,
        "superseded_by_revision_id": None,
        "is_current": True,
        "package": _package(),
    }
    values.update(overrides)
    return models.PreviewRevision(**values)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"month": "2026-7"}, "month"),
        ({"start_date": date(2026, 7, 2)}, "month"),
        ({"end_date": date(2026, 7, 31)}, "month"),
        ({"monthly_status": "provisional"}, "status"),
        (
            {"source_snapshot": _snapshot(start=date(2026, 7, 2))},
            "coverage",
        ),
        (
            {
                "source_snapshot": replace(
                    _snapshot(status="provisional", end=date(2026, 7, 20), cutoff=date(2026, 7, 19)),
                    effective_coverage_end_date=date(2026, 7, 20),
                ),
                "monthly_status": "provisional",
            },
            "cutoff",
        ),
        (
            {"source_snapshot": _snapshot(status="settled", end=date(2026, 7, 31), cutoff=date(2026, 8, 1))},
            "coverage",
        ),
        (
            {"source_snapshot": _snapshot(status="settled", end=date(2026, 8, 1), cutoff=date(2026, 7, 31))},
            "cutoff",
        ),
        (
            {
                "source_snapshot": _snapshot(status="provisional", end=date(2026, 7, 20), cutoff=None),
                "monthly_status": "provisional",
            },
            "cutoff",
        ),
        (
            {
                "source_snapshot": _snapshot(status="provisional", end=date(2026, 8, 2), cutoff=date(2026, 8, 2)),
                "monthly_status": "provisional",
            },
            "coverage",
        ),
    ],
)
def test_candidate_rejects_revision_scope_snapshot_mismatches(changes: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _candidate(**changes)


def test_revision_and_candidate_are_frozen_and_normalize_publication_time() -> None:
    candidate = _candidate(published_at=datetime(2026, 8, 3, 17, tzinfo=UTC))
    revision = _revision(published_at=datetime(2026, 8, 3, 17, tzinfo=UTC))

    assert candidate.published_at == datetime(2026, 8, 3, 17, tzinfo=UTC)
    assert revision.published_at == datetime(2026, 8, 3, 17, tzinfo=UTC)
    with pytest.raises((AttributeError, TypeError)):
        candidate.month = "2026-08"
    with pytest.raises((AttributeError, TypeError)):
        revision.is_current = False


@pytest.mark.parametrize(
    "changes",
    [
        {"revision_id": ""},
        {"tenant_name_at_publication": " "},
        {"ecosystem": "aws"},
        {"tenant_id": ""},
        {"material_sha256": "A" * 64},
        {"material_sha256": "not-a-digest"},
        {"published_at": datetime(2026, 8, 4)},
        {"revision_id": "revision-1", "supersedes_revision_id": "revision-1"},
    ],
)
def test_candidate_rejects_invalid_identity_digest_and_timestamp(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _candidate(**changes)


def test_revision_rejects_current_successor_and_noncurrent_without_successor() -> None:
    with pytest.raises(ValueError):
        _revision(superseded_by_revision_id="revision-2", is_current=True)
    with pytest.raises(ValueError):
        _revision(superseded_by_revision_id=None, is_current=False)


def test_shared_revision_invariant_is_used_by_both_domain_types(monkeypatch: pytest.MonkeyPatch) -> None:
    models = _models()
    calls: list[str] = []
    original = models.validate_preview_revision_invariant

    def capture(**kwargs: object) -> None:
        calls.append(str(kwargs["month"]))
        original(**kwargs)

    monkeypatch.setattr(models, "validate_preview_revision_invariant", capture)

    _candidate()
    _revision()

    assert calls == ["2026-07", "2026-07", "2026-07"]


def test_revision_retention_pending_timestamp_is_optional_and_normalized() -> None:
    pending = datetime(2026, 8, 4, 3, tzinfo=UTC)

    assert _revision().retention_pending_at is None
    assert _revision(retention_pending_at=pending).retention_pending_at == pending


def test_revision_rejects_naive_retention_pending_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone"):
        _revision(retention_pending_at=datetime(2026, 8, 4, 3))


def test_revision_validation_summary_accepts_only_passed_nonnegative_integer_counts() -> None:
    models = _models()

    summary = models.PreviewRevisionValidationSummary(
        status="passed",
        mapping_profile_version="focus-1.4-v1",
        source_records=17,
        rows=9,
        mapping_errors=0,
        artifact_integrity="passed",
    )

    assert summary.source_records == 17
    assert summary.rows == 9


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "failed"},
        {"mapping_profile_version": " "},
        {"source_records": -1},
        {"source_records": True},
        {"rows": -1},
        {"rows": False},
        {"mapping_errors": 1},
        {"mapping_errors": 0.0},
        {"mapping_errors": False},
        {"artifact_integrity": "failed"},
    ],
)
def test_revision_validation_summary_rejects_invalid_values(changes: dict[str, object]) -> None:
    models = _models()
    values: dict[str, object] = {
        "status": "passed",
        "mapping_profile_version": "focus-1.4-v1",
        "source_records": 17,
        "rows": 9,
        "mapping_errors": 0,
        "artifact_integrity": "passed",
    }
    values.update(changes)

    with pytest.raises(ValueError):
        models.PreviewRevisionValidationSummary(**values)
