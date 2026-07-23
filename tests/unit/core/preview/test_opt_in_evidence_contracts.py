from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, tzinfo
from enum import StrEnum
from importlib import import_module

import pytest

NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)
START = datetime(2026, 7, 1, tzinfo=UTC)
MID = datetime(2026, 7, 2, tzinfo=UTC)
END = datetime(2026, 7, 3, tzinfo=UTC)


def test_organization_authority_attempt_enforces_closed_state_invariants() -> None:
    authority = import_module("core.preview.organization_authority")

    pending = authority.OrganizationAuthorityAttempt(
        attempt_sequence=1,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        status=authority.OrganizationAuthorityAttemptStatus.PENDING,
        started_at=NOW,
        completed_at=None,
        organization_id=None,
        failure_reason=None,
    )
    available = authority.OrganizationAuthorityAttempt(
        attempt_sequence=2,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        status=authority.OrganizationAuthorityAttemptStatus.AVAILABLE,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        organization_id="org-1",
        failure_reason=None,
    )

    assert pending.status.value == "pending"
    assert available.organization_id == "org-1"
    with pytest.raises(ValueError):
        authority.OrganizationAuthorityAttempt(
            attempt_sequence=3,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            status=authority.OrganizationAuthorityAttemptStatus.AVAILABLE,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
            organization_id=None,
            failure_reason=authority.OrganizationAuthorityFailureReason.PROVIDER_ERROR,
        )


@pytest.mark.parametrize(
    ("status_name", "reason_name"),
    [
        ("UNAVAILABLE", "PROVIDER_ERROR"),
        ("UNAVAILABLE", "CAPABILITY_UNAVAILABLE"),
        ("UNAVAILABLE", "RESOURCE_PERSISTENCE_FAILED"),
        ("UNAVAILABLE", "INVALID_CARDINALITY"),
        ("CONFLICTING", "INVALID_CARDINALITY"),
        ("CONFLICTING", "BINDING_CONFLICT"),
    ],
)
def test_organization_authority_accepts_only_documented_terminal_pairs(
    status_name: str,
    reason_name: str,
) -> None:
    authority = import_module("core.preview.organization_authority")
    value = authority.OrganizationAuthorityAttempt(
        attempt_sequence=4,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        status=getattr(authority.OrganizationAuthorityAttemptStatus, status_name),
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        organization_id=None,
        failure_reason=getattr(authority.OrganizationAuthorityFailureReason, reason_name),
    )

    assert value.failure_reason.value == reason_name.lower()


def test_preview_evidence_availability_is_immutable_and_collector_deduplicates() -> None:
    availability = import_module("core.preview.storage_availability")
    issue = availability.PreviewEvidenceIssue(
        revision="026",
        kind=availability.PreviewEvidenceIssueKind.SCHEMA_INCOMPATIBLE,
        error_type="PreviewEvidenceSchemaError",
    )
    collector = availability.PreviewEvidenceIssueCollector()
    collector.record(issue)
    collector.record(issue)

    result = availability.PreviewEvidenceAvailability(
        state=availability.PreviewEvidenceAvailabilityState.UNAVAILABLE,
        issues=collector.snapshot(),
    )

    assert result.issues == (issue,)
    with pytest.raises((AttributeError, TypeError)):
        result.state = availability.PreviewEvidenceAvailabilityState.READY


@pytest.mark.parametrize(
    ("status", "windows", "rows", "reason", "valid"),
    [
        ("ALREADY_CURRENT", 0, 0, None, True),
        ("BOOTSTRAPPED", 1, 2, None, True),
        ("UNAVAILABLE", 0, 0, "NO_LEGACY_EVIDENCE", True),
        ("ALREADY_CURRENT", 1, 0, None, False),
        ("BOOTSTRAPPED", 0, 2, None, False),
        ("UNAVAILABLE", 0, 0, None, False),
    ],
)
def test_bootstrap_result_invariants(
    status: str,
    windows: int,
    rows: int,
    reason: str | None,
    valid: bool,
) -> None:
    evidence = import_module("core.preview.evidence")
    kwargs = {
        "status": getattr(evidence.PreviewEvidenceBootstrapStatus, status),
        "bootstrapped_windows": windows,
        "bootstrapped_rows": rows,
        "reason": None if reason is None else getattr(evidence.PreviewEvidenceBootstrapReason, reason),
    }
    if valid:
        result = evidence.PreviewEvidenceBootstrapResult(**kwargs)
        assert result.bootstrapped_windows == windows
    else:
        with pytest.raises(ValueError):
            evidence.PreviewEvidenceBootstrapResult(**kwargs)


class _ForeignSourceAttemptStatus(StrEnum):
    PENDING = "pending"


class _ForeignSourceAttemptReason(StrEnum):
    CONSTRUCTION_FAILED = "construction_failed"


class _ForeignBootstrapStatus(StrEnum):
    ALREADY_CURRENT = "already_current"


class _ForeignBootstrapReason(StrEnum):
    NO_LEGACY_EVIDENCE = "no_legacy_evidence"


class _ForeignGatherDisposition(StrEnum):
    NOT_ATTEMPTED = "not_attempted"


class _ForeignGatherFailure(StrEnum):
    CONSTRUCTION_FAILED = "construction_failed"


class _ForeignLineageFailure(StrEnum):
    CONSTRUCTION_FAILED = "construction_failed"


class _ForeignUnavailableLineageReason(StrEnum):
    PERSISTENCE_FAILED = "persistence_failed"


@pytest.mark.parametrize("status", ["pending", object(), _ForeignSourceAttemptStatus.PENDING])
def test_source_attempt_rejects_string_and_non_enum_status(status: object) -> None:
    evidence = import_module("core.preview.evidence")

    with pytest.raises(ValueError, match="invalid source attempt status"):
        evidence.PreviewSourceAttempt(
            attempt_sequence=1,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            refresh_token="refresh-1",
            refresh_start=START,
            refresh_end=END,
            status=status,
            started_at=NOW,
            completed_at=None,
            failure_reason=None,
        )


@pytest.mark.parametrize(
    "reason",
    [
        "construction_failed",
        object(),
        _ForeignSourceAttemptReason.CONSTRUCTION_FAILED,
    ],
)
def test_source_attempt_rejects_raw_arbitrary_and_foreign_enum_reasons(reason: object) -> None:
    evidence = import_module("core.preview.evidence")

    with pytest.raises(ValueError, match="invalid source attempt failure reason"):
        evidence.PreviewSourceAttempt(
            attempt_sequence=1,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            refresh_token="refresh-1",
            refresh_start=START,
            refresh_end=END,
            status=evidence.SourceAttemptStatus.FAILED,
            started_at=NOW,
            completed_at=NOW + timedelta(seconds=1),
            failure_reason=reason,
        )


_FAILED_REASON_NAMES = {
    "ATTEMPT_BEGIN_FAILED",
    "CONSTRUCTION_FAILED",
    "PERSISTENCE_FAILED",
    "CAPABILITY_UNAVAILABLE",
    "BOOTSTRAP_INVALID",
    "BOOTSTRAP_CONCURRENT_CHANGE",
}
_ABORTED_REASON_NAMES = {"GENERIC_GATHER_FAILED", "GENERIC_COMMIT_FAILED"}
_ALL_REASON_NAMES = (None, *sorted(_FAILED_REASON_NAMES | _ABORTED_REASON_NAMES))


@pytest.mark.parametrize(
    ("status_name", "reason_name"),
    [
        (status_name, reason_name)
        for status_name in ("PENDING", "COMPLETE", "FAILED", "ABORTED")
        for reason_name in _ALL_REASON_NAMES
    ],
)
def test_source_attempt_terminal_matrix_is_exact(
    status_name: str,
    reason_name: str | None,
) -> None:
    evidence = import_module("core.preview.evidence")
    status = getattr(evidence.SourceAttemptStatus, status_name)
    reason = None if reason_name is None else getattr(evidence.SourceAttemptFailureReason, reason_name)
    completed_at = None if status_name == "PENDING" else NOW + timedelta(seconds=1)
    valid = (
        (status_name in {"PENDING", "COMPLETE"} and reason_name is None)
        or (status_name == "FAILED" and reason_name in _FAILED_REASON_NAMES)
        or (status_name == "ABORTED" and reason_name in _ABORTED_REASON_NAMES)
    )
    kwargs = {
        "attempt_sequence": 1,
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "refresh_token": "refresh-1",
        "refresh_start": START,
        "refresh_end": END,
        "status": status,
        "started_at": NOW,
        "completed_at": completed_at,
        "failure_reason": reason,
    }

    if valid:
        assert evidence.PreviewSourceAttempt(**kwargs).status is status
    else:
        with pytest.raises(ValueError):
            evidence.PreviewSourceAttempt(**kwargs)


@pytest.mark.parametrize(
    "status",
    ["already_current", object(), _ForeignBootstrapStatus.ALREADY_CURRENT],
)
def test_bootstrap_result_rejects_raw_arbitrary_and_foreign_status(status: object) -> None:
    evidence = import_module("core.preview.evidence")

    with pytest.raises(ValueError, match="invalid Preview evidence bootstrap status"):
        evidence.PreviewEvidenceBootstrapResult(
            status=status,
            bootstrapped_windows=0,
            bootstrapped_rows=0,
            reason=None,
        )


@pytest.mark.parametrize(
    "reason",
    ["no_legacy_evidence", object(), _ForeignBootstrapReason.NO_LEGACY_EVIDENCE],
)
def test_bootstrap_result_rejects_raw_arbitrary_and_foreign_reason(reason: object) -> None:
    evidence = import_module("core.preview.evidence")

    with pytest.raises(ValueError, match="invalid Preview evidence bootstrap reason"):
        evidence.PreviewEvidenceBootstrapResult(
            status=evidence.PreviewEvidenceBootstrapStatus.UNAVAILABLE,
            bootstrapped_windows=0,
            bootstrapped_rows=0,
            reason=reason,
        )


@pytest.mark.parametrize(
    "disposition",
    ["not_attempted", object(), _ForeignGatherDisposition.NOT_ATTEMPTED],
)
def test_billing_gather_result_rejects_raw_arbitrary_and_foreign_disposition(
    disposition: object,
) -> None:
    orchestrator = import_module("core.engine.orchestrator")

    with pytest.raises(ValueError, match="invalid source gather disposition"):
        orchestrator.BillingGatherResult(
            dates=frozenset(),
            source_disposition=disposition,
        )


@pytest.mark.parametrize(
    "failure",
    ["construction_failed", object(), _ForeignGatherFailure.CONSTRUCTION_FAILED],
)
def test_billing_gather_result_rejects_raw_arbitrary_and_foreign_failure(failure: object) -> None:
    orchestrator = import_module("core.engine.orchestrator")

    with pytest.raises(ValueError, match="invalid source gather failure"):
        orchestrator.BillingGatherResult(
            dates=frozenset(),
            source_disposition=orchestrator.SourceGatherDisposition.ATTEMPTED,
            source_refresh_token="refresh-1",
            source_attempt_sequence=1,
            source_failure=failure,
        )


@pytest.mark.parametrize(
    "failure",
    ["construction_failed", object(), _ForeignLineageFailure.CONSTRUCTION_FAILED],
)
def test_calculation_result_rejects_raw_arbitrary_and_foreign_lineage_failure(
    failure: object,
) -> None:
    orchestrator = import_module("core.engine.orchestrator")

    with pytest.raises(ValueError, match="invalid lineage failure"):
        orchestrator.CalculationPhaseResult(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=date(2026, 7, 1),
            rows_written=0,
            calculation_id="calculation-1",
            calculation_completed_at=NOW,
            lineage_capture=None,
            lineage_failure=failure,
        )


@pytest.mark.parametrize(
    "reason",
    ["persistence_failed", object(), _ForeignUnavailableLineageReason.PERSISTENCE_FAILED],
)
def test_unavailable_lineage_run_rejects_raw_arbitrary_and_foreign_reason(reason: object) -> None:
    evidence = import_module("core.preview.evidence")

    with pytest.raises(ValueError, match="requires a closed reason"):
        evidence.AllocationLineageUnavailableRun(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=date(2026, 7, 1),
            calculation_id="calculation-1",
            calculation_completed_at=NOW,
            status=evidence.AllocationLineageRunStatus.UNAVAILABLE,
            reason=reason,
        )


def test_native_source_window_requires_aware_ordered_bounds() -> None:
    capture = import_module("core.preview.evidence_capture")

    assert capture.NativeSourceWindow(start=START, end=MID).end == MID
    with pytest.raises(ValueError):
        capture.NativeSourceWindow(start=START.replace(tzinfo=None), end=MID)
    with pytest.raises(ValueError):
        capture.NativeSourceWindow(start=MID, end=START)
    with pytest.raises(ValueError):
        capture.SourceWindowWriteResult(records_written=-1)


def test_source_capture_receipt_requires_exact_gap_free_partition_and_count() -> None:
    evidence = import_module("core.preview.evidence")
    capture = import_module("core.preview.evidence_capture")
    first = evidence.PreviewSourceReadiness(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        window_start=START,
        window_end=MID,
        capture_id="capture-1",
        captured_at=NOW,
        source_count=1,
        attempt_sequence=7,
    )
    second = evidence.PreviewSourceReadiness(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        window_start=MID,
        window_end=END,
        capture_id="capture-2",
        captured_at=NOW,
        source_count=0,
        attempt_sequence=7,
    )

    receipt = capture.PreviewSourceCaptureReceipt(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        attempt_sequence=7,
        refresh_start=START,
        refresh_end=END,
        captures=(first, second),
        source_count=1,
    )

    assert receipt.captures == (first, second)
    with pytest.raises(ValueError):
        capture.PreviewSourceCaptureReceipt(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            attempt_sequence=7,
            refresh_start=START,
            refresh_end=END,
            captures=(first,),
            source_count=1,
        )
    with pytest.raises(ValueError):
        capture.PreviewSourceCaptureReceipt(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            attempt_sequence=7,
            refresh_start=START,
            refresh_end=END,
            captures=(first, second),
            source_count=2,
        )


def test_lineage_run_values_are_closed_and_identity_preserving() -> None:
    evidence = import_module("core.preview.evidence")
    complete = evidence.AllocationLineageRun(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-1",
        calculation_completed_at=NOW,
        status=evidence.AllocationLineageRunStatus.COMPLETE,
        portion_count=0,
    )
    unavailable = evidence.AllocationLineageUnavailableRun(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-2",
        calculation_completed_at=NOW,
        status=evidence.AllocationLineageRunStatus.UNAVAILABLE,
        reason=evidence.AllocationLineageUnavailableReason.PERSISTENCE_FAILED,
    )

    assert complete.portion_count == 0
    assert unavailable.portion_count == 0
    with pytest.raises(ValueError):
        evidence.AllocationLineageUnavailableRun(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=date(2026, 7, 1),
            calculation_id="calculation-2",
            calculation_completed_at=NOW,
            status=evidence.AllocationLineageRunStatus.COMPLETE,
            reason=evidence.AllocationLineageUnavailableReason.CAPTURE_FAILED,
        )


def test_billing_gather_result_rejects_source_state_combinations_outside_closed_matrix() -> None:
    orchestrator = import_module("core.engine.orchestrator")
    capture = import_module("core.preview.evidence_capture")

    not_attempted = orchestrator.BillingGatherResult(dates=frozenset())
    unavailable = orchestrator.BillingGatherResult(
        dates=frozenset({date(2026, 7, 1)}),
        source_disposition=orchestrator.SourceGatherDisposition.STORAGE_UNAVAILABLE,
    )

    assert not_attempted.source_disposition.value == "not_attempted"
    assert unavailable.source_disposition.value == "storage_unavailable"
    with pytest.raises(ValueError):
        orchestrator.BillingGatherResult(
            dates=frozenset(),
            source_disposition=orchestrator.SourceGatherDisposition.NOT_ATTEMPTED,
            source_refresh_token="unexpected",
        )
    with pytest.raises(ValueError):
        orchestrator.BillingGatherResult(
            dates=frozenset(),
            source_disposition=orchestrator.SourceGatherDisposition.BEGIN_FAILED,
            source_refresh_token="token",
            source_attempt_sequence=1,
            source_failure=capture.SourceCaptureFailure.CONSTRUCTION_FAILED,
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"dates_gathered": 1},
        {"errors": ["unexpected gather error"]},
        {"source_disposition": "storage_unavailable"},
        {
            "source_disposition": "attempted",
            "source_refresh_token": "refresh-1",
            "source_attempt_sequence": 1,
            "source_failure": "capability_unavailable",
        },
    ],
)
def test_skipped_gather_result_rejects_every_nonempty_gather_outcome(
    overrides: dict[str, object],
) -> None:
    orchestrator = import_module("core.engine.orchestrator")
    capture = import_module("core.preview.evidence_capture")
    canonical = {
        "storage_unavailable": orchestrator.SourceGatherDisposition.STORAGE_UNAVAILABLE,
        "attempted": orchestrator.SourceGatherDisposition.ATTEMPTED,
        "capability_unavailable": capture.SourceCaptureFailure.CAPABILITY_UNAVAILABLE,
    }
    kwargs = {
        "dates_gathered": 0,
        "errors": [],
        "skipped": True,
    }
    kwargs.update(
        {
            key: canonical[value] if isinstance(value, str) and value in canonical else value
            for key, value in overrides.items()
        }
    )

    with pytest.raises(ValueError, match="skipped gather result"):
        orchestrator.GatherResult(**kwargs)


def test_skipped_gather_result_accepts_only_the_empty_not_attempted_outcome() -> None:
    orchestrator = import_module("core.engine.orchestrator")

    result = orchestrator.GatherResult(dates_gathered=0, errors=[], skipped=True)

    assert result.source_disposition is orchestrator.SourceGatherDisposition.NOT_ATTEMPTED


def test_source_attempt_begin_failure_never_carries_exception_text() -> None:
    capture = import_module("core.preview.evidence_capture")
    value = capture.SourceAttemptBeginFailure(
        refresh_token="token-1",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_start=START,
        refresh_end=END,
        started_at=NOW,
    )

    assert set(value.__dataclass_fields__) == {
        "refresh_token",
        "ecosystem",
        "tenant_id",
        "refresh_start",
        "refresh_end",
        "started_at",
    }


def test_calculation_phase_result_requires_exact_owner_date_and_capture_identity() -> None:
    orchestrator = import_module("core.engine.orchestrator")
    from core.storage.interface import AllocationLineageRunCapture

    value = orchestrator.CalculationPhaseResult(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=date(2026, 7, 1),
        rows_written=0,
        calculation_id="calculation-1",
        calculation_completed_at=NOW,
        lineage_capture=AllocationLineageRunCapture(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=date(2026, 7, 1),
            calculation_id="calculation-1",
            captures=(),
        ),
        lineage_failure=None,
    )

    assert value.rows_written == 0
    with pytest.raises(ValueError):
        orchestrator.CalculationPhaseResult(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=date(2026, 7, 1),
            rows_written=0,
            calculation_id="calculation-1",
            calculation_completed_at=NOW,
            lineage_capture=None,
            lineage_failure=None,
        )

    class IndeterminateTimezone(tzinfo):
        def utcoffset(self, value: datetime | None) -> None:
            del value
            return None

        def dst(self, value: datetime | None) -> None:
            del value
            return None

    with pytest.raises(ValueError, match="invalid calculation result"):
        orchestrator.CalculationPhaseResult(
            ecosystem=value.ecosystem,
            tenant_id=value.tenant_id,
            tracking_date=value.tracking_date,
            rows_written=value.rows_written,
            calculation_id=value.calculation_id,
            calculation_completed_at=datetime(2026, 7, 22, tzinfo=IndeterminateTimezone()),
            lineage_capture=value.lineage_capture,
            lineage_failure=None,
        )
