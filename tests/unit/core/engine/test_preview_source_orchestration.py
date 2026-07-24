from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.engine.orchestrator import (
    ChargebackOrchestrator,
    GatherPhase,
    GatherPlan,
    GatherResult,
    SourceAttemptBeginFailure,
    SourceCaptureFailure,
    SourceEvidenceStorageUnavailable,
    SourceGatherDisposition,
)
from core.models.billing import CoreBillingLineItem
from core.preview.evidence import (
    PreviewSourceAttempt,
    SourceAttemptFailureReason,
    SourceAttemptFinalStatus,
    SourceAttemptStatus,
)
from core.preview.evidence_capture import NativeSourceEvidenceCapture
from core.preview.storage_availability import (
    PreviewEvidenceAvailability,
    PreviewEvidenceAvailabilityState,
)
from tests.unit.core.preview.evidence_backend_double import (
    PreviewEvidenceBackendDouble,
    preview_evidence_backend_double,
)

NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)
START = NOW - timedelta(days=1)


class _Context:
    def __init__(self, value: Any, events: list[str], name: str) -> None:
        self.value = value
        self.events = events
        self.name = name

    def __enter__(self) -> Any:
        self.events.append(f"{self.name}:enter")
        return self.value

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.events.append(f"{self.name}:exit")


def _pending() -> PreviewSourceAttempt:
    return PreviewSourceAttempt(
        attempt_sequence=11,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_token="refresh-1",
        refresh_start=START,
        refresh_end=NOW,
        status=SourceAttemptStatus.PENDING,
        started_at=NOW,
        completed_at=None,
        failure_reason=None,
    )


def _uow(events: list[str], name: str, *, commit_error: Exception | None = None) -> MagicMock:
    uow = MagicMock()
    uow.commit.side_effect = commit_error
    uow.commit.side_effect = commit_error if commit_error is not None else lambda: events.append(f"{name}:commit")
    uow.pipeline_state.find_needing_calculation.return_value = []
    return uow


def _orchestrator(
    backend: PreviewEvidenceBackendDouble,
    gather_phase: MagicMock,
) -> ChargebackOrchestrator:
    orchestrator = object.__new__(ChargebackOrchestrator)
    orchestrator._tenant_name = "production"
    orchestrator._tenant_id = "tenant-1"
    orchestrator._ecosystem = "confluent_cloud"
    orchestrator._storage_backend = backend
    orchestrator._tenant_config = MagicMock(
        focus_preview_enabled=True,
        lookback_days=1,
        cutoff_days=0,
    )
    orchestrator._gather_phase = gather_phase
    orchestrator._calculate_phase = MagicMock()
    orchestrator._consecutive_gather_failures = 0
    orchestrator._gather_failure_threshold = 3
    orchestrator._topic_overlay_phase = None
    orchestrator._shutdown_check = None
    orchestrator._progress_callback = None
    return orchestrator


def _backend() -> PreviewEvidenceBackendDouble:
    return preview_evidence_backend_double()


def _plan() -> GatherPlan:
    return GatherPlan(
        now=NOW,
        refresh_start=START,
        refresh_end=NOW,
        should_refresh=True,
    )


def test_enabled_source_capture_is_persisted_only_after_generic_commit_and_uow_exit() -> None:
    events: list[str] = []
    capture = MagicMock(spec=NativeSourceEvidenceCapture)
    capture.persist.side_effect = lambda *args, **kwargs: events.append("capture:persist")
    gather_result = GatherResult(
        dates_gathered=1,
        errors=[],
        source_disposition=SourceGatherDisposition.ATTEMPTED,
        source_refresh_token="refresh-1",
        source_attempt_sequence=11,
        source_capture=capture,
        source_failure=None,
    )
    gather_phase = MagicMock()
    gather_phase.plan_refresh.return_value = _plan()
    gather_phase.run.side_effect = lambda *args, **kwargs: events.append("provider:gather") or gather_result
    backend = _backend()
    begin_uow = _uow(events, "begin")
    begin_uow.source_readiness.begin_attempt.return_value = _pending()
    generic_uow = _uow(events, "generic")
    persist_uow = _uow(events, "persist")
    pending_uow = _uow(events, "pending")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin_uow, events, "begin"),
        _Context(persist_uow, events, "persist"),
    ]
    backend.create_unit_of_work.side_effect = [
        _Context(generic_uow, events, "generic"),
        _Context(pending_uow, events, "pending"),
    ]
    orchestrator = _orchestrator(backend, gather_phase)
    orchestrator._refresh_preview_organization_authority = MagicMock(
        side_effect=lambda: events.append("organization:refresh")
    )

    result = orchestrator.run()

    assert result.errors == []
    assert events.index("begin:commit") < events.index("provider:gather")
    assert events.index("provider:gather") < events.index("generic:commit")
    assert events.index("generic:exit") < events.index("capture:persist")
    assert events.index("persist:commit") < events.index("organization:refresh")
    gather_phase.accept_refresh.assert_called_once_with(_plan())
    capture.persist.assert_called_once()
    persist_call = capture.persist.call_args
    assert persist_call.args == (persist_uow.source_windows, persist_uow.source_readiness)
    assert persist_call.kwargs["attempt_sequence"] == 11
    assert datetime.now(UTC) - persist_call.kwargs["captured_at"] < timedelta(seconds=2)


@pytest.mark.parametrize(
    ("commit_error", "reason"),
    [
        (None, SourceAttemptFailureReason.GENERIC_GATHER_FAILED),
        (RuntimeError("generic commit failed"), SourceAttemptFailureReason.GENERIC_COMMIT_FAILED),
    ],
)
def test_primary_pending_attempt_is_aborted_on_generic_failure(
    commit_error: Exception | None,
    reason: SourceAttemptFailureReason,
) -> None:
    events: list[str] = []
    gather_phase = MagicMock()
    gather_phase.plan_refresh.return_value = _plan()
    if commit_error is None:
        gather_phase.run.side_effect = RuntimeError("generic gather failed")
    else:
        gather_phase.run.return_value = GatherResult(dates_gathered=1, errors=[])
    backend = _backend()
    begin_uow = _uow(events, "begin")
    begin_uow.source_readiness.begin_attempt.return_value = _pending()
    generic_uow = _uow(events, "generic", commit_error=commit_error)
    abort_uow = _uow(events, "abort")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin_uow, events, "begin"),
        _Context(abort_uow, events, "abort"),
    ]
    backend.create_unit_of_work.return_value = _Context(generic_uow, events, "generic")
    orchestrator = _orchestrator(backend, gather_phase)

    result = orchestrator.run()

    assert result.dates_gathered == 0
    assert len(result.errors) == 1
    abort_uow.source_readiness.finalize_attempt.assert_called_once()
    args = abort_uow.source_readiness.finalize_attempt.call_args
    assert args.args == (11, SourceAttemptFinalStatus.ABORTED)
    assert args.kwargs["reason"] is reason
    abort_uow.commit.assert_called_once_with()


def test_source_persistence_and_failure_marker_double_failure_preserves_generic_success() -> None:
    events: list[str] = []
    capture = MagicMock(spec=NativeSourceEvidenceCapture)
    capture.persist.side_effect = RuntimeError("source persistence failed")
    gather_result = GatherResult(
        dates_gathered=0,
        errors=[],
        source_disposition=SourceGatherDisposition.ATTEMPTED,
        source_refresh_token="refresh-1",
        source_attempt_sequence=11,
        source_capture=capture,
        source_failure=None,
    )
    gather_phase = MagicMock()
    gather_phase.plan_refresh.return_value = _plan()
    gather_phase.run.return_value = gather_result
    backend = _backend()
    begin_uow = _uow(events, "begin")
    begin_uow.source_readiness.begin_attempt.return_value = _pending()
    generic_uow = _uow(events, "generic")
    persist_uow = _uow(events, "persist")
    marker_uow = _uow(events, "marker", commit_error=RuntimeError("marker commit failed"))
    pending_uow = _uow(events, "pending")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin_uow, events, "begin"),
        _Context(persist_uow, events, "persist"),
        _Context(marker_uow, events, "marker"),
    ]
    backend.create_unit_of_work.side_effect = [
        _Context(generic_uow, events, "generic"),
        _Context(pending_uow, events, "pending"),
    ]
    orchestrator = _orchestrator(backend, gather_phase)
    orchestrator._refresh_preview_organization_authority = MagicMock()

    result = orchestrator.run()

    assert result.errors == []
    generic_uow.commit.assert_called_once_with()
    marker_uow.source_readiness.finalize_attempt.assert_called_once()
    args = marker_uow.source_readiness.finalize_attempt.call_args
    assert args.args == (11, SourceAttemptFinalStatus.FAILED)
    assert args.kwargs["reason"] is SourceAttemptFailureReason.PERSISTENCE_FAILED


def test_ambiguous_begin_commit_returns_fallback_identity_and_abort_recovers_pending_attempt() -> None:
    events: list[str] = []
    backend = _backend()
    begin_uow = _uow(events, "begin", commit_error=RuntimeError("ambiguous commit"))
    begin_uow.source_readiness.begin_attempt.return_value = _pending()
    abort_uow = _uow(events, "abort")
    abort_uow.source_readiness.get_by_token.return_value = _pending()
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin_uow, events, "begin"),
        _Context(abort_uow, events, "abort"),
    ]
    orchestrator = _orchestrator(backend, MagicMock())

    source_state = orchestrator._prepare_preview_source_state(_plan())
    assert isinstance(source_state, SourceAttemptBeginFailure)
    assert source_state.refresh_start == START
    assert source_state.refresh_end == NOW

    orchestrator._abort_preview_source_state(
        source_state,
        reason=SourceAttemptFailureReason.GENERIC_COMMIT_FAILED,
    )

    abort_uow.source_readiness.get_by_token.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        source_state.refresh_token,
    )
    abort_uow.source_readiness.finalize_attempt.assert_called_once_with(
        11,
        SourceAttemptFinalStatus.ABORTED,
        completed_at=abort_uow.source_readiness.finalize_attempt.call_args.kwargs["completed_at"],
        reason=SourceAttemptFailureReason.GENERIC_COMMIT_FAILED,
    )
    abort_uow.commit.assert_called_once_with()


def test_full_run_begin_commit_failure_gathers_generically_and_journals_fallback_atomically() -> None:
    events: list[str] = []

    class GenericCostInput:
        def gather(self, tenant_id: str, start: datetime, end: datetime, uow: object) -> tuple[()]:
            del tenant_id, start, end, uow
            events.append("provider:gather")
            return ()

    class Plugin:
        def get_cost_input(self) -> GenericCostInput:
            return GenericCostInput()

    billing_phase = object.__new__(GatherPhase)
    billing_phase._bundle = SimpleNamespace(plugin=Plugin())
    billing_phase._tenant_id = "tenant-1"
    gather_phase = MagicMock()
    gather_phase.plan_refresh.return_value = _plan()

    terminal = PreviewSourceAttempt(
        attempt_sequence=11,
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_token="refresh-1",
        refresh_start=START,
        refresh_end=NOW,
        status=SourceAttemptStatus.FAILED,
        started_at=NOW,
        completed_at=NOW,
        failure_reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
    )

    def gather(uow: MagicMock, *, plan: GatherPlan, source_attempt: object) -> GatherResult:
        billing = billing_phase._gather_billing(uow, plan, source_attempt=source_attempt)  # type: ignore[arg-type]
        return GatherResult(
            dates_gathered=len(billing.dates),
            errors=[],
            source_disposition=billing.source_disposition,
            source_refresh_token=billing.source_refresh_token,
            source_attempt_sequence=billing.source_attempt_sequence,
            source_capture=billing.source_capture,
            source_failure=billing.source_failure,
        )

    gather_phase.run.side_effect = gather
    backend = _backend()
    begin_uow = _uow(events, "begin", commit_error=RuntimeError("ambiguous begin commit"))
    begin_uow.source_readiness.begin_attempt.return_value = _pending()
    generic_spies = _uow(events, "generic")
    generic_uow = SimpleNamespace(
        billing=generic_spies.billing,
        source_attempt_fallback=generic_spies.source_attempt_fallback,
        commit=generic_spies.commit,
    )
    generic_uow.source_attempt_fallback.ensure_begin_failed.side_effect = lambda *args, **kwargs: (
        events.append("fallback:journal") or terminal
    )
    pending_uow = _uow(events, "pending")
    backend.create_preview_evidence_unit_of_work.side_effect = [
        _Context(begin_uow, events, "begin"),
    ]
    backend.create_unit_of_work.side_effect = [
        _Context(generic_uow, events, "generic"),
        _Context(pending_uow, events, "pending"),
    ]
    orchestrator = _orchestrator(backend, gather_phase)
    orchestrator._refresh_preview_organization_authority = MagicMock()

    result = orchestrator.run()

    assert result.errors == []
    assert events.index("provider:gather") < events.index("fallback:journal")
    assert events.index("fallback:journal") < events.index("generic:commit")
    generic_uow.source_attempt_fallback.ensure_begin_failed.assert_called_once()
    fallback_call = generic_uow.source_attempt_fallback.ensure_begin_failed.call_args
    assert isinstance(fallback_call.args[0], SourceAttemptBeginFailure)
    assert fallback_call.kwargs["completed_at"].tzinfo is not None
    assert terminal.status is SourceAttemptStatus.FAILED
    assert terminal.failure_reason is SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED
    assert backend.create_preview_evidence_unit_of_work.call_count == 1


def test_unavailable_evidence_storage_returns_immutable_disposition_without_opening_uow() -> None:
    backend = _backend()
    backend.preview_evidence_availability = PreviewEvidenceAvailability(
        state=PreviewEvidenceAvailabilityState.UNAVAILABLE,
    )
    orchestrator = _orchestrator(backend, MagicMock())

    source_state = orchestrator._prepare_preview_source_state(_plan())

    assert isinstance(source_state, SourceEvidenceStorageUnavailable)
    assert source_state.refresh_start == START
    assert source_state.refresh_end == NOW
    backend.create_preview_evidence_unit_of_work.assert_not_called()


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        (SourceCaptureFailure.CONSTRUCTION_FAILED, SourceAttemptFailureReason.CONSTRUCTION_FAILED),
        (SourceCaptureFailure.CAPABILITY_UNAVAILABLE, SourceAttemptFailureReason.CAPABILITY_UNAVAILABLE),
    ],
)
def test_capture_construction_and_capability_failures_finalize_exact_reason(
    failure: SourceCaptureFailure,
    reason: SourceAttemptFailureReason,
) -> None:
    backend = _backend()
    orchestrator = _orchestrator(backend, MagicMock())
    orchestrator._finalize_preview_source_failure = MagicMock()
    result = GatherResult(
        dates_gathered=0,
        errors=[],
        source_disposition=SourceGatherDisposition.ATTEMPTED,
        source_refresh_token="refresh-1",
        source_attempt_sequence=11,
        source_capture=None,
        source_failure=failure,
    )

    orchestrator._persist_preview_source_capture(result)

    orchestrator._finalize_preview_source_failure.assert_called_once_with(
        11,
        status=SourceAttemptFinalStatus.FAILED,
        reason=reason,
    )


def test_abort_finalization_failure_is_isolated_from_generic_gather_result() -> None:
    events: list[str] = []
    backend = _backend()
    abort_uow = _uow(events, "abort")
    abort_uow.source_readiness.finalize_attempt.side_effect = RuntimeError("finalize unavailable")
    backend.create_preview_evidence_unit_of_work.return_value = _Context(abort_uow, events, "abort")
    orchestrator = _orchestrator(backend, MagicMock())

    orchestrator._abort_preview_source_state(
        _pending(),
        reason=SourceAttemptFailureReason.GENERIC_GATHER_FAILED,
    )

    abort_uow.commit.assert_not_called()


def test_native_capture_writer_readiness_mismatch_is_rejected_before_attempt_completion() -> None:
    from core.preview.evidence_capture import (
        NativeSourceWindow,
        SourceWindowCount,
        SourceWindowWriteResult,
    )
    from plugins.confluent_cloud.source_capture import (
        CCloudNativeSourceEvidenceCapture,
        PreviewSourceCapturePersistenceError,
    )

    capture = CCloudNativeSourceEvidenceCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_start=START,
        refresh_end=NOW,
        windows=(NativeSourceWindow(START, NOW),),
        records=(),
    )
    source_windows = MagicMock()
    source_windows.replace_capture.return_value = SourceWindowWriteResult(
        records_written=1,
        window_counts=(
            SourceWindowCount(
                window=NativeSourceWindow(START, NOW),
                source_count=1,
            ),
        ),
    )
    source_readiness = MagicMock()
    source_readiness.replace_overlapping.return_value = ()

    with pytest.raises(PreviewSourceCapturePersistenceError, match="does not match"):
        capture.persist(
            source_windows,
            source_readiness,
            attempt_sequence=11,
            captured_at=NOW,
        )

    source_readiness.finalize_attempt.assert_not_called()


def test_native_capture_writer_compares_and_returns_canonical_second_plan() -> None:
    from core.preview.evidence_capture import (
        NativeSourceWindow,
        SourceWindowWriteResult,
    )
    from plugins.confluent_cloud.source_capture import (
        CCloudNativeSourceEvidenceCapture,
    )

    refresh_start = START.replace(microsecond=111_111)
    refresh_end = NOW.replace(microsecond=222_222)
    captured_at = NOW.replace(microsecond=333_333)
    capture = CCloudNativeSourceEvidenceCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_start=refresh_start,
        refresh_end=refresh_end,
        windows=(NativeSourceWindow(refresh_start, refresh_end),),
        records=(),
    )
    source_windows = MagicMock()
    source_windows.replace_capture.return_value = SourceWindowWriteResult(
        records_written=0,
    )
    source_readiness = MagicMock()
    source_readiness.replace_overlapping.side_effect = lambda _ecosystem, _tenant_id, _start, _end, captures: tuple(
        captures
    )

    receipt = capture.write(
        source_windows,
        source_readiness,
        attempt_sequence=11,
        captured_at=captured_at,
    )

    assert receipt.refresh_start == START
    assert receipt.refresh_end == NOW
    assert receipt.captures[0].window_start == START
    assert receipt.captures[0].window_end == NOW
    assert receipt.captures[0].captured_at == NOW
    readiness_call = source_readiness.replace_overlapping.call_args.args
    assert readiness_call[2:] == (
        START,
        NOW,
        receipt.captures,
    )


def test_generic_billing_gather_persists_each_line_before_requesting_the_next() -> None:
    events: list[str] = []
    uow = MagicMock()

    def billing_lines():
        events.append("yield:first")
        yield CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            timestamp=START,
            resource_id="resource-1",
            product_category="kafka",
            product_type="KAFKA_CKU",
            quantity=1,
            unit_price=1,
            total_cost=1,
            granularity="daily",
        )
        assert uow.billing.upsert.call_count == 1
        events.append("yield:second")
        yield CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            timestamp=START + timedelta(hours=1),
            resource_id="resource-2",
            product_category="kafka",
            product_type="KAFKA_CKU",
            quantity=1,
            unit_price=1,
            total_cost=1,
            granularity="daily",
        )

    cost_input = MagicMock()
    cost_input.gather.side_effect = lambda *args: billing_lines()
    plugin = MagicMock()
    plugin.get_cost_input.return_value = cost_input
    phase = object.__new__(GatherPhase)
    phase._bundle = SimpleNamespace(plugin=plugin)
    phase._tenant_id = "tenant-1"

    result = phase._gather_billing(uow, _plan(), source_attempt=None)

    assert result.dates == frozenset({START.date()})
    assert events == ["yield:first", "yield:second"]
    assert uow.billing.upsert.call_count == 2
