from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future
from datetime import date
from typing import Any

import pytest

from tests.unit.core.preview.conftest import preview_module


class ControlledExecutor:
    """Complete executor double with caller-controlled task completion."""

    def __init__(self) -> None:
        self.pending: list[tuple[Callable[[], None], Future[None]]] = []
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def submit(self, task: Callable[[], None]) -> Future[None]:
        future: Future[None] = Future()
        self.pending.append((task, future))
        return future

    def run_next(self) -> None:
        task, future = self.pending.pop(0)
        if future.set_running_or_notify_cancel():
            try:
                task()
            except BaseException as exc:
                future.set_exception(exc)
                raise
            else:
                future.set_result(None)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


class RejectingExecutor:
    def submit(self, task: Callable[[], None]) -> Future[None]:
        del task
        raise RuntimeError("executor rejected submission")

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures


class RejectAfterFirstExecutor(ControlledExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.submit_count = 0

    def submit(self, task: Callable[[], None]) -> Future[None]:
        self.submit_count += 1
        if self.submit_count > 1:
            raise RuntimeError("executor rejected later submission")
        return super().submit(task)


def _owner(
    tenant_id: str,
    *,
    tenant_name: str | None = None,
    fingerprint: str | None = None,
) -> Any:
    artifacts = preview_module("artifacts")
    return artifacts.PreviewArtifactOwner(
        tenant_name or tenant_id,
        "confluent_cloud",
        tenant_id,
        fingerprint or (tenant_id[-1] * 64),
    )


def _scheduler(
    executor: object,
    *,
    workers: int = 1,
    queued: int = 4,
    tenant_running: int = 1,
    tenant_queued: int = 2,
) -> Any:
    capacity = preview_module("capacity")
    return capacity.PreviewGenerationScheduler(
        max_workers=workers,
        max_queued_generations=queued,
        max_running_generations_per_tenant=tenant_running,
        max_queued_generations_per_tenant=tenant_queued,
        executor=executor,
    )


def _attach_requested(scheduler: Any, owner: Any, work_id: str, run: Callable[[], None]) -> Any:
    reservation = scheduler.reserve_requested(owner=owner)
    reservation.attach(work_id=work_id, run=run)
    return reservation


def test_scheduler_submits_only_promoted_work_and_never_fills_executor_private_queue() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor, workers=2, queued=8, tenant_running=1, tenant_queued=2)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")

    _attach_requested(scheduler, owner_a, "a-1", lambda: None)
    _attach_requested(scheduler, owner_a, "a-2", lambda: None)
    _attach_requested(scheduler, owner_b, "b-1", lambda: None)
    _attach_requested(scheduler, owner_b, "b-2", lambda: None)

    snapshot = scheduler.snapshot()
    assert len(executor.pending) == 2
    assert snapshot.global_running == 2
    assert snapshot.global_queued == 2
    assert snapshot.owner_running(owner_a) == 1
    assert snapshot.owner_running(owner_b) == 1
    assert snapshot.owner_queued(owner_a) == 1
    assert snapshot.owner_queued(owner_b) == 1


def test_scheduler_preserves_owner_fifo_and_round_robins_across_owners() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")
    ran: list[str] = []

    _attach_requested(scheduler, owner_a, "a-1", lambda: ran.append("a-1"))
    _attach_requested(scheduler, owner_a, "a-2", lambda: ran.append("a-2"))
    _attach_requested(scheduler, owner_b, "b-1", lambda: ran.append("b-1"))

    executor.run_next()
    assert ran == ["a-1"]
    executor.run_next()
    assert ran == ["a-1", "b-1"]
    executor.run_next()
    assert ran == ["a-1", "b-1", "a-2"]
    assert scheduler.snapshot().global_running == 0
    assert scheduler.snapshot().global_queued == 0


def test_scheduler_enforces_per_owner_pending_capacity_before_global_capacity() -> None:
    capacity = preview_module("capacity")
    executor = ControlledExecutor()
    scheduler = _scheduler(executor, queued=6, tenant_queued=1)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")

    _attach_requested(scheduler, owner_a, "a-1", lambda: None)
    _attach_requested(scheduler, owner_a, "a-2", lambda: None)
    with pytest.raises(capacity.PreviewCapacityUnavailable):
        scheduler.reserve_requested(owner=owner_a)

    _attach_requested(scheduler, owner_b, "b-1", lambda: None)
    assert scheduler.snapshot().global_queued == 2


def test_storage_owner_identity_not_mutable_tenant_label_controls_capacity() -> None:
    capacity = preview_module("capacity")
    executor = ControlledExecutor()
    scheduler = _scheduler(executor, queued=4, tenant_queued=1)
    first_label = _owner("tenant-a", tenant_name="old-label")
    renamed = _owner("tenant-a", tenant_name="new-label")

    _attach_requested(scheduler, first_label, "a-1", lambda: None)
    _attach_requested(scheduler, renamed, "a-2", lambda: None)
    with pytest.raises(capacity.PreviewCapacityUnavailable):
        scheduler.reserve_requested(owner=renamed)


def test_zero_queue_mode_starts_immediately_or_rejects_without_waiting() -> None:
    capacity = preview_module("capacity")
    executor = ControlledExecutor()
    scheduler = _scheduler(executor, queued=0, tenant_queued=0)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")

    _attach_requested(scheduler, owner_a, "a-1", lambda: None)
    with pytest.raises(capacity.PreviewCapacityUnavailable):
        scheduler.reserve_requested(owner=owner_b)
    assert scheduler.admit_scheduled(owner=owner_b, month=date(2026, 7, 1), run=lambda: None) is False
    assert scheduler.snapshot().global_queued == 0

    executor.run_next()
    assert scheduler.admit_scheduled(owner=owner_b, month=date(2026, 7, 1), run=lambda: None) is True
    assert len(executor.pending) == 1


def test_admitted_scheduled_month_cannot_be_starved_by_continuous_requested_arrivals() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor, queued=8, tenant_queued=3)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")
    ran: list[str] = []

    _attach_requested(scheduler, owner_a, "a-1", lambda: ran.append("a-1"))
    assert (
        scheduler.admit_scheduled(
            owner=owner_b,
            month=date(2026, 7, 1),
            run=lambda: ran.append("b-scheduled"),
        )
        is True
    )
    _attach_requested(scheduler, owner_a, "a-2", lambda: ran.append("a-2"))
    _attach_requested(scheduler, owner_a, "a-3", lambda: ran.append("a-3"))

    executor.run_next()
    executor.run_next()
    assert ran == ["a-1", "b-scheduled"]


def test_scheduled_month_deduplicates_while_admitted_and_can_be_readmitted_after_completion() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor)
    owner = _owner("tenant-a")
    month = date(2026, 7, 1)

    assert scheduler.admit_scheduled(owner=owner, month=month, run=lambda: None) is True
    assert scheduler.admit_scheduled(owner=owner, month=month, run=lambda: None) is False
    assert scheduler.snapshot().global_running == 1

    executor.run_next()
    assert scheduler.admit_scheduled(owner=owner, month=month, run=lambda: None) is True


def test_submission_failure_and_idempotent_cancel_release_reservations() -> None:
    service = preview_module("service")
    scheduler = _scheduler(RejectingExecutor())
    owner = _owner("tenant-a")
    reservation = scheduler.reserve_requested(owner=owner)

    with pytest.raises(service.PreviewWorkerUnavailable):
        reservation.attach(work_id="request-1", run=lambda: None)
    reservation.cancel()
    reservation.cancel()

    snapshot = scheduler.snapshot()
    assert snapshot.global_running == 0
    assert snapshot.global_queued == 0
    assert snapshot.owner_running(owner) == 0
    assert snapshot.owner_queued(owner) == 0


def test_worker_failure_releases_running_slot_and_promotes_next_waiter() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")
    ran: list[str] = []

    def fail() -> None:
        ran.append("a-failed")
        raise RuntimeError("synthetic worker failure")

    _attach_requested(scheduler, owner_a, "a-1", fail)
    _attach_requested(scheduler, owner_b, "b-1", lambda: ran.append("b-1"))

    with pytest.raises(RuntimeError, match="synthetic worker failure"):
        executor.run_next()
    assert scheduler.snapshot().global_running == 1
    assert scheduler.snapshot().global_queued == 0
    executor.run_next()
    assert ran == ["a-failed", "b-1"]
    assert scheduler.snapshot().global_running == 0


def test_later_executor_rejection_notifies_persisted_waiter_and_releases_capacity() -> None:
    executor = RejectAfterFirstExecutor()
    scheduler = _scheduler(executor)
    owner = _owner("tenant-a")
    cancelled: list[str] = []
    _attach_requested(scheduler, owner, "first", lambda: None)
    waiting = scheduler.reserve_requested(owner=owner)
    waiting.attach(
        work_id="second",
        run=lambda: None,
        on_cancel=lambda: cancelled.append("second"),
    )

    executor.run_next()

    assert cancelled == ["second"]
    assert scheduler.snapshot().global_running == 0
    assert scheduler.snapshot().global_queued == 0


def test_new_scheduler_starts_with_zero_process_local_reservations_after_restart() -> None:
    first_executor = ControlledExecutor()
    first = _scheduler(first_executor)
    _attach_requested(first, _owner("tenant-a"), "a-1", lambda: None)
    assert first.snapshot().global_running == 1

    restarted = _scheduler(ControlledExecutor())

    assert restarted.snapshot().global_running == 0
    assert restarted.snapshot().global_queued == 0


def test_close_without_wait_drops_waiting_work_and_running_completion_releases_once() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor)
    owner_a = _owner("tenant-a")
    owner_b = _owner("tenant-b")
    ran: list[str] = []

    _attach_requested(scheduler, owner_a, "a-1", lambda: ran.append("a-1"))
    waiting = _attach_requested(scheduler, owner_a, "a-2", lambda: ran.append("a-2"))
    assert scheduler.admit_scheduled(owner=owner_b, month=date(2026, 7, 1), run=lambda: ran.append("b")) is True

    scheduler.close(wait=False)
    waiting.cancel()
    assert scheduler.snapshot().global_queued == 0

    executor.run_next()
    assert ran == ["a-1"]
    assert scheduler.snapshot().global_running == 0
    assert executor.shutdown_calls == [(False, False)]


def test_close_with_wait_drains_waiting_work_before_owned_executor_shutdown() -> None:
    capacity = preview_module("capacity")
    owner = _owner("tenant-a")
    first_started = threading.Event()
    release_first = threading.Event()
    closed = threading.Event()
    ran: list[str] = []
    scheduler = capacity.PreviewGenerationScheduler(
        max_workers=1,
        max_queued_generations=4,
        max_running_generations_per_tenant=1,
        max_queued_generations_per_tenant=2,
    )

    def first() -> None:
        first_started.set()
        assert release_first.wait(timeout=5)
        ran.append("first")

    _attach_requested(scheduler, owner, "first", first)
    _attach_requested(scheduler, owner, "second", lambda: ran.append("second"))
    closer = threading.Thread(target=lambda: (scheduler.close(wait=True), closed.set()))
    try:
        assert first_started.wait(timeout=5)
        closer.start()
        assert not closed.wait(timeout=0.1)
        release_first.set()
        closer.join(timeout=5)
        assert not closer.is_alive()
        assert ran == ["first", "second"]
        assert scheduler.snapshot().global_running == 0
        assert scheduler.snapshot().global_queued == 0
    finally:
        release_first.set()
        closer.join(timeout=5)
        scheduler.close(wait=False)


def test_close_without_wait_notifies_only_dropped_requested_work() -> None:
    executor = ControlledExecutor()
    scheduler = _scheduler(executor)
    owner = _owner("tenant-a")
    cancelled: list[str] = []
    first = scheduler.reserve_requested(owner=owner)
    first.attach(work_id="first", run=lambda: None, on_cancel=lambda: cancelled.append("first"))
    second = scheduler.reserve_requested(owner=owner)
    second.attach(work_id="second", run=lambda: None, on_cancel=lambda: cancelled.append("second"))

    scheduler.close(wait=False)

    assert cancelled == ["second"]
    executor.run_next()
    assert cancelled == ["second"]
