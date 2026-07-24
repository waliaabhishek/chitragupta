from __future__ import annotations

import logging
import threading
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from enum import Enum, auto
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from core.preview.artifacts import PreviewArtifactOwner

logger = logging.getLogger(__name__)

OwnerKey = tuple[str, str, str]
ScheduledKey = tuple[OwnerKey, date]


class PreviewCapacityUnavailable(RuntimeError):  # noqa: N818 - stable design/API name
    """The bounded Preview generation scheduler cannot admit more work."""


class PreviewExecutor(Protocol):
    def submit(self, task: Callable[[], None]) -> Future[None]: ...

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None: ...


@dataclass(frozen=True)
class PreviewCapacityLimits:
    max_workers: int
    max_queued_generations: int
    max_running_generations_per_tenant: int
    max_queued_generations_per_tenant: int

    def __post_init__(self) -> None:
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if self.max_running_generations_per_tenant <= 0:
            raise ValueError("max_running_generations_per_tenant must be positive")
        if self.max_running_generations_per_tenant > self.max_workers:
            raise ValueError("per-tenant running capacity must not exceed global running capacity")
        global_queued = self.max_queued_generations
        owner_queued = self.max_queued_generations_per_tenant
        if global_queued < 0 or owner_queued < 0:
            raise ValueError("queued capacities must not be negative")
        if (global_queued == 0) != (owner_queued == 0):
            raise ValueError("global and per-tenant queued capacities must both be zero or both be positive")
        if global_queued > 0 and owner_queued >= global_queued:
            raise ValueError("per-tenant queued capacity must be lower than global queued capacity")


@dataclass(frozen=True)
class PreviewCapacitySnapshot:
    global_running: int
    global_queued: int
    _running_by_owner: tuple[tuple[OwnerKey, int], ...]
    _queued_by_owner: tuple[tuple[OwnerKey, int], ...]

    def owner_running(self, owner: PreviewArtifactOwner) -> int:
        return dict(self._running_by_owner).get(_owner_key(owner), 0)

    def owner_queued(self, owner: PreviewArtifactOwner) -> int:
        return dict(self._queued_by_owner).get(_owner_key(owner), 0)


class _WorkState(Enum):
    QUEUED = auto()
    RUNNING = auto()
    RELEASED = auto()


@dataclass(eq=False)
class _WorkItem:
    owner: PreviewArtifactOwner
    owner_key: OwnerKey
    scheduled_key: ScheduledKey | None
    state: _WorkState
    work_id: str | None = None
    run: Callable[[], None] | None = None
    on_cancel: Callable[[], None] | None = None
    submitted: bool = False
    submission_failed: bool = False


def _owner_key(owner: PreviewArtifactOwner) -> OwnerKey:
    return (owner.ecosystem, owner.tenant_id, owner.storage_backend_fingerprint)


class PreviewRequestedReservation:
    """An admitted requested-generation slot that can be attached after persistence."""

    def __init__(self, scheduler: PreviewGenerationScheduler, item: _WorkItem) -> None:
        self._scheduler = scheduler
        self._item = item

    def attach(
        self,
        *,
        work_id: str,
        run: Callable[[], None],
        on_cancel: Callable[[], None] | None = None,
    ) -> None:
        if not work_id:
            raise ValueError("work_id must not be blank")
        try:
            self._scheduler._attach_requested(
                self._item,
                work_id=work_id,
                run=run,
                on_cancel=on_cancel,
            )
        except _ExecutorSubmissionError as exc:
            # Import lazily so service can depend on this module without a module-load cycle.
            from core.preview.service import PreviewWorkerUnavailable

            raise PreviewWorkerUnavailable("preview worker rejected generation") from exc

    def cancel(self) -> None:
        self._scheduler._cancel(self._item)


class _ExecutorSubmissionError(RuntimeError):
    pass


class PreviewGenerationScheduler:
    """Process-local bounded owner-fair scheduler for requested and scheduled work."""

    def __init__(
        self,
        *,
        max_workers: int,
        max_queued_generations: int,
        max_running_generations_per_tenant: int,
        max_queued_generations_per_tenant: int,
        executor: PreviewExecutor | None = None,
        shutdown_executor: bool = True,
    ) -> None:
        self._limits = PreviewCapacityLimits(
            max_workers=max_workers,
            max_queued_generations=max_queued_generations,
            max_running_generations_per_tenant=max_running_generations_per_tenant,
            max_queued_generations_per_tenant=max_queued_generations_per_tenant,
        )
        self._executor: PreviewExecutor = executor or ThreadPoolExecutor(max_workers=max_workers)
        self._shutdown_executor = shutdown_executor
        self._lock = threading.RLock()
        self._idle = threading.Condition(self._lock)
        self._queues: dict[OwnerKey, deque[_WorkItem]] = {}
        self._running_by_owner: dict[OwnerKey, int] = {}
        self._owner_ring: list[OwnerKey] = []
        self._last_started_owner: OwnerKey | None = None
        self._scheduled_keys: set[ScheduledKey] = set()
        self._global_running = 0
        self._global_queued = 0
        self._closed = False
        self._drain_queued = False
        self._lifecycle_callbacks: deque[Callable[[], None]] = deque()

    def reserve_requested(self, *, owner: PreviewArtifactOwner) -> PreviewRequestedReservation:
        with self._lock:
            item = self._admit_locked(owner=owner, scheduled_key=None, run=None)
        self._run_lifecycle_callbacks()
        if item is None:
            raise PreviewCapacityUnavailable
        return PreviewRequestedReservation(self, item)

    def admit_scheduled(
        self,
        *,
        owner: PreviewArtifactOwner,
        month: date,
        run: Callable[[], None],
        on_cancel: Callable[[], None] | None = None,
    ) -> bool:
        key = (_owner_key(owner), month)
        try:
            with self._lock:
                if key in self._scheduled_keys:
                    return False
                item = self._admit_locked(owner=owner, scheduled_key=key, run=run)
                if item is None:
                    return False
                item.on_cancel = on_cancel
                self._scheduled_keys.add(key)
                if item.state is _WorkState.RUNNING:
                    try:
                        self._submit_locked(item)
                    except _ExecutorSubmissionError:
                        self._promote_locked()
                        return False
                else:
                    self._promote_locked()
                return not item.submission_failed
        finally:
            self._run_lifecycle_callbacks()

    def snapshot(self) -> PreviewCapacitySnapshot:
        with self._lock:
            queued_by_owner = tuple((owner, len(queue)) for owner, queue in self._queues.items() if queue)
            running_by_owner = tuple((owner, count) for owner, count in self._running_by_owner.items() if count)
            return PreviewCapacitySnapshot(
                global_running=self._global_running,
                global_queued=self._global_queued,
                _running_by_owner=running_by_owner,
                _queued_by_owner=queued_by_owner,
            )

    def close(self, wait: bool = True) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._drain_queued = wait
            if wait:
                self._promote_locked()
            else:
                for queue in tuple(self._queues.values()):
                    for item in tuple(queue):
                        self._release_locked(item, notify_cancel=True)
                for owner in tuple(self._owner_ring):
                    self._remove_owner_if_idle_locked(owner)
        self._run_lifecycle_callbacks()
        if wait:
            self.wait_idle()
        if self._shutdown_executor:
            self._executor.shutdown(wait=wait, cancel_futures=False)

    def wait_idle(self) -> None:
        with self._idle:
            while self._global_running or self._global_queued:
                self._idle.wait()

    def _admit_locked(
        self,
        *,
        owner: PreviewArtifactOwner,
        scheduled_key: ScheduledKey | None,
        run: Callable[[], None] | None,
    ) -> _WorkItem | None:
        if self._closed:
            return None
        owner_key = _owner_key(owner)
        self._ensure_owner_locked(owner_key)
        self._promote_locked()

        no_waiting = self._limits.max_queued_generations == 0
        can_start = self._can_start_locked(owner_key) and self._global_queued == 0
        if can_start:
            item = _WorkItem(
                owner=owner,
                owner_key=owner_key,
                scheduled_key=scheduled_key,
                state=_WorkState.RUNNING,
                run=run,
            )
            self._mark_running_locked(item)
            return item
        if no_waiting:
            self._remove_owner_if_idle_locked(owner_key)
            return None

        queue = self._queues.setdefault(owner_key, deque())
        if (
            self._global_queued >= self._limits.max_queued_generations
            or len(queue) >= self._limits.max_queued_generations_per_tenant
        ):
            self._remove_owner_if_idle_locked(owner_key)
            return None
        item = _WorkItem(
            owner=owner,
            owner_key=owner_key,
            scheduled_key=scheduled_key,
            state=_WorkState.QUEUED,
            run=run,
        )
        queue.append(item)
        self._global_queued += 1
        return item

    def _attach_requested(
        self,
        item: _WorkItem,
        *,
        work_id: str,
        run: Callable[[], None],
        on_cancel: Callable[[], None] | None,
    ) -> None:
        try:
            with self._lock:
                if item.state is _WorkState.RELEASED:
                    raise _ExecutorSubmissionError("reservation is no longer active")
                if item.run is not None:
                    raise RuntimeError("requested reservation is already attached")
                item.work_id = work_id
                item.run = run
                item.on_cancel = on_cancel
                if item.state is _WorkState.RUNNING:
                    try:
                        self._submit_locked(item, notify_failure=False)
                    except _ExecutorSubmissionError:
                        self._promote_locked()
                        raise
                else:
                    self._promote_locked()
                if item.submission_failed:
                    raise _ExecutorSubmissionError("executor rejected submitted work")
        finally:
            self._run_lifecycle_callbacks()

    def _cancel(self, item: _WorkItem) -> None:
        with self._lock:
            if item.state is _WorkState.RELEASED:
                return
            if item.state is _WorkState.RUNNING and item.submitted:
                return
            self._release_locked(item)
            self._promote_locked()
        self._run_lifecycle_callbacks()

    def _can_start_locked(self, owner: OwnerKey) -> bool:
        return (
            self._global_running < self._limits.max_workers
            and self._running_by_owner.get(owner, 0) < self._limits.max_running_generations_per_tenant
        )

    def _mark_running_locked(self, item: _WorkItem) -> None:
        item.state = _WorkState.RUNNING
        self._global_running += 1
        self._running_by_owner[item.owner_key] = self._running_by_owner.get(item.owner_key, 0) + 1
        self._last_started_owner = item.owner_key

    def _promote_locked(self) -> None:
        while (not self._closed or self._drain_queued) and self._global_running < self._limits.max_workers:
            owner = self._next_promotable_owner_locked()
            if owner is None:
                return
            queue = self._queues[owner]
            item = queue.popleft()
            self._global_queued -= 1
            self._mark_running_locked(item)
            if item.run is not None:
                try:
                    self._submit_locked(item, notify_failure=True)
                except _ExecutorSubmissionError:
                    continue

    def _next_promotable_owner_locked(self) -> OwnerKey | None:
        if not self._owner_ring:
            return None
        start = 0
        if self._last_started_owner in self._owner_ring:
            start = (self._owner_ring.index(self._last_started_owner) + 1) % len(self._owner_ring)
        for offset in range(len(self._owner_ring)):
            owner = self._owner_ring[(start + offset) % len(self._owner_ring)]
            queue = self._queues.get(owner)
            if queue and queue[0].run is not None and self._can_start_locked(owner):
                return owner
        return None

    def _submit_locked(self, item: _WorkItem, *, notify_failure: bool = True) -> None:
        if item.submitted or item.state is not _WorkState.RUNNING:
            return

        def execute() -> None:
            try:
                run = item.run
                if run is None:
                    raise RuntimeError("preview generation work was submitted before attachment")
                run()
            finally:
                self._complete(item)

        try:
            self._executor.submit(execute)
        except BaseException as exc:
            item.submission_failed = True
            self._release_locked(item, notify_cancel=notify_failure)
            raise _ExecutorSubmissionError("executor rejected submitted work") from exc
        item.submitted = True

    def _complete(self, item: _WorkItem) -> None:
        with self._lock:
            self._release_locked(item)
            self._promote_locked()
        self._run_lifecycle_callbacks()

    def _release_locked(self, item: _WorkItem, *, notify_cancel: bool = False) -> None:
        if item.state is _WorkState.RELEASED:
            return
        if item.state is _WorkState.QUEUED:
            queue = self._queues.get(item.owner_key)
            if queue is not None:
                try:
                    queue.remove(item)
                except ValueError:
                    pass
                else:
                    self._global_queued -= 1
        elif item.state is _WorkState.RUNNING:
            self._global_running -= 1
            running = self._running_by_owner[item.owner_key] - 1
            if running:
                self._running_by_owner[item.owner_key] = running
            else:
                self._running_by_owner.pop(item.owner_key, None)
        item.state = _WorkState.RELEASED
        if item.scheduled_key is not None:
            self._scheduled_keys.discard(item.scheduled_key)
        if notify_cancel and item.on_cancel is not None:
            self._lifecycle_callbacks.append(item.on_cancel)
            item.on_cancel = None
        self._remove_owner_if_idle_locked(item.owner_key)
        if self._global_running == 0 and self._global_queued == 0:
            self._idle.notify_all()

    def _ensure_owner_locked(self, owner: OwnerKey) -> None:
        if owner not in self._owner_ring:
            self._owner_ring.append(owner)

    def _remove_owner_if_idle_locked(self, owner: OwnerKey) -> None:
        if self._running_by_owner.get(owner, 0) or self._queues.get(owner):
            return
        self._queues.pop(owner, None)
        if owner not in self._owner_ring:
            return
        index = self._owner_ring.index(owner)
        if self._last_started_owner == owner:
            if len(self._owner_ring) == 1:
                self._last_started_owner = None
            else:
                self._last_started_owner = self._owner_ring[index - 1]
        self._owner_ring.pop(index)

    def _run_lifecycle_callbacks(self) -> None:
        while True:
            with self._lock:
                if not self._lifecycle_callbacks:
                    return
                callback = self._lifecycle_callbacks.popleft()
            try:
                callback()
            except Exception:
                logger.exception("Preview scheduler lifecycle callback failed")
