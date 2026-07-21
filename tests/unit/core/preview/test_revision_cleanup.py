from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, Self
from unittest.mock import MagicMock

import pytest

from core.config.models import TenantConfig


class _Repository:
    def __init__(self, *, pending: list[Any], due: list[Any]) -> None:
        self.pending = pending
        self.due = due
        self.attempted_defer: list[tuple[str, datetime]] = []
        self.attempted_delete: list[str] = []
        self.claim_limits: list[int] = []
        self.delete_returns: dict[str, bool] = {}
        self.delete_raises: set[str] = set()
        self.defer_raises: set[str] = set()
        self.last_operation: str | None = None
        self.deleted_candidate: Any | None = None
        self.deferred_previous: Any | None = None

    def list_retention_pending(self, *, ecosystem: str, tenant_id: str, limit: int) -> tuple[Any, ...]:
        del ecosystem, tenant_id
        return tuple(sorted(self.pending, key=lambda item: (item.retention_pending_at, item.revision_id))[:limit])

    def mark_retention_due(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        cutoff_date: Any,
        pending_at: datetime,
        limit: int,
    ) -> tuple[Any, ...]:
        del ecosystem, tenant_id, cutoff_date
        self.last_operation = "claim"
        self.claim_limits.append(limit)
        selected, self.due = self.due[:limit], self.due[limit:]
        claimed = [replace(item, retention_pending_at=pending_at) for item in selected]
        self.pending.extend(claimed)
        return tuple(claimed)

    def get_retention_pending_tail(self, *, ecosystem: str, tenant_id: str) -> datetime | None:
        del ecosystem, tenant_id
        return max((item.retention_pending_at for item in self.pending), default=None)

    def defer_retention_pending(self, *, candidate: Any, retry_at: datetime) -> bool:
        self.attempted_defer.append((candidate.revision_id, retry_at))
        self.last_operation = "defer"
        if candidate.revision_id in self.defer_raises:
            raise RuntimeError("synthetic deferral persistence failure")
        for index, item in enumerate(self.pending):
            if item == candidate:
                self.deferred_previous = item
                self.pending[index] = replace(item, retention_pending_at=retry_at)
                return True
        return False

    def delete_retention_pending(self, *, candidate: Any) -> bool:
        self.attempted_delete.append(candidate.revision_id)
        self.last_operation = "delete"
        if candidate.revision_id in self.delete_raises:
            raise RuntimeError("synthetic guarded row deletion failure")
        if not self.delete_returns.get(candidate.revision_id, True):
            return False
        try:
            self.pending.remove(candidate)
        except ValueError:
            return False
        self.deleted_candidate = candidate
        return True


class _UnitOfWork:
    def __init__(self, backend: _Backend) -> None:
        self.backend = backend
        repository = backend.repository
        self.revisions = repository
        self.requests = MagicMock()
        self.commits = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback

    def commit(self) -> None:
        self.commits += 1
        repository = self.backend.repository
        if repository.last_operation == "delete" and self.backend.fail_delete_commit_once:
            self.backend.fail_delete_commit_once = False
            assert repository.deleted_candidate is not None
            repository.pending.append(repository.deleted_candidate)
            repository.deleted_candidate = None
            raise RuntimeError("synthetic final deletion commit failure")
        if repository.last_operation == "defer" and self.backend.fail_defer_commit_once:
            self.backend.fail_defer_commit_once = False
            assert repository.deferred_previous is not None
            revision_id = repository.deferred_previous.revision_id
            repository.pending = [
                repository.deferred_previous if item.revision_id == revision_id else item for item in repository.pending
            ]
            raise RuntimeError("synthetic deferral commit failure")

    def rollback(self) -> None:
        return None


class _Backend:
    def __init__(self, repository: _Repository) -> None:
        self.repository = repository
        self.fail_delete_commit_once = False
        self.fail_defer_commit_once = False

    def create_preview_read_unit_of_work(self) -> _UnitOfWork:
        return _UnitOfWork(self)

    def create_preview_write_unit_of_work(self) -> _UnitOfWork:
        return _UnitOfWork(self)


class _Store:
    def __init__(self, *, failures: set[str] | None = None, absent: set[str] | None = None) -> None:
        self.failures = failures or set()
        self.absent = absent or set()
        self.attempts: list[str] = []

    def delete_package(self, *, storage_key: str) -> bool:
        self.attempts.append(storage_key)
        if storage_key in self.failures:
            raise OSError("synthetic artifact failure")
        return storage_key not in self.absent

    def cleanup_staging(self) -> int:
        return 0


def _candidate(index: int, *, pending_at: datetime) -> Any:
    persistence = import_module("core.preview.persistence")
    return persistence.PreviewRetentionCandidate(
        revision_id=f"revision-{index:03d}",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        storage_key=f"package-{index:03d}",
        retention_pending_at=pending_at,
    )


def _service(store: _Store) -> Any:
    revisions = import_module("core.preview.revisions")
    return revisions.PreviewRevisionService(
        artifact_store=store,
        package_generator=MagicMock(),
    )


def test_cleanup_rejects_naive_cycle_time() -> None:
    repository = _Repository(pending=[], due=[])
    with pytest.raises(ValueError, match="timezone"):
        _service(_Store()).cleanup_retention(
            tenant_name="production",
            tenant_config=TenantConfig(ecosystem="confluent_cloud", tenant_id="tenant-1"),
            backend=_Backend(repository),
            now=datetime(2026, 8, 5),
        )


def test_cleanup_reserves_fifty_attempts_for_retry_and_new_lanes() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    pending = [_candidate(index, pending_at=base + timedelta(microseconds=index)) for index in range(100)]
    due = [_candidate(100 + index, pending_at=base) for index in range(60)]
    repository = _Repository(pending=pending, due=due)
    store = _Store(failures={item.storage_key for item in (*pending, *due)})

    result = _service(store).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            retention_days=30,
        ),
        backend=_Backend(repository),
        now=base,
    )

    assert repository.claim_limits == [50]
    assert len(store.attempts) == 100
    assert len([key for key in store.attempts if key < "package-100"]) == 50
    assert len([key for key in store.attempts if key >= "package-100"]) == 50
    assert result.claimed_count == 50
    assert result.deleted_count == 0
    assert result.deferred_count == 100


def test_cleanup_defers_failures_after_complete_persisted_tail_and_reaches_older_retries_next_cycle() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    pending = [_candidate(index, pending_at=base + timedelta(microseconds=index)) for index in range(101)]
    due = [_candidate(200 + index, pending_at=base) for index in range(60)]
    repository = _Repository(pending=pending, due=due)
    store = _Store(failures={item.storage_key for item in (*pending, *due)})
    service = _service(store)
    tenant = TenantConfig(ecosystem="confluent_cloud", tenant_id="tenant-1", retention_days=30)

    service.cleanup_retention(tenant_name="production", tenant_config=tenant, backend=_Backend(repository), now=base)

    retry_times = {retry_at for _revision_id, retry_at in repository.attempted_defer}
    assert retry_times == {base + timedelta(microseconds=101)}
    assert "package-100" not in store.attempts

    store.attempts.clear()
    service = _service(store)
    service.cleanup_retention(tenant_name="production", tenant_config=tenant, backend=_Backend(repository), now=base)

    assert "package-100" in store.attempts


def test_cleanup_backfills_unused_lane_capacity_and_treats_absent_artifact_as_success() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    pending = [_candidate(index, pending_at=base) for index in range(10)]
    due = [_candidate(100 + index, pending_at=base) for index in range(90)]
    repository = _Repository(pending=pending, due=due)
    store = _Store(absent={item.storage_key for item in pending})

    result = _service(store).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(ecosystem="confluent_cloud", tenant_id="tenant-1", retention_days=30),
        backend=_Backend(repository),
        now=base,
    )

    assert repository.claim_limits == [90]
    assert len(store.attempts) == 100
    assert result.claimed_count == 90
    assert result.deleted_count == 100
    assert result.deferred_count == 0
    assert repository.pending == []


def test_cleanup_retries_backfill_when_newly_due_lane_underfills_capacity() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    pending = [_candidate(index, pending_at=base) for index in range(90)]
    due = [_candidate(100 + index, pending_at=base) for index in range(10)]
    repository = _Repository(pending=pending, due=due)
    store = _Store()

    result = _service(store).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            retention_days=30,
        ),
        backend=_Backend(repository),
        now=base,
    )

    assert repository.claim_limits == [50]
    assert len(store.attempts) == 100
    assert len([key for key in store.attempts if key < "package-100"]) == 90
    assert len([key for key in store.attempts if key >= "package-100"]) == 10
    assert result.claimed_count == 10
    assert result.deleted_count == 100
    assert result.deferred_count == 0


def test_cleanup_sustains_both_lanes_until_original_retry_and_due_sentinels_are_attempted() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    pending = [_candidate(index, pending_at=base + timedelta(microseconds=index)) for index in range(200)]
    due = [_candidate(200 + index, pending_at=base) for index in range(200)]
    repository = _Repository(pending=pending, due=due)
    store = _Store(failures={item.storage_key for item in (*pending, *due)})
    service = _service(store)
    tenant = TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        retention_days=30,
    )
    attempted_by_cycle: list[set[str]] = []

    for _cycle in range(4):
        store.attempts.clear()
        service.cleanup_retention(
            tenant_name="production",
            tenant_config=tenant,
            backend=_Backend(repository),
            now=base,
        )
        attempted = set(store.attempts)
        attempted_by_cycle.append(attempted)
        assert len(attempted) == 100
        assert len([key for key in attempted if key < "package-200"]) == 50
        assert len([key for key in attempted if key >= "package-200"]) == 50

    all_attempted = set().union(*attempted_by_cycle)
    assert "package-199" in all_attempted
    assert "package-399" in all_attempted
    assert repository.claim_limits == [50, 50, 50, 50]


def test_cleanup_defers_when_guarded_row_delete_returns_false() -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    candidate = _candidate(1, pending_at=base)
    repository = _Repository(pending=[candidate], due=[])
    repository.delete_returns[candidate.revision_id] = False

    result = _service(_Store()).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(ecosystem="confluent_cloud", tenant_id="tenant-1", retention_days=30),
        backend=_Backend(repository),
        now=base,
    )

    assert result.deleted_count == 0
    assert result.deferred_count == 1
    assert repository.attempted_defer == [(candidate.revision_id, base + timedelta(microseconds=1))]


def test_cleanup_defers_one_microsecond_after_later_cycle_time() -> None:
    pending_at = datetime(2026, 8, 5, tzinfo=UTC)
    cycle_time = pending_at + timedelta(hours=2)
    candidate = _candidate(1, pending_at=pending_at)
    repository = _Repository(pending=[candidate], due=[])
    store = _Store(failures={candidate.storage_key})

    result = _service(store).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            retention_days=30,
        ),
        backend=_Backend(repository),
        now=cycle_time,
    )

    assert repository.attempted_defer == [(candidate.revision_id, cycle_time + timedelta(microseconds=1))]
    assert result.deleted_count == 0
    assert result.deferred_count == 1


@pytest.mark.parametrize("failure", ["row-delete", "delete-commit"])
def test_cleanup_uses_same_strictly_later_guarded_deferral_for_finalization_failures(
    failure: str,
) -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    candidate = _candidate(1, pending_at=base)
    repository = _Repository(pending=[candidate], due=[])
    backend = _Backend(repository)
    if failure == "row-delete":
        repository.delete_raises.add(candidate.revision_id)
    else:
        backend.fail_delete_commit_once = True

    result = _service(_Store()).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            retention_days=30,
        ),
        backend=backend,
        now=base,
    )

    retry_at = base + timedelta(microseconds=1)
    assert repository.attempted_defer == [(candidate.revision_id, retry_at)]
    assert result.deleted_count == 0
    assert result.deferred_count == 1
    assert repository.pending == [replace(candidate, retention_pending_at=retry_at)]


@pytest.mark.parametrize("failure", ["method", "commit"])
def test_deferral_persistence_failure_leaves_original_pending_claim_durable(
    failure: str,
) -> None:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    candidate = _candidate(1, pending_at=base)
    repository = _Repository(pending=[candidate], due=[])
    repository.delete_returns[candidate.revision_id] = False
    backend = _Backend(repository)
    if failure == "method":
        repository.defer_raises.add(candidate.revision_id)
    else:
        backend.fail_defer_commit_once = True

    result = _service(_Store()).cleanup_retention(
        tenant_name="production",
        tenant_config=TenantConfig(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            retention_days=30,
        ),
        backend=backend,
        now=base,
    )

    assert repository.attempted_defer == [(candidate.revision_id, base + timedelta(microseconds=1))]
    assert result.deleted_count == 0
    assert result.deferred_count == 0
    assert repository.pending == [candidate]
