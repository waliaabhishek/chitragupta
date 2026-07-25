from __future__ import annotations

import inspect
import threading
from datetime import UTC, date, datetime
from importlib import import_module
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import text

from core.config.models import FocusPreviewTenantConfig, TenantConfig
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider


def _tenant(tenant_id: str) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        lookback_days=200,
        cutoff_days=5,
        retention_days=250,
        focus_preview=FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=date(2026, 1, 1),
            effective_end_date=date(2026, 12, 31),
        ),
    )


def _repair_module() -> ModuleType:
    return import_module("core.preview.repair")


class _BlockingRunner:
    def __init__(self) -> None:
        self.started: list[str] = []
        self.started_event = threading.Event()
        self.release = threading.Event()
        self.raise_for: set[str] = set()
        self._lock = threading.Lock()

    def run_focus_preview_repair(
        self,
        repair_id: str,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> None:
        del repair_id, tenant_config
        with self._lock:
            self.started.append(tenant_name)
            self.started_event.set()
        if tenant_name in self.raise_for:
            raise RuntimeError("controlled worker failure")
        if not self.release.wait(timeout=5):
            raise TimeoutError("test did not release repair worker")


@pytest.fixture
def capacity_backend(tmp_path: Path):
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'capacity.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    yield backend
    backend.dispose()


def _runtime(
    backend: SQLModelBackend,
    runner: _BlockingRunner,
    *,
    workers: int,
    waiting: int,
) -> tuple[object, dict[str, TenantConfig]]:
    repair = _repair_module()
    tenants = {
        "one": _tenant("tenant-1"),
        "two": _tenant("tenant-2"),
        "three": _tenant("tenant-3"),
    }
    runtime = repair.PreviewRepairRuntime(
        runner=runner,
        backend_provider=FixedTenantBackendProvider({name: backend for name in tenants}),
        max_workers=workers,
        max_queued_repairs=waiting,
        configured_owners=tuple(tenants.items()),
        clock=lambda: datetime(2026, 7, 24, tzinfo=UTC),
    )
    return runtime, tenants


def _submit(
    runtime: object,
    backend: SQLModelBackend,
    tenant_name: str,
    tenant: TenantConfig,
) -> object:
    return runtime.submit(  # type: ignore[attr-defined]
        backend=backend,
        tenant_name=tenant_name,
        tenant_config=tenant,
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 2),
        created_at=datetime(2026, 7, 24, tzinfo=UTC),
    )


def _row_counts(backend: SQLModelBackend) -> tuple[int, int, int]:
    with backend._engine.connect() as connection:
        return (
            connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one(),
            connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_dates")).scalar_one(),
            connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_heads")).scalar_one(),
        )


def test_zero_wait_capacity_admits_only_available_running_position(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=0)
    repair = _repair_module()
    try:
        _submit(runtime, capacity_backend, "one", tenants["one"])
        assert runner.started_event.wait(timeout=2)

        with pytest.raises(repair.PreviewRepairCapacityUnavailable):
            _submit(runtime, capacity_backend, "two", tenants["two"])

        assert _row_counts(capacity_backend) == (1, 1, 1)
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_one_running_one_waiting_and_third_owner_is_rejected_before_persistence(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=1)
    repair = _repair_module()
    try:
        _submit(runtime, capacity_backend, "one", tenants["one"])
        assert runner.started_event.wait(timeout=2)
        _submit(runtime, capacity_backend, "two", tenants["two"])

        with pytest.raises(repair.PreviewRepairCapacityUnavailable):
            _submit(runtime, capacity_backend, "three", tenants["three"])

        assert runner.started == ["one"]
        assert _row_counts(capacity_backend) == (2, 2, 2)
        runner.release.set()
        runtime.close(wait=True)
        assert runner.started == ["one", "two"]
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_same_tenant_active_conflict_precedes_global_capacity(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=0)
    try:
        _submit(runtime, capacity_backend, "one", tenants["one"])
        assert runner.started_event.wait(timeout=2)

        with pytest.raises(RuntimeError, match="active_repair"):
            _submit(runtime, capacity_backend, "one", tenants["one"])

        assert _row_counts(capacity_backend) == (1, 1, 1)
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_waiting_work_starts_after_running_work_releases(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=1)
    try:
        _submit(runtime, capacity_backend, "one", tenants["one"])
        assert runner.started_event.wait(timeout=2)
        _submit(runtime, capacity_backend, "two", tenants["two"])
        runner.release.set()
        runtime.close(wait=True)

        assert runner.started == ["one", "two"]
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_worker_exception_terminalizes_state_and_releases_capacity(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runner.raise_for.add("one")
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=0)
    try:
        first = _submit(runtime, capacity_backend, "one", tenants["one"])
        assert runner.started_event.wait(timeout=2)
        persisted_status = None
        for _ in range(100):
            with capacity_backend._engine.connect() as connection:
                persisted_status = connection.execute(
                    text(
                        """
                        SELECT status
                        FROM ccloud_focus_preview_repairs
                        WHERE repair_id = :repair_id
                        """
                    ),
                    {"repair_id": first.repair_id},  # type: ignore[union-attr]
                ).scalar_one()
            if persisted_status == "failed":
                break
            threading.Event().wait(0.01)
        assert persisted_status == "failed"
        with capacity_backend.create_preview_generation_read_unit_of_work() as uow:
            persisted = uow.repairs.get_for_owner(
                first.repair_id,  # type: ignore[union-attr]
                "confluent_cloud",
                "tenant-1",
            )
        assert persisted is not None and persisted.status.value == "failed"

        second = _submit(runtime, capacity_backend, "two", tenants["two"])
        assert second.tenant_name == "two"  # type: ignore[union-attr]
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_persistence_failure_releases_reserved_capacity(
    capacity_backend: SQLModelBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=0)
    original = capacity_backend.create_preview_evidence_unit_of_work

    class _FailingCommit:
        def __init__(self) -> None:
            self.inner = original()

        def __enter__(self) -> _FailingCommit:
            entered = self.inner.__enter__()
            self.repairs = entered.repairs
            return self

        def __exit__(self, *args: object) -> object:
            return self.inner.__exit__(*args)  # type: ignore[arg-type]

        def commit(self) -> None:
            raise RuntimeError("controlled persistence failure")

    monkeypatch.setattr(
        capacity_backend,
        "create_preview_evidence_unit_of_work",
        _FailingCommit,
    )
    try:
        with pytest.raises(RuntimeError, match="controlled persistence failure"):
            _submit(runtime, capacity_backend, "one", tenants["one"])
        monkeypatch.setattr(
            capacity_backend,
            "create_preview_evidence_unit_of_work",
            original,
        )

        admitted = _submit(runtime, capacity_backend, "two", tenants["two"])

        assert admitted.tenant_name == "two"  # type: ignore[union-attr]
        assert _row_counts(capacity_backend) == (1, 1, 1)
    finally:
        runner.release.set()
        runtime.close(wait=True)


def test_close_prevents_new_admission_and_drains_accepted_work(
    capacity_backend: SQLModelBackend,
) -> None:
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=1)
    _submit(runtime, capacity_backend, "one", tenants["one"])
    assert runner.started_event.wait(timeout=2)
    _submit(runtime, capacity_backend, "two", tenants["two"])

    runtime.close(wait=False)
    with pytest.raises(RuntimeError, match="active_repair"):
        _submit(runtime, capacity_backend, "one", tenants["one"])
    with pytest.raises(RuntimeError, match="worker|closed"):
        _submit(runtime, capacity_backend, "three", tenants["three"])
    runner.release.set()
    runtime.close(wait=True)

    assert runner.started == ["one", "two"]


def test_close_waits_for_inflight_submit_then_drains_accepted_work(
    capacity_backend: SQLModelBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_module = import_module("plugins.confluent_cloud.storage.preview_repositories")
    repository_type = repository_module.SQLModelPreviewRepairRepository
    original_create_queued = repository_type.create_queued
    persistence_entered = threading.Event()
    release_persistence = threading.Event()
    close_finished = threading.Event()
    runner = _BlockingRunner()
    runtime, tenants = _runtime(capacity_backend, runner, workers=1, waiting=0)
    accepted: list[object] = []
    errors: list[BaseException] = []

    def blocking_create_queued(repository: object, repair: object) -> object:
        persistence_entered.set()
        if not release_persistence.wait(timeout=2):
            raise TimeoutError("test did not release repair persistence")
        return original_create_queued(repository, repair)

    monkeypatch.setattr(repository_type, "create_queued", blocking_create_queued)

    def submit() -> None:
        try:
            accepted.append(_submit(runtime, capacity_backend, "one", tenants["one"]))
        except BaseException as exc:
            errors.append(exc)

    def close_without_wait() -> None:
        runtime.close(wait=False)  # type: ignore[attr-defined]
        close_finished.set()

    submit_thread = threading.Thread(target=submit)
    close_thread = threading.Thread(target=close_without_wait)
    try:
        submit_thread.start()
        assert persistence_entered.wait(timeout=2)
        close_thread.start()
        assert not close_finished.wait(timeout=0.1)

        release_persistence.set()
        submit_thread.join(timeout=2)
        close_thread.join(timeout=2)

        assert not submit_thread.is_alive()
        assert not close_thread.is_alive()
        assert errors == []
        assert len(accepted) == 1
        assert _row_counts(capacity_backend) == (1, 1, 1)
        assert runner.started_event.wait(timeout=2)
    finally:
        release_persistence.set()
        runner.release.set()
        runtime.close(wait=True)

    assert runner.started == ["one"]


def test_repair_runtime_does_not_use_thread_pool_executor_or_unbounded_submission() -> None:
    source = inspect.getsource(_repair_module().PreviewRepairRuntime)

    assert "ThreadPoolExecutor" not in source
    assert "executor.submit" not in source
