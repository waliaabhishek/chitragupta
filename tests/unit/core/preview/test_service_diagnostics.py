from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.preview.conftest import preview_module
from tests.unit.core.preview.test_service import (
    ControlledExecutor,
    _aggregate,
    _allocation,
    _runtime,
    _seed,
    _source,
    _tenant_config,
)


def _backend(tmp_path: Path) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'preview.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    return backend


def _pipeline_state(
    tracking_date: date,
    *,
    calculated: bool,
    calculation_id: str | None = None,
    has_completed_at: bool = False,
) -> PipelineState:
    return PipelineState(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=tracking_date,
        billing_gathered=True,
        resources_gathered=True,
        chargeback_calculated=calculated,
        calculation_id=calculation_id,
        calculation_completed_at=(datetime(2026, 7, 4, tracking_date.day, tzinfo=UTC) if has_completed_at else None),
        calculation_run_id=None,
    )


def _submit_range(runtime: object, backend: SQLModelBackend, end_date: date) -> object:
    return runtime.submit(  # type: ignore[attr-defined,no-any-return]
        tenant_name="production",
        tenant_config=_tenant_config(backend._connection_string),
        backend=backend,
        start_date=date(2026, 7, 1),
        end_date=end_date,
        grain="daily",
        column_profile="full",
    )


@pytest.mark.parametrize(
    ("state_specs", "end_date", "code", "message", "retryable"),
    [
        (
            [(date(2026, 7, 1), True, None, False)],
            date(2026, 7, 2),
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
            False,
        ),
        (
            [(date(2026, 7, 1), True, None, False), (date(2026, 7, 2), False, None, False)],
            date(2026, 7, 3),
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
            False,
        ),
        (
            [(date(2026, 7, 1), True, "usable-a", True), (date(2026, 7, 2), True, None, False)],
            date(2026, 7, 3),
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
            False,
        ),
        (
            [
                (date(2026, 7, 1), True, "usable-a", True),
                (date(2026, 7, 2), True, None, False),
                (date(2026, 7, 3), False, None, False),
            ],
            date(2026, 7, 4),
            "calculation_metadata_unavailable",
            "One or more requested dates lack preview calculation metadata.",
            False,
        ),
        (
            [(date(2026, 7, 1), False, None, False)],
            date(2026, 7, 2),
            "calculation_unavailable",
            "No successful persisted calculation is available for the requested dates; run the pipeline and retry.",
            True,
        ),
        (
            [(date(2026, 7, 1), True, "usable-a", True), (date(2026, 7, 2), False, None, False)],
            date(2026, 7, 3),
            "calculation_coverage_incomplete",
            "No successful persisted calculation covers every requested date; run the pipeline and retry.",
            True,
        ),
    ],
)
def test_calculation_diagnostic_matrix_and_incomplete_metadata_precedence(
    tmp_path: Path,
    state_specs: list[tuple[date, bool, str | None, bool]],
    end_date: date,
    code: str,
    message: str,
    retryable: bool,
) -> None:
    states = [
        _pipeline_state(
            tracking_date,
            calculated=calculated,
            calculation_id=calculation_id,
            has_completed_at=has_completed_at,
        )
        for tracking_date, calculated, calculation_id, has_completed_at in state_specs
    ]
    backend = _backend(tmp_path)
    _seed(backend, state=states[0])
    with backend.create_unit_of_work() as uow:
        for state in states[1:]:
            uow.pipeline_state.upsert(state)
        uow.commit()
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    try:
        queued = _submit_range(runtime, backend, end_date)
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == code
        assert failed.diagnostic.message == message
        assert failed.diagnostic.retryable is retryable
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
    finally:
        runtime.close()
        backend.dispose()


class FailingExecutor:
    """Full PreviewExecutor fake whose scheduler rejects work."""

    def submit(self, task: Callable[[], None]) -> Future[None]:
        del task
        raise RuntimeError("scheduler offline")

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures


def test_scheduling_failure_persists_failed_request_and_raises_worker_unavailable(tmp_path: Path) -> None:
    backend = _backend(tmp_path)
    _seed(backend, state=_pipeline_state(date(2026, 7, 1), calculated=False))
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        max_workers=1,
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
        request_id_factory=lambda: "request-1",
        executor=FailingExecutor(),
    )
    try:
        with pytest.raises(service.PreviewWorkerUnavailable):
            _submit_range(runtime, backend, date(2026, 7, 2))
        failed = runtime.get_request(
            backend=backend,
            request_id="request-1",
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_worker_unavailable"
        assert failed.package is None
    finally:
        runtime.close()
        backend.dispose()


def test_unexpected_scheduler_failure_is_logged_and_persists_redacted_diagnostic(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, state=_pipeline_state(date(2026, 7, 1), calculated=False))
    artifacts = preview_module("artifacts")
    service = preview_module("service")
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        max_workers=1,
        request_id_factory=lambda: "request-logged",
        executor=FailingExecutor(),
    )
    try:
        with (
            caplog.at_level("ERROR", logger="core.preview.service"),
            pytest.raises(service.PreviewWorkerUnavailable),
        ):
            _submit_range(runtime, backend, date(2026, 7, 2))
        assert any(
            record.levelname == "ERROR"
            and "worker scheduling failed" in record.getMessage().casefold()
            and record.exc_info is not None
            for record in caplog.records
        )
    finally:
        runtime.close()
        backend.dispose()


def test_unexpected_generation_failure_logs_request_and_leaves_no_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    service = preview_module("service")
    monkeypatch.setattr(service, "build_daily_full_package", lambda **_kwargs: (_ for _ in ()).throw(OSError("disk")))
    try:
        queued = _submit_range(runtime, backend, date(2026, 7, 2))
        with caplog.at_level("ERROR", logger="core.preview.service"):
            executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_generation_failed"
        assert failed.source_snapshot is None
        assert failed.package is None
        assert failed.storage_key is None
        assert not (tmp_path / "artifacts").exists() or list((tmp_path / "artifacts").iterdir()) == []
        assert any(
            record.levelname == "ERROR" and queued.request_id in record.getMessage() and record.exc_info is not None
            for record in caplog.records
        )
    finally:
        runtime.close()
        backend.dispose()


def test_post_rename_pre_ready_failure_leaves_inaccessible_orphan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    persistence = preview_module("persistence")
    real_mark_ready = persistence.SQLModelPreviewRequestRepository.mark_ready

    def fail_ready(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        raise OSError("database unavailable after rename")

    monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", fail_ready)
    try:
        queued = _submit_range(runtime, backend, date(2026, 7, 2))
        executor.run_all()
        failed = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert failed.status.value == "failed"
        assert failed.diagnostic.code == "preview_generation_failed"
        assert failed.storage_key is None
        assert failed.package is None
        final_paths = [path for path in (tmp_path / "artifacts").iterdir() if not path.name.endswith(".staging")]
        assert len(final_paths) == 1
        with pytest.raises(FileNotFoundError):
            runtime.read_manifest_bytes(failed)
    finally:
        monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", real_mark_ready)
        runtime.close()
        backend.dispose()


def test_runtime_owns_and_shuts_down_only_default_executor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = preview_module("service")
    artifacts = preview_module("artifacts")
    created: list[ControlledExecutor] = []

    def make_executor(*, max_workers: int) -> ControlledExecutor:
        assert max_workers == 2
        executor = ControlledExecutor()
        created.append(executor)
        return executor

    monkeypatch.setattr(service, "ThreadPoolExecutor", make_executor)
    runtime = service.PreviewRuntime(
        artifact_store=artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts"),
        max_workers=2,
    )

    runtime.close(wait=True)
    runtime.close(wait=True)

    assert len(created) == 1
    assert created[0].shutdown_calls == [(True, False)]
