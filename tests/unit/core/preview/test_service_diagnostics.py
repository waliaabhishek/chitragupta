from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import Future
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import event

from core.models.pipeline import PipelineState
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
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
        focus_preview_enabled=True,
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
        effective_columns=preview_module("mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
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
            "calculation_pending_cutoff_window",
            (
                "One or more requested dates are still inside the configured acquisition cutoff window; "
                "wait for the dates to enter the acquisition window, run the pipeline, and retry."
            ),
            True,
        ),
        (
            [(date(2026, 7, 1), True, "usable-a", True), (date(2026, 7, 2), False, None, False)],
            date(2026, 7, 3),
            "calculation_pending_cutoff_window",
            (
                "One or more requested dates are still inside the configured acquisition cutoff window; "
                "wait for the dates to enter the acquisition window, run the pipeline, and retry."
            ),
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
        backend_provider=FixedTenantBackendProvider({"production": backend}),
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
        backend_provider=FixedTenantBackendProvider({"production": backend}),
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
        records = [
            record
            for record in caplog.records
            if record.levelname == "ERROR" and "worker scheduling failed" in record.getMessage().casefold()
        ]
        assert len(records) == 1
        assert "tenant=production" in records[0].getMessage()
        assert "request_id=request-logged" in records[0].getMessage()
        assert "error_type=RuntimeError" in records[0].getMessage()
        assert records[0].exc_info is None
        assert "scheduler offline" not in caplog.text
        assert "Traceback" not in caplog.text
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
    generator = preview_module("generator")
    monkeypatch.setattr(
        generator,
        "build_preview_data_package",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("disk")),
    )
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
        records = [
            record
            for record in caplog.records
            if record.levelname == "ERROR" and queued.request_id in record.getMessage()
        ]
        assert len(records) == 1
        assert "tenant=production" in records[0].getMessage()
        assert "error_type=OSError" in records[0].getMessage()
        assert records[0].exc_info is None
        assert "disk" not in caplog.text
        assert "Traceback" not in caplog.text
    finally:
        runtime.close()
        backend.dispose()


@pytest.mark.parametrize("failure_point", ["stage", "publish"])
def test_stage_or_publish_failure_leaves_failed_request_and_no_ready_package(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    store = runtime._artifact_store

    real_begin = store.begin_generation

    class GenerationFailure:
        def __init__(self, generation: object) -> None:
            self.generation = generation

        @property
        def workspace(self) -> object:
            return self.generation.workspace

        @property
        def files(self) -> object:
            return self.generation.files

        def stage_data_files(self, data_files: object) -> None:
            if failure_point == "stage":
                raise OSError("stage failed")
            self.generation.stage_data_files(data_files)

        def stage_metadata_file(self, metadata_file: object) -> None:
            self.generation.stage_metadata_file(metadata_file)

        def publish(self, *, manifest_body: bytes) -> object:
            if failure_point == "publish":
                raise OSError("publish failed")
            return self.generation.publish(manifest_body=manifest_body)

        def close(self) -> None:
            self.generation.close()

        def __enter__(self) -> GenerationFailure:
            self.generation.__enter__()
            return self

        def __exit__(self, *args: object) -> None:
            self.generation.__exit__(*args)

    monkeypatch.setattr(
        store,
        "begin_generation",
        lambda **kwargs: GenerationFailure(real_begin(**kwargs)),
    )
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
        assert not list((tmp_path / "artifacts").glob("*.staging"))
    finally:
        runtime.close()
        backend.dispose()


def test_post_rename_pre_ready_failure_reclaims_inaccessible_orphan(
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
        owner = preview_module("artifacts").preview_artifact_owner(
            "production",
            _tenant_config(backend._connection_string),
        )
        runtime.ensure_owner_recovered(backend=backend, owner=owner)
        final_paths = [path for path in (tmp_path / "artifacts").iterdir() if path.name.startswith("v1-")]
        assert final_paths == []
        with pytest.raises(FileNotFoundError):
            runtime.read_manifest_bytes(failed)
    finally:
        monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", real_mark_ready)
        runtime.close()
        backend.dispose()


def test_false_ready_compare_and_set_is_not_silently_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    persistence = preview_module("persistence")

    monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", lambda *_args, **_kwargs: False)
    try:
        queued = _submit_range(runtime, backend, date(2026, 7, 2))
        with caplog.at_level("ERROR", logger="core.preview.service"):
            executor.run_all()
        terminal = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert terminal is not None
        assert terminal.status.value == "failed"
        assert terminal.diagnostic.code == "preview_generation_failed"
        assert terminal.package is None
        assert any("ready transition rejected" in message for message in caplog.messages)
    finally:
        runtime.close()
        backend.dispose()


def test_worker_ready_path_preserves_validation_artifact_and_compare_and_set_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    runtime = _runtime(tmp_path, backend, executor)
    mapping = preview_module("mapping")
    models = preview_module("models")
    persistence = preview_module("persistence")
    generator = preview_module("generator")
    service = preview_module("service")
    artifacts = preview_module("artifacts")
    events: list[str] = []

    real_mark_running = persistence.SQLModelPreviewRequestRepository.mark_running
    real_mark_ready = persistence.SQLModelPreviewRequestRepository.mark_ready
    real_validate_columns = mapping.validate_preview_effective_columns
    real_candidate_validate = mapping.validate_preview_request_snapshot
    real_build_data = generator.build_preview_data_package
    real_build_manifest = service.build_requested_preview_manifest
    real_begin = runtime._artifact_store.begin_generation
    real_replace = persistence.replace
    real_post_init = models.PreviewRequest.__post_init__
    real_strict_validate = persistence.validate_preview_request_snapshot

    def mark_running(*args: object, **kwargs: object) -> object:
        result = real_mark_running(*args, **kwargs)
        if result is not None:
            events.append("running-candidate")
        return result

    def validate_columns(profile: str, columns: tuple[str, ...]) -> None:
        events.append("effective-columns")
        real_validate_columns(profile, columns)

    def candidate_validate(**kwargs: object) -> None:
        if kwargs["mode"] == "candidate_ready":
            events.append("candidate-snapshot")
        real_candidate_validate(**kwargs)

    def build_data(*args: object, **kwargs: object) -> object:
        result = real_build_data(*args, **kwargs)
        events.append("data-draft-built")
        return result

    class RecordingGeneration:
        def __init__(self, generation: object) -> None:
            self._generation = generation

        @property
        def workspace(self) -> object:
            return self._generation.workspace

        @property
        def files(self) -> object:
            return self._generation.files

        def stage_data_files(self, data_files: object) -> None:
            self._generation.stage_data_files(data_files)
            events.append("data-staged-fsynced")

        def stage_metadata_file(self, metadata_file: object) -> None:
            self._generation.stage_metadata_file(metadata_file)

        def publish(self, *, manifest_body: bytes) -> object:
            events.append("published")
            return self._generation.publish(manifest_body=manifest_body)

        def close(self) -> None:
            self._generation.close()

        def __enter__(self) -> RecordingGeneration:
            self._generation.__enter__()
            return self

        def __exit__(self, *args: object) -> None:
            self._generation.__exit__(*args)

    def begin(*args: object, **kwargs: object) -> object:
        return RecordingGeneration(real_begin(*args, **kwargs))

    captured_manifest_times: list[tuple[datetime, datetime]] = []

    def build_manifest(*args: object, **kwargs: object) -> bytes:
        captured_manifest_times.append((kwargs["ready_at"], kwargs["expires_at"]))
        events.append("manifest-built")
        return real_build_manifest(*args, **kwargs)

    def replace_candidate(instance: object, /, **changes: object) -> object:
        if changes.get("status") is models.PreviewRequestStatus.READY:
            events.append("ready-construction")
        return real_replace(instance, **changes)

    def post_init(instance: object) -> None:
        if getattr(instance, "status", None) is models.PreviewRequestStatus.READY:
            events.append("ready-post-init")
        real_post_init(instance)

    def strict_validate(**kwargs: object) -> None:
        if kwargs["mode"] == "strict_materialized" and kwargs["resulting_status"] is models.PreviewRequestStatus.READY:
            events.append("ready-strict-validation")
        real_strict_validate(**kwargs)

    def capture_sql(
        _conn: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if events and events[-1] == "ready-strict-validation" and statement.lstrip().upper().startswith("UPDATE"):
            events.append("ready-cas-sql")

    captured_ready_times: list[tuple[datetime, datetime]] = []

    def mark_ready(*args: object, **kwargs: object) -> bool:
        captured_ready_times.append((args[2], args[3]))
        lock_paths = tuple((tmp_path / "artifacts" / ".locks").rglob("*.lock"))
        assert len(lock_paths) == 1
        with lock_paths[0].open("a+b") as competing_handle, pytest.raises(BlockingIOError):
            artifacts.fcntl.flock(
                competing_handle.fileno(),
                artifacts.fcntl.LOCK_EX | artifacts.fcntl.LOCK_NB,
            )
        events.append("stable-lock-held")
        result = real_mark_ready(*args, **kwargs)
        events.append(f"ready-cas-result:{result}")
        return result

    monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_running", mark_running)
    monkeypatch.setattr(persistence.SQLModelPreviewRequestRepository, "mark_ready", mark_ready)
    monkeypatch.setattr(mapping, "validate_preview_effective_columns", validate_columns)
    monkeypatch.setattr(mapping, "validate_preview_request_snapshot", candidate_validate)
    monkeypatch.setattr(generator, "build_preview_data_package", build_data)
    monkeypatch.setattr(service, "build_requested_preview_manifest", build_manifest)
    monkeypatch.setattr(runtime._artifact_store, "begin_generation", begin)
    monkeypatch.setattr(persistence, "replace", replace_candidate)
    monkeypatch.setattr(models.PreviewRequest, "__post_init__", post_init)
    monkeypatch.setattr(persistence, "validate_preview_request_snapshot", strict_validate)
    event.listen(backend._engine, "before_cursor_execute", capture_sql)
    try:
        queued = _submit_range(runtime, backend, date(2026, 7, 2))
        events.clear()
        executor.run_all()

        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )
        assert ready is not None and ready.status.value == "ready"
        assert events[:13] == [
            "running-candidate",
            "effective-columns",
            "candidate-snapshot",
            "data-draft-built",
            "data-staged-fsynced",
            "manifest-built",
            "published",
            "stable-lock-held",
            "ready-construction",
            "ready-post-init",
            "ready-strict-validation",
            "ready-cas-sql",
            "ready-cas-result:True",
        ]
        assert captured_manifest_times == captured_ready_times
        assert len(captured_ready_times) == 1
        ready_at, expires_at = captured_ready_times[0]
        assert ready_at.microsecond == 0
        assert expires_at == ready_at + timedelta(days=7)
        assert not tuple((tmp_path / "artifacts").rglob("*.lock"))
    finally:
        event.remove(backend._engine, "before_cursor_execute", capture_sql)
        runtime.close()
        backend.dispose()


def test_rendering_and_data_fsync_finish_before_retention_clock_starts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _backend(tmp_path)
    _seed(backend, source=_source(), aggregate=_aggregate(), allocation=_allocation())
    executor = ControlledExecutor()
    artifacts = preview_module("artifacts")
    generator = preview_module("generator")
    service = preview_module("service")

    class AdvancingClock:
        now = datetime(2026, 7, 4, tzinfo=UTC)

        def __call__(self) -> datetime:
            return self.now

    clock = AdvancingClock()
    store = artifacts.LocalPreviewArtifactStore(tmp_path / "artifacts")
    runtime = service.PreviewRuntime(
        artifact_store=store,
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        clock=clock,
        request_id_factory=lambda: "request-retention",
        executor=executor,
    )
    real_build = generator.build_preview_data_package
    real_begin = store.begin_generation

    def slow_build(**kwargs: object) -> object:
        result = real_build(**kwargs)
        clock.now += timedelta(hours=3)
        return result

    class SlowGeneration:
        def __init__(self, generation: object) -> None:
            self.generation = generation

        def __getattr__(self, name: str) -> object:
            return getattr(self.generation, name)

        def stage_data_files(self, data_files: object) -> None:
            self.generation.stage_data_files(data_files)
            clock.now += timedelta(hours=4)

        def __enter__(self) -> SlowGeneration:
            self.generation.__enter__()
            return self

        def __exit__(self, *args: object) -> None:
            self.generation.__exit__(*args)

    def slow_begin(**kwargs: object) -> object:
        return SlowGeneration(real_begin(**kwargs))

    monkeypatch.setattr(generator, "build_preview_data_package", slow_build)
    monkeypatch.setattr(store, "begin_generation", slow_begin)
    try:
        queued = _submit_range(runtime, backend, date(2026, 7, 2))
        executor.run_all()
        ready = runtime.get_request(
            backend=backend,
            request_id=queued.request_id,
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
        )

        assert ready.status.value == "ready"
        assert ready.completed_at == datetime(2026, 7, 4, 7, tzinfo=UTC)
        assert ready.expires_at == datetime(2026, 7, 11, 7, tzinfo=UTC)
        manifest = json.loads(runtime.read_manifest_bytes(ready))
        assert manifest["lifecycle"] == {
            "ready_at": "2026-07-04T07:00:00Z",
            "expires_at": "2026-07-11T07:00:00Z",
            "retention_days": 7,
        }
    finally:
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
        backend_provider=FixedTenantBackendProvider(),
        max_workers=2,
    )

    runtime.close(wait=True)
    runtime.close(wait=True)

    assert len(created) == 1
    assert created[0].shutdown_calls == [(True, False)]
