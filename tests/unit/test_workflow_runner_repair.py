from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.config.models import (
    AppSettings,
    FocusPreviewTenantConfig,
    StorageConfig,
    TenantConfig,
)
from core.engine.orchestrator import (
    CalculationPhaseResult,
    HistoricalRepairDateResult,
    PipelineRunResult,
)
from core.models.pipeline import PipelineState
from core.preview.evidence import PreviewEvidenceScope
from core.preview.evidence_capture import NativeSourceWindow
from core.preview.repair import PreviewRepairRuntime
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.interface import AllocationLineageRunCapture
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash

NOW = datetime(2026, 7, 23, tzinfo=UTC)
DAY = date(2026, 7, 1)


class _ControlledExecutor:
    def __init__(self) -> None:
        self.pending: list[Any] = []

    def submit(self, fn: Any) -> object:
        self.pending.append(fn)
        return object()

    def run_all(self) -> None:
        while self.pending:
            self.pending.pop(0)()

    def shutdown(self, wait: bool = True) -> None:
        del wait


def _tenant(tmp_path: Path) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=200,
        cutoff_days=5,
        retention_days=250,
        storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'repair-runtime.db'}"),
        focus_preview=FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=date(2026, 1, 1),
            effective_end_date=date(2026, 12, 31),
        ),
    )


def _setup(
    tmp_path: Path,
    *,
    orchestrator: object | None = None,
) -> tuple[
    TenantConfig,
    SQLModelBackend,
    WorkflowRunner,
    PreviewRepairRuntime,
    _ControlledExecutor,
]:
    tenant = _tenant(tmp_path)
    backend = SQLModelBackend(
        tenant.storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    runner = WorkflowRunner(
        AppSettings(tenants={"production": tenant}),
        MagicMock(),
    )
    runner._tenant_runtimes["production"] = TenantRuntime(
        tenant_name="production",
        plugin=MagicMock(),
        storage=backend,
        orchestrator=orchestrator or MagicMock(),
        config_hash=_config_hash(tenant),
        created_at=NOW,
    )
    executor = _ControlledExecutor()
    runtime = PreviewRepairRuntime(
        runner=runner,
        backend_provider=FixedTenantBackendProvider({"production": backend}),
        max_workers=1,
        configured_owners=(("production", tenant),),
        executor=executor,
    )
    return tenant, backend, runner, runtime, executor


def _queue(
    runtime: PreviewRepairRuntime,
    backend: SQLModelBackend,
    tenant: TenantConfig,
):
    return runtime.create_queued(
        backend=backend,
        tenant_name="production",
        tenant_config=tenant,
        start_date=DAY,
        end_date=DAY + timedelta(days=1),
        created_at=NOW,
    )


def _read(backend: SQLModelBackend, repair_id: str):
    with backend.create_preview_generation_read_unit_of_work() as uow:
        value = uow.repairs.get_for_owner(
            repair_id,
            "confluent_cloud",
            "tenant-1",
        )
    assert value is not None
    return value


def _seed_retained_state(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                tracking_date=DAY,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id="legacy-calculation",
                calculation_completed_at=NOW - timedelta(days=1),
            )
        )
        uow.commit()


def _successful_result(
    *,
    calculation_microsecond: int = 0,
) -> HistoricalRepairDateResult:
    day_start = datetime.combine(DAY, datetime.min.time(), tzinfo=UTC)
    calculation_id = "repaired-calculation"
    calculation_completed_at = (NOW + timedelta(seconds=1)).replace(
        microsecond=calculation_microsecond,
    )
    capture = CCloudNativeSourceEvidenceCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        refresh_start=day_start,
        refresh_end=day_start + timedelta(days=1),
        windows=(NativeSourceWindow(day_start, day_start + timedelta(days=1)),),
        records=(),
    )
    lineage = AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        tracking_date=DAY,
        calculation_id=calculation_id,
        captures=(),
    )
    return HistoricalRepairDateResult(
        source_capture=capture,
        calculation=CalculationPhaseResult(
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            tracking_date=DAY,
            rows_written=0,
            calculation_id=calculation_id,
            calculation_completed_at=calculation_completed_at,
            lineage_capture=lineage,
            lineage_failure=None,
        ),
        billing_rows_written=0,
    )


def test_busy_race_uses_guarded_queued_failure_without_provider_call(
    tmp_path: Path,
) -> None:
    orchestrator = MagicMock()
    tenant, backend, runner, runtime, _executor = _setup(
        tmp_path,
        orchestrator=orchestrator,
    )
    queued = _queue(runtime, backend, tenant)
    runner._running_tenants.add("production")
    try:
        runner.run_focus_preview_repair(
            queued.repair_id,
            "production",
            tenant,
        )
        failed = _read(backend, queued.repair_id)
        assert failed.status.value == "failed"
        assert failed.diagnostic is not None
        assert failed.diagnostic.code == "focus_preview_repair_tenant_busy"
        assert [item.status.value for item in failed.dates] == ["failed"]
        orchestrator.repair_historical_date.assert_not_called()
    finally:
        runner._running_tenants.discard("production")
        runtime.close(wait=True)
        backend.dispose()


def test_missing_retained_state_fails_date_and_continues_to_normal_completion(
    tmp_path: Path,
) -> None:
    orchestrator = MagicMock()
    tenant, backend, runner, runtime, _executor = _setup(
        tmp_path,
        orchestrator=orchestrator,
    )
    queued = _queue(runtime, backend, tenant)
    try:
        runner.run_focus_preview_repair(
            queued.repair_id,
            "production",
            tenant,
        )
        failed = _read(backend, queued.repair_id)
        assert failed.status.value == "completed_with_failures"
        assert failed.dates[0].failure_stage is not None
        assert failed.dates[0].failure_stage.value == "retained_state"
        assert failed.dates[0].diagnostic is not None
        assert failed.dates[0].diagnostic.code == "focus_preview_repair_retained_calculation_unavailable"
        orchestrator.repair_historical_date.assert_not_called()
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_guarded_date_transition_conflict_is_terminalized_by_worker_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugins.confluent_cloud.storage.preview_repositories import (
        SQLModelPreviewRepairRepository,
    )

    tenant, backend, _runner, runtime, executor = _setup(tmp_path)
    queued = _queue(runtime, backend, tenant)
    monkeypatch.setattr(
        SQLModelPreviewRepairRepository,
        "mark_date_running",
        lambda *args, **kwargs: None,
    )
    try:
        runtime.schedule(queued, tenant_config=tenant)
        executor.run_all()
        failed = _read(backend, queued.repair_id)
        assert failed.status.value == "failed"
        assert failed.diagnostic is not None
        assert failed.diagnostic.code == "focus_preview_repair_worker_unavailable"
        assert [item.status.value for item in failed.dates] == ["failed"]
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_worker_failure_equivalent_committed_state_matches_at_whole_seconds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugins.confluent_cloud.storage.preview_repositories import (
        SQLModelPreviewRepairRepository,
    )

    tenant, backend, runner, runtime, executor = _setup(tmp_path)
    queued = _queue(runtime, backend, tenant)
    runner.run_focus_preview_repair = MagicMock(
        side_effect=RuntimeError("controlled worker failure"),
    )
    runtime.clock = lambda: NOW.replace(microsecond=987_654)
    original = SQLModelPreviewRepairRepository.fail_queued_before_execution

    def persist_but_hide_return(
        repository: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        original(repository, *args, **kwargs)
        return None

    monkeypatch.setattr(
        SQLModelPreviewRepairRepository,
        "fail_queued_before_execution",
        persist_but_hide_return,
    )
    try:
        runtime.schedule(queued, tenant_config=tenant)
        executor.run_all()

        failed = _read(backend, queued.repair_id)
        assert failed.status.value == "failed"
        assert failed.completed_at == NOW
        assert failed.diagnostic is not None
        assert failed.diagnostic.code == "focus_preview_repair_worker_unavailable"
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_equivalent_committed_repair_transitions_match_at_whole_seconds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from plugins.confluent_cloud.storage.preview_repositories import (
        SQLModelPreviewRepairRepository,
    )

    orchestrator = MagicMock()
    orchestrator.repair_historical_date.return_value = _successful_result(
        calculation_microsecond=654_321,
    )
    tenant, backend, runner, runtime, _executor = _setup(
        tmp_path,
        orchestrator=orchestrator,
    )
    _seed_retained_state(backend)
    queued = _queue(runtime, backend, tenant)

    for method_name in (
        "mark_running",
        "mark_date_running",
        "mark_date_succeeded_from_running",
        "finalize_completed",
    ):
        original = getattr(SQLModelPreviewRepairRepository, method_name)

        def persist_but_hide_return(
            repository: object,
            *args: object,
            _original: Any = original,
            **kwargs: object,
        ) -> None:
            _original(repository, *args, **kwargs)
            return None

        monkeypatch.setattr(
            SQLModelPreviewRepairRepository,
            method_name,
            persist_but_hide_return,
        )
    monkeypatch.setattr(
        "core.preview.generator.PreviewPackageGenerator.generate",
        lambda *args, **kwargs: (MagicMock(), MagicMock()),
    )
    try:
        runner.run_focus_preview_repair(
            queued.repair_id,
            "production",
            tenant,
        )

        repaired = _read(backend, queued.repair_id)
        assert repaired.status.value == "completed"
        assert repaired.started_at is not None and repaired.started_at.microsecond == 0
        assert repaired.completed_at is not None and repaired.completed_at.microsecond == 0
        assert repaired.dates[0].started_at is not None
        assert repaired.dates[0].started_at.microsecond == 0
        assert repaired.dates[0].completed_at is not None
        assert repaired.dates[0].completed_at.microsecond == 0
        assert repaired.dates[0].calculation_completed_at == NOW + timedelta(seconds=1)
    finally:
        runtime.close(wait=True)
        backend.dispose()


@pytest.mark.parametrize("ambiguous_commit", [False, True])
def test_evidence_failure_fails_closed_but_exact_ambiguous_commit_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ambiguous_commit: bool,
) -> None:
    orchestrator = MagicMock()
    orchestrator.repair_historical_date.return_value = _successful_result()
    tenant, backend, runner, runtime, _executor = _setup(
        tmp_path,
        orchestrator=orchestrator,
    )
    _seed_retained_state(backend)
    queued = _queue(runtime, backend, tenant)
    original_factory = backend.create_preview_evidence_unit_of_work
    calls = 0

    def create_uow():
        nonlocal calls
        calls += 1
        uow = original_factory()
        if calls == 4:
            commit = uow.commit

            def fail_commit() -> None:
                if ambiguous_commit:
                    commit()
                raise RuntimeError("controlled evidence commit outcome")

            uow.commit = fail_commit  # type: ignore[method-assign]
        return uow

    monkeypatch.setattr(
        backend,
        "create_preview_evidence_unit_of_work",
        create_uow,
    )
    monkeypatch.setattr(
        "core.preview.generator.PreviewPackageGenerator.generate",
        lambda *args, **kwargs: (MagicMock(), MagicMock()),
    )
    try:
        runner.run_focus_preview_repair(
            queued.repair_id,
            "production",
            tenant,
        )
        repaired = _read(backend, queued.repair_id)
        expected = "completed" if ambiguous_commit else "completed_with_failures"
        assert repaired.status.value == expected
        assert repaired.dates[0].status.value == ("succeeded" if ambiguous_commit else "failed")
        if not ambiguous_commit:
            assert repaired.dates[0].failure_stage is not None
            assert repaired.dates[0].failure_stage.value == "evidence"
        with backend.create_preview_generation_read_unit_of_work() as uow:
            attempt = uow.source_readiness.get_by_token(
                "confluent_cloud",
                "tenant-1",
                f"repair:{queued.repair_id}:{DAY.isoformat()}",
            )
        assert attempt is not None
        assert attempt.status.value == ("complete" if ambiguous_commit else "failed")
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_preview_validation_failure_is_reported_for_the_date_and_operation_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.preview.generator import PreviewGenerationError
    from core.preview.models import PreviewDiagnostic

    orchestrator = MagicMock()
    orchestrator.repair_historical_date.return_value = _successful_result()
    tenant, backend, runner, runtime, _executor = _setup(
        tmp_path,
        orchestrator=orchestrator,
    )
    _seed_retained_state(backend)
    queued = _queue(runtime, backend, tenant)
    validation_diagnostic = PreviewDiagnostic(
        "preview_source_unavailable",
        "controlled validation failure",
        False,
    )
    monkeypatch.setattr(
        "core.preview.generator.PreviewPackageGenerator.generate",
        MagicMock(side_effect=PreviewGenerationError(validation_diagnostic)),
    )
    try:
        runner.run_focus_preview_repair(
            queued.repair_id,
            "production",
            tenant,
        )
        repaired = _read(backend, queued.repair_id)
        assert repaired.status.value == "completed_with_failures"
        assert repaired.dates[0].status.value == "failed"
        assert repaired.dates[0].failure_stage is not None
        assert repaired.dates[0].failure_stage.value == "preview_validation"
        assert repaired.dates[0].diagnostic == validation_diagnostic
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_runtime_recover_fails_queued_and_partially_running_operations(
    tmp_path: Path,
) -> None:
    tenant, backend, _runner, runtime, _executor = _setup(tmp_path)
    queued = _queue(runtime, backend, tenant)
    try:
        runtime.recover()
        recovered_queued = _read(backend, queued.repair_id)
        assert recovered_queued.status.value == "failed"
        assert recovered_queued.diagnostic is not None
        assert recovered_queued.diagnostic.code == "focus_preview_repair_interrupted"

        running = _queue(runtime, backend, tenant)
        with backend.create_preview_evidence_unit_of_work() as uow:
            assert uow.repairs.mark_running(running.repair_id, started_at=NOW + timedelta(seconds=1))
            assert uow.repairs.mark_date_running(
                running.repair_id,
                DAY,
                started_at=NOW + timedelta(seconds=1),
            )
            uow.commit()
        runtime.recover()
        recovered_running = _read(backend, running.repair_id)
        assert recovered_running.status.value == "failed"
        assert [item.status.value for item in recovered_running.dates] == ["failed"]
        assert recovered_running.diagnostic is not None
        assert recovered_running.diagnostic.code == "focus_preview_repair_interrupted"
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_retry_after_runtime_recovery_succeeds_without_duplicate_current_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    orchestrator = MagicMock()
    orchestrator.repair_historical_date.return_value = _successful_result()
    tenant, backend, runner, runtime, executor = _setup(tmp_path, orchestrator=orchestrator)
    interrupted = _queue(runtime, backend, tenant)
    _seed_retained_state(backend)
    monkeypatch.setattr(
        "core.preview.generator.PreviewPackageGenerator.generate",
        lambda *args, **kwargs: (MagicMock(), MagicMock()),
    )
    try:
        runtime.recover()
        retry = _queue(runtime, backend, tenant)
        runtime.schedule(retry, tenant_config=tenant)
        executor.run_all()

        assert _read(backend, interrupted.repair_id).status.value == "failed"
        assert _read(backend, retry.repair_id).status.value == "completed"
        with backend.create_preview_generation_read_unit_of_work() as uow:
            authority = uow.source_readiness.resolve_authority(
                "confluent_cloud",
                "tenant-1",
                datetime.combine(DAY, datetime.min.time(), tzinfo=UTC),
                datetime.combine(DAY + timedelta(days=1), datetime.min.time(), tzinfo=UTC),
            )
            lineage = tuple(
                uow.allocation_evidence.iter_preview_allocation_runs(
                    PreviewEvidenceScope(
                        "confluent_cloud",
                        "tenant-1",
                        datetime.combine(DAY, datetime.min.time(), tzinfo=UTC),
                        datetime.combine(DAY + timedelta(days=1), datetime.min.time(), tzinfo=UTC),
                    ),
                    ("repaired-calculation",),
                )
            )
        assert len(authority) == 1
        assert authority[0].attempt is not None
        assert authority[0].attempt.refresh_token == f"repair:{retry.repair_id}:{DAY.isoformat()}"
        assert len(lineage) == 1
    finally:
        runtime.close(wait=True)
        backend.dispose()


def test_active_repair_claim_blocks_same_tenant_but_not_another_tenant(
    tmp_path: Path,
) -> None:
    _tenant_config, backend, runner, runtime, _executor = _setup(tmp_path)
    sandbox = _tenant(tmp_path).model_copy(
        update={
            "tenant_id": "tenant-2",
            "storage": StorageConfig(connection_string=f"sqlite:///{tmp_path / 'sandbox-runtime.db'}"),
        }
    )
    sandbox_backend = SQLModelBackend(
        sandbox.storage.connection_string.get_secret_value(),
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    sandbox_backend.create_tables()
    sandbox_orchestrator = MagicMock()
    sandbox_orchestrator.run.return_value = PipelineRunResult(
        tenant_name="sandbox",
        tenant_id="tenant-2",
        dates_gathered=1,
        dates_calculated=1,
        chargeback_rows_written=1,
        dates_pending_calculation=0,
    )
    runner._settings.tenants["sandbox"] = sandbox
    runner._tenant_runtimes["sandbox"] = TenantRuntime(
        tenant_name="sandbox",
        plugin=MagicMock(),
        storage=sandbox_backend,
        orchestrator=sandbox_orchestrator,
        config_hash=_config_hash(sandbox),
        created_at=NOW,
    )
    runner._bootstrapped = True
    try:
        with runner._claim_tenant("production") as repair_claimed:
            assert repair_claimed is True
            blocked = runner.run_tenant("production")
            assert blocked.already_running is True
            other = runner.run_tenant("sandbox")
            assert other.already_running is False
            assert other.dates_calculated == 1
            sandbox_orchestrator.run.assert_called_once()
    finally:
        runtime.close(wait=True)
        backend.dispose()
        sandbox_backend.dispose()
