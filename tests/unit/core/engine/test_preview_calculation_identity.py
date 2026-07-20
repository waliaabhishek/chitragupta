from __future__ import annotations

import inspect
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest

from core.config.models import TenantConfig
from core.engine.allocation import AllocatorRegistry
from core.engine.orchestrator import CalculatePhase, ChargebackOrchestrator, PipelineRunResult
from core.models.pipeline import PipelineRun
from core.plugin.registry import EcosystemBundle
from core.storage.interface import BillingRepository, ChargebackRepository, PipelineStateRepository, UnitOfWork
from workflow_runner import TenantRuntime, WorkflowRunner, _config_hash


def _phase(
    *,
    calculation_id_factory: object,
    calculation_clock: object,
) -> CalculatePhase:
    return CalculatePhase(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        bundle=MagicMock(spec=EcosystemBundle),
        retry_checker=MagicMock(),
        metrics_source=None,
        allocator_registry=AllocatorRegistry(),
        identity_overrides={},
        allocator_params={},
        metrics_step=timedelta(hours=1),
        calculation_id_factory=calculation_id_factory,
        calculation_clock=calculation_clock,
    )


def _empty_uow() -> tuple[MagicMock, MagicMock, MagicMock]:
    uow = MagicMock(spec=UnitOfWork)
    billing = MagicMock(spec=BillingRepository)
    pipeline_state = MagicMock(spec=PipelineStateRepository)
    billing.find_by_date.return_value = []
    uow.billing = billing
    uow.chargebacks = MagicMock(spec=ChargebackRepository)
    uow.pipeline_state = pipeline_state
    return uow, billing, pipeline_state


def test_calculation_signatures_preserve_old_calls_and_add_keyword_only_seams() -> None:
    from core.engine import orchestrator

    init = inspect.signature(CalculatePhase.__init__)
    run = inspect.signature(CalculatePhase.run)
    wrapper = inspect.signature(ChargebackOrchestrator._calculate_date)
    orchestrator_run = inspect.signature(ChargebackOrchestrator.run)

    assert list(init.parameters)[-2:] == ["calculation_id_factory", "calculation_clock"]
    assert init.parameters["calculation_id_factory"].kind is inspect.Parameter.KEYWORD_ONLY
    assert init.parameters["calculation_id_factory"].default is orchestrator._new_calculation_id
    assert init.parameters["calculation_clock"].kind is inspect.Parameter.KEYWORD_ONLY
    assert init.parameters["calculation_clock"].default is orchestrator._calculation_utc_now
    assert run.parameters["calculation_run_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert run.parameters["calculation_run_id"].default is None
    assert wrapper.parameters["calculation_run_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert wrapper.parameters["calculation_run_id"].default is None
    assert orchestrator_run.parameters["calculation_run_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert orchestrator_run.parameters["calculation_run_id"].default is None
    assert run.return_annotation in (int, "int")
    assert wrapper.return_annotation in (int, "int")
    assert orchestrator_run.return_annotation in (PipelineRunResult, "PipelineRunResult")


def test_calculate_phase_marks_zero_row_success_with_deterministic_identity_time_and_provenance() -> None:
    completed_at = datetime(2026, 7, 3, 2, tzinfo=UTC)
    id_factory = MagicMock(return_value="calculation-1")
    clock = MagicMock(return_value=completed_at)
    phase = _phase(calculation_id_factory=id_factory, calculation_clock=clock)
    uow, billing, pipeline_state = _empty_uow()

    rows = phase.run(uow, date(2026, 7, 1), calculation_run_id=17)

    assert rows == 0
    billing.find_by_date.assert_called_once_with("confluent_cloud", "tenant-1", date(2026, 7, 1))
    pipeline_state.mark_chargeback_calculated.assert_called_once_with(
        "confluent_cloud",
        "tenant-1",
        date(2026, 7, 1),
        calculation_id="calculation-1",
        calculation_completed_at=completed_at,
        calculation_run_id=17,
    )
    id_factory.assert_called_once_with()
    clock.assert_called_once_with()


@pytest.mark.parametrize(
    ("calculation_id", "completed_at"),
    [
        ("", datetime(2026, 7, 3, 2, tzinfo=UTC)),
        ("calculation-1", datetime(2026, 7, 3, 2)),
    ],
)
def test_calculate_phase_rejects_invalid_success_metadata_before_marking(
    calculation_id: str,
    completed_at: datetime,
) -> None:
    phase = _phase(
        calculation_id_factory=MagicMock(return_value=calculation_id),
        calculation_clock=MagicMock(return_value=completed_at),
    )
    uow, _billing, pipeline_state = _empty_uow()

    with pytest.raises(ValueError):
        phase.run(uow, date(2026, 7, 1))

    pipeline_state.mark_chargeback_calculated.assert_not_called()


def test_backward_compatible_calculate_wrapper_preserves_default_call_shape_and_forwards_provenance() -> None:
    orchestrator = ChargebackOrchestrator.__new__(ChargebackOrchestrator)
    orchestrator._calculate_phase = MagicMock()
    uow = MagicMock(spec=UnitOfWork)

    orchestrator._calculate_date(uow, date(2026, 7, 1))
    orchestrator._calculate_date(uow, date(2026, 7, 2), calculation_run_id=17)

    assert orchestrator._calculate_phase.run.call_args_list == [
        call(uow, date(2026, 7, 1)),
        call(uow, date(2026, 7, 2), calculation_run_id=17),
    ]


def test_workflow_runner_forwards_persisted_run_id_only_as_provenance() -> None:
    settings = MagicMock()
    settings.tenants = {}
    runner = WorkflowRunner(settings, MagicMock())
    tenant_config = TenantConfig(ecosystem="confluent_cloud", tenant_id="tenant-1")
    orchestrator = MagicMock()
    result = PipelineRunResult(
        tenant_name="production",
        tenant_id="tenant-1",
        dates_gathered=1,
        dates_calculated=1,
        chargeback_rows_written=1,
        dates_pending_calculation=0,
        errors=[],
    )
    orchestrator.run.return_value = result
    runtime = TenantRuntime(
        tenant_name="production",
        plugin=MagicMock(),
        storage=MagicMock(),
        orchestrator=orchestrator,
        config_hash=_config_hash(tenant_config),
        created_at=datetime(2026, 7, 3, tzinfo=UTC),
    )
    runner._bootstrapped = True
    runner._tenant_runtimes["production"] = runtime
    pipeline_run = PipelineRun(
        id=17,
        tenant_name="production",
        started_at=datetime(2026, 7, 3, tzinfo=UTC),
        status="running",
    )

    with (
        patch("workflow_runner.PipelineRunTracker.create", return_value=pipeline_run),
        patch("workflow_runner.PipelineRunTracker.make_progress_callback", return_value=MagicMock()),
        patch("workflow_runner.PipelineRunTracker.finalize") as finalize,
    ):
        returned = runner._run_tenant("production", tenant_config)

    assert returned is result
    orchestrator.run.assert_called_once_with(calculation_run_id=17)
    finalize.assert_called_once_with(pipeline_run, result)
