from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path

import pytest

from core.preview.models import PreviewDiagnostic
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)


@pytest.fixture
def backend(tmp_path: Path):
    value = SQLModelBackend(
        f"sqlite:///{tmp_path / 'repairs.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    value.create_tables()
    yield value
    value.dispose()


def _diagnostic(code: str) -> PreviewDiagnostic:
    return PreviewDiagnostic(code=code, message=f"sanitized {code}", retryable=True)


def _queued(
    *,
    repair_id: str = "repair-1",
    tenant_name: str = "production",
    tenant_id: str = "tenant-1",
    start: date = date(2026, 7, 1),
    end: date = date(2026, 7, 3),
) -> object:
    repair = import_module("core.preview.repair")
    days = tuple(start + timedelta(days=offset) for offset in range((end - start).days))
    return repair.PreviewRepair(
        repair_id=repair_id,
        tenant_name=tenant_name,
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        start_date=start,
        end_date=end,
        status=repair.PreviewRepairStatus.QUEUED,
        created_at=NOW,
        started_at=None,
        completed_at=None,
        diagnostic=None,
        dates=tuple(
            repair.PreviewRepairDate(
                repair_id=repair_id,
                tracking_date=tracking_date,
                status=repair.PreviewRepairDateStatus.QUEUED,
                started_at=None,
                completed_at=None,
                calculation_id=None,
                calculation_completed_at=None,
                rows_written=None,
                failure_stage=None,
                diagnostic=None,
            )
            for tracking_date in days
        ),
    )


def _create(backend: object, operation: object) -> object:
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        created = uow.repairs.create_queued(operation)
        uow.commit()
        return created


def _get(backend: object, repair_id: str = "repair-1") -> object | None:
    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        return uow.repairs.get_for_owner(repair_id, "confluent_cloud", "tenant-1")


def test_interruption_recovery_is_exactly_owner_scoped(backend: object) -> None:
    _create(backend, _queued(repair_id="repair-1"))
    _create(
        backend,
        _queued(
            repair_id="repair-2",
            tenant_name="sandbox",
            tenant_id="tenant-2",
        ),
    )

    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        changed = uow.repairs.fail_interrupted_before(
            "confluent_cloud",
            "tenant-1",
            NOW + timedelta(seconds=1),
            completed_at=NOW + timedelta(seconds=2),
            diagnostic=_diagnostic("focus_preview_repair_interrupted"),
        )
        uow.commit()

    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        recovered = uow.repairs.get_for_owner(
            "repair-1",
            "confluent_cloud",
            "tenant-1",
        )
        untouched = uow.repairs.get_for_owner(
            "repair-2",
            "confluent_cloud",
            "tenant-2",
        )

    assert changed == 1
    assert recovered is not None and recovered.status.value == "failed"
    assert untouched is not None and untouched.status.value == "queued"


def test_queued_operation_and_complete_date_set_are_durable_and_owner_scoped(
    backend: object,
) -> None:
    operation = _queued()

    created = _create(backend, operation)
    loaded = _get(backend)
    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        foreign = uow.repairs.get_for_owner("repair-1", "confluent_cloud", "tenant-2")

    assert created == operation
    assert loaded == operation
    assert foreign is None


def test_only_one_queued_or_running_repair_is_active_for_an_owner(backend: object) -> None:
    _create(backend, _queued(repair_id="repair-1"))

    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        active = uow.repairs.find_active_for_owner("confluent_cloud", "tenant-1")

    assert active is not None
    assert active.repair_id == "repair-1"  # type: ignore[attr-defined]


def test_operation_and_partial_month_date_follow_exact_guarded_success_path(
    backend: object,
) -> None:
    repair = import_module("core.preview.repair")
    _create(
        backend,
        _queued(start=date(2026, 7, 2), end=date(2026, 7, 3)),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        assert uow.repairs.mark_running("repair-1", started_at=NOW) is not None
        assert uow.repairs.mark_date_running("repair-1", date(2026, 7, 2), started_at=NOW) is not None
        result = uow.repairs.mark_date_succeeded_from_running(
            "repair-1",
            date(2026, 7, 2),
            completed_at=NOW + timedelta(seconds=2),
            calculation_id="calculation-1",
            calculation_completed_at=NOW + timedelta(seconds=1),
            rows_written=2,
        )
        completed = uow.repairs.finalize_completed(
            "repair-1",
            completed_at=NOW + timedelta(seconds=3),
        )
        uow.commit()

    assert result is not None
    assert result.status is repair.PreviewRepairDateStatus.SUCCEEDED
    assert completed is not None
    assert completed.status is repair.PreviewRepairStatus.COMPLETED


def test_terminal_date_and_operation_transitions_are_immutable(backend: object) -> None:
    _create(
        backend,
        _queued(start=date(2026, 7, 2), end=date(2026, 7, 3)),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.repairs.mark_running("repair-1", started_at=NOW)
        uow.repairs.mark_date_running("repair-1", date(2026, 7, 2), started_at=NOW)
        uow.repairs.mark_date_succeeded_from_running(
            "repair-1",
            date(2026, 7, 2),
            completed_at=NOW + timedelta(seconds=2),
            calculation_id="calculation-1",
            calculation_completed_at=NOW + timedelta(seconds=1),
            rows_written=1,
        )
        uow.repairs.finalize_completed("repair-1", completed_at=NOW + timedelta(seconds=3))
        uow.commit()
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        assert uow.repairs.mark_running("repair-1", started_at=NOW) is None
        assert uow.repairs.mark_date_running("repair-1", date(2026, 7, 2), started_at=NOW) is None
        assert (
            uow.repairs.mark_date_failed_from_running(
                "repair-1",
                date(2026, 7, 2),
                completed_at=NOW + timedelta(seconds=4),
                stage=import_module("core.preview.repair").PreviewRepairFailureStage.WORKER,
                diagnostic=_diagnostic("focus_preview_repair_interrupted"),
            )
            is None
        )

    assert _get(backend).status.value == "completed"  # type: ignore[union-attr]
    assert _get(backend).dates[0].status.value == "succeeded"  # type: ignore[union-attr]


def test_normal_finalization_rejects_any_nonterminal_child(backend: object) -> None:
    _create(backend, _queued())
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.repairs.mark_running("repair-1", started_at=NOW)
        uow.repairs.mark_date_running("repair-1", date(2026, 7, 1), started_at=NOW)

        assert (
            uow.repairs.finalize_completed(
                "repair-1",
                completed_at=NOW + timedelta(seconds=2),
            )
            is None
        )
        assert (
            uow.repairs.finalize_completed_with_failures(
                "repair-1",
                completed_at=NOW + timedelta(seconds=2),
            )
            is None
        )


def test_monthly_compare_and_set_updates_every_expected_row_or_none(backend: object) -> None:
    repair = import_module("core.preview.repair")
    _create(
        backend,
        _queued(start=date(2026, 7, 1), end=date(2026, 8, 1)),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.repairs.mark_running("repair-1", started_at=NOW)
        for day in (date(2026, 7, 1), date(2026, 7, 2)):
            uow.repairs.mark_date_running("repair-1", day, started_at=NOW)
        uow.repairs.mark_date_daily_validated(
            "repair-1",
            date(2026, 7, 1),
            calculation_id="calculation-1",
            calculation_completed_at=NOW,
            rows_written=1,
        )
        uow.commit()

    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        result = uow.repairs.finalize_month_dates(
            "repair-1",
            (date(2026, 7, 1), date(2026, 7, 2)),
            terminal_status=repair.PreviewRepairDateStatus.SUCCEEDED,
            completed_at=NOW + timedelta(seconds=1),
            stage=None,
            diagnostic=None,
        )
        uow.commit()

    loaded = _get(backend)
    assert result is None
    assert loaded.dates[0].status.value == "daily_validated"  # type: ignore[union-attr]
    assert loaded.dates[1].status.value == "running"  # type: ignore[union-attr]


def test_queued_scheduling_failure_atomically_fails_parent_and_every_date(
    backend: object,
) -> None:
    _create(backend, _queued())
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        failed = uow.repairs.fail_queued_before_execution(
            "repair-1",
            completed_at=NOW + timedelta(seconds=1),
            diagnostic=_diagnostic("focus_preview_repair_worker_unavailable"),
        )
        uow.commit()

    assert failed.status.value == "failed"  # type: ignore[union-attr]
    assert [item.status.value for item in failed.dates] == ["failed", "failed"]  # type: ignore[union-attr]
    assert all(item.failure_stage.value == "worker" for item in failed.dates)  # type: ignore[union-attr]


def test_running_recovery_preserves_terminal_dates_and_fails_all_nonterminal_dates(
    backend: object,
) -> None:
    repair = import_module("core.preview.repair")
    _create(
        backend,
        _queued(start=date(2026, 7, 1), end=date(2026, 8, 1)),
    )
    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.repairs.mark_running("repair-1", started_at=NOW)
        for day in (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)):
            uow.repairs.mark_date_running("repair-1", day, started_at=NOW)
        uow.repairs.mark_date_succeeded_from_running(
            "repair-1",
            date(2026, 7, 1),
            completed_at=NOW + timedelta(seconds=1),
            calculation_id="calculation-1",
            calculation_completed_at=NOW,
            rows_written=1,
        )
        uow.repairs.mark_date_failed_from_running(
            "repair-1",
            date(2026, 7, 2),
            completed_at=NOW + timedelta(seconds=1),
            stage=repair.PreviewRepairFailureStage.PROVIDER_SOURCE,
            diagnostic=_diagnostic("focus_preview_repair_provider_history_unavailable"),
        )
        uow.repairs.mark_date_daily_validated(
            "repair-1",
            date(2026, 7, 3),
            calculation_id="calculation-3",
            calculation_completed_at=NOW,
            rows_written=1,
        )
        uow.commit()

    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        recovered = uow.repairs.fail_running_worker(
            "repair-1",
            completed_at=NOW + timedelta(seconds=2),
            diagnostic=_diagnostic("focus_preview_repair_interrupted"),
        )
        uow.commit()

    statuses = {item.tracking_date: item.status.value for item in recovered.dates}  # type: ignore[union-attr]
    assert recovered.status.value == "failed"  # type: ignore[union-attr]
    assert statuses[date(2026, 7, 1)] == "succeeded"
    assert statuses[date(2026, 7, 2)] == "failed"
    assert statuses[date(2026, 7, 3)] == "failed"
    assert all(status == "failed" for day, status in statuses.items() if day >= date(2026, 7, 3))
