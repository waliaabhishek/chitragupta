from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path

import pytest
from sqlalchemy import text

from core.preview.models import PreviewDiagnostic
from core.preview.storage_availability import PreviewEvidenceSchemaError
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
        changed = uow.repairs.fail_interrupted_for_owner(
            "confluent_cloud",
            "tenant-1",
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


def test_same_second_repair_restart_recovers_owner_and_allows_replacement(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'same-second-restart.db'}"
    first = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    first.create_tables()
    _create(first, _queued(repair_id="repair-terminal"))
    with first.create_preview_evidence_unit_of_work() as uow:
        terminal = uow.repairs.fail_queued_before_execution(
            "repair-terminal",
            completed_at=NOW,
            diagnostic=_diagnostic("already_terminal"),
        )
        uow.commit()
    assert terminal is not None and terminal.status.value == "failed"
    _create(first, _queued(repair_id="repair-interrupted"))
    _create(
        first,
        _queued(
            repair_id="repair-foreign",
            tenant_name="sandbox",
            tenant_id="tenant-2",
        ),
    )
    first.dispose()

    reopened = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    reopened.create_tables()
    with reopened.create_preview_evidence_unit_of_work() as uow:
        changed = uow.repairs.fail_interrupted_for_owner(
            "confluent_cloud",
            "tenant-1",
            completed_at=NOW,
            diagnostic=_diagnostic("focus_preview_repair_interrupted"),
        )
        uow.commit()
    with reopened.create_preview_generation_read_unit_of_work() as uow:
        interrupted = uow.repairs.get_for_owner(
            "repair-interrupted",
            "confluent_cloud",
            "tenant-1",
        )
        foreign = uow.repairs.get_for_owner(
            "repair-foreign",
            "confluent_cloud",
            "tenant-2",
        )
        terminal_after = uow.repairs.get_for_owner(
            "repair-terminal",
            "confluent_cloud",
            "tenant-1",
        )
    replacement = _create(
        reopened,
        _queued(repair_id="repair-replacement"),
    )

    assert changed == 1
    assert interrupted is not None
    assert interrupted.status.value == "failed"
    assert interrupted.completed_at == NOW
    assert {item.status.value for item in interrupted.dates} == {"failed"}
    assert foreign is not None and foreign.status.value == "queued"
    assert terminal_after == terminal
    assert replacement.status.value == "queued"  # type: ignore[attr-defined]
    reopened.dispose()


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


def _progress(backend: object, tenant_id: str = "tenant-1") -> object:
    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        return uow.repairs.get_current_progress_for_owner(
            "confluent_cloud",
            tenant_id,
        )


def _set_repair_rows(
    backend: object,
    *,
    repair_id: str,
    parent_status: str,
    date_statuses: tuple[str, ...],
) -> None:
    engine = backend._engine  # type: ignore[attr-defined]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repairs
                SET status = :status,
                    started_at = CASE WHEN :status = 'queued' THEN NULL ELSE :now END,
                    completed_at = CASE
                        WHEN :status IN ('completed', 'completed_with_failures', 'failed')
                        THEN :now ELSE NULL END,
                    diagnostic_code = CASE WHEN :status = 'failed' THEN 'repair_failed' ELSE NULL END,
                    diagnostic_message = CASE WHEN :status = 'failed' THEN 'retry repair' ELSE NULL END,
                    diagnostic_retryable = CASE WHEN :status = 'failed' THEN 1 ELSE NULL END
                WHERE repair_id = :repair_id
                """
            ),
            {
                "repair_id": repair_id,
                "status": parent_status,
                "now": NOW,
            },
        )
        dates = tuple(
            connection.execute(
                text(
                    """
                    SELECT tracking_date
                    FROM ccloud_focus_preview_repair_dates
                    WHERE repair_id = :repair_id
                    ORDER BY tracking_date
                    """
                ),
                {"repair_id": repair_id},
            ).scalars()
        )
        for tracking_date, status in zip(dates, date_statuses, strict=True):
            connection.execute(
                text(
                    """
                    UPDATE ccloud_focus_preview_repair_dates
                    SET status = :status
                    WHERE repair_id = :repair_id AND tracking_date = :tracking_date
                    """
                ),
                {
                    "repair_id": repair_id,
                    "tracking_date": tracking_date,
                    "status": status,
                },
            )


def test_current_progress_is_absent_without_head_or_history(backend: object) -> None:
    assert _progress(backend) is None


def test_repair_history_without_head_fails_closed(backend: object) -> None:
    _create(backend, _queued())
    with backend._engine.begin() as connection:  # type: ignore[attr-defined]
        connection.execute(
            text(
                """
                DELETE FROM ccloud_focus_preview_repair_heads
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                """
            )
        )

    with pytest.raises(PreviewEvidenceSchemaError, match="head"):
        _progress(backend)


def test_null_head_reports_unresolved_history(backend: object) -> None:
    repair = import_module("core.preview.repair")
    _create(backend, _queued())
    with backend._engine.begin() as connection:  # type: ignore[attr-defined]
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repair_heads
                SET repair_id = NULL
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                """
            )
        )

    assert isinstance(_progress(backend), repair.PreviewRepairHistoryUnresolved)


@pytest.mark.parametrize(
    ("parent_status", "date_statuses", "completed"),
    [
        ("queued", ("queued", "queued"), 0),
        ("running", ("succeeded", "daily_validated"), 1),
        ("running", ("failed", "running"), 1),
        ("completed", ("succeeded", "succeeded"), 2),
        ("completed_with_failures", ("succeeded", "failed"), 2),
        ("failed", ("failed", "failed"), 2),
    ],
)
def test_current_progress_counts_only_durable_terminal_dates(
    backend: object,
    parent_status: str,
    date_statuses: tuple[str, ...],
    completed: int,
) -> None:
    _create(backend, _queued())
    _set_repair_rows(
        backend,
        repair_id="repair-1",
        parent_status=parent_status,
        date_statuses=date_statuses,
    )

    progress = _progress(backend)

    assert progress.status.value == parent_status  # type: ignore[union-attr]
    assert progress.completed_dates == completed  # type: ignore[union-attr]
    assert progress.total_dates == 2  # type: ignore[union-attr]


def test_new_same_second_retry_replaces_current_head_without_timestamp_tiebreak(
    backend: object,
) -> None:
    _create(backend, _queued(repair_id="repair-failed"))
    _set_repair_rows(
        backend,
        repair_id="repair-failed",
        parent_status="failed",
        date_statuses=("failed", "failed"),
    )
    _create(backend, _queued(repair_id="repair-retry"))
    _set_repair_rows(
        backend,
        repair_id="repair-retry",
        parent_status="completed",
        date_statuses=("succeeded", "succeeded"),
    )

    progress = _progress(backend)
    with backend._engine.connect() as connection:  # type: ignore[attr-defined]
        head = connection.execute(
            text(
                """
                SELECT repair_id
                FROM ccloud_focus_preview_repair_heads
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                """
            )
        ).scalar_one()

    assert head == "repair-retry"
    assert progress.status.value == "completed"  # type: ignore[union-attr]
    assert progress.completed_dates == progress.total_dates == 2  # type: ignore[union-attr]


def test_new_explicit_repair_resolves_null_head(backend: object) -> None:
    _create(backend, _queued(repair_id="repair-old"))
    _set_repair_rows(
        backend,
        repair_id="repair-old",
        parent_status="failed",
        date_statuses=("failed", "failed"),
    )
    with backend._engine.begin() as connection:  # type: ignore[attr-defined]
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repair_heads
                SET repair_id = NULL
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                """
            )
        )

    _create(backend, _queued(repair_id="repair-new"))

    progress = _progress(backend)
    assert progress.status.value == "queued"  # type: ignore[union-attr]
    assert progress.completed_dates == 0  # type: ignore[union-attr]
    assert progress.total_dates == 2  # type: ignore[union-attr]


def test_current_head_and_progress_are_owner_isolated(backend: object) -> None:
    _create(backend, _queued(repair_id="repair-1"))
    _create(
        backend,
        _queued(
            repair_id="repair-2",
            tenant_name="sandbox",
            tenant_id="tenant-2",
        ),
    )
    _set_repair_rows(
        backend,
        repair_id="repair-1",
        parent_status="failed",
        date_statuses=("failed", "failed"),
    )

    first = _progress(backend, "tenant-1")
    second = _progress(backend, "tenant-2")

    assert first.status.value == "failed"  # type: ignore[union-attr]
    assert second.status.value == "queued"  # type: ignore[union-attr]


def test_owner_mismatched_head_fails_closed(backend: object) -> None:
    _create(backend, _queued(repair_id="repair-1"))
    _create(
        backend,
        _queued(
            repair_id="repair-2",
            tenant_name="sandbox",
            tenant_id="tenant-2",
        ),
    )
    with backend._engine.begin() as connection:  # type: ignore[attr-defined]
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repair_heads
                SET repair_id = NULL
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-2'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE ccloud_focus_preview_repair_heads
                SET repair_id = 'repair-2'
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                """
            )
        )

    with pytest.raises(PreviewEvidenceSchemaError, match="owner"):
        _progress(backend, "tenant-1")


def test_progress_aggregate_does_not_materialize_repair_dates(
    backend: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _create(backend, _queued())
    with backend.create_preview_generation_read_unit_of_work() as uow:  # type: ignore[attr-defined]
        monkeypatch.setattr(
            uow.repairs,
            "_get",
            lambda *_args, **_kwargs: pytest.fail("progress must use an aggregate query"),
        )

        progress = uow.repairs.get_current_progress_for_owner(
            "confluent_cloud",
            "tenant-1",
        )

    assert progress.completed_dates == 0
    assert progress.total_dates == 2


def test_head_write_failure_rolls_back_parent_dates_and_head(
    backend: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemy.exc import IntegrityError

    with backend.create_preview_evidence_unit_of_work() as uow:  # type: ignore[attr-defined]
        session = uow.repairs._session

        def fail_flush() -> None:
            raise IntegrityError("head constraint", params=None, orig=RuntimeError("forced"))

        monkeypatch.setattr(session, "flush", fail_flush)
        with pytest.raises(IntegrityError):
            uow.repairs.create_queued(_queued())

    with backend._engine.connect() as connection:  # type: ignore[attr-defined]
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repairs")).scalar_one() == 0
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_dates")).scalar_one() == 0
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_focus_preview_repair_heads")).scalar_one() == 0
