from __future__ import annotations

from datetime import UTC, date, datetime
from importlib import import_module
from types import ModuleType

import pytest

from core.config.models import FocusPreviewTenantConfig, TenantConfig
from core.preview.models import PreviewDiagnostic

CREATED_AT = datetime(2026, 7, 22, 12, 30, tzinfo=UTC)
START = date(2026, 7, 1)
END = date(2026, 7, 4)


def _repair_module() -> ModuleType:
    return import_module("core.preview.repair")


def _diagnostic(code: str = "focus_preview_repair_interrupted") -> PreviewDiagnostic:
    return PreviewDiagnostic(code=code, message="retry the bounded repair", retryable=True)


def _repair_date(
    tracking_date: date,
    *,
    status: str = "queued",
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    calculation_id: str | None = None,
    calculation_completed_at: datetime | None = None,
    rows_written: int | None = None,
    failure_stage: str | None = None,
    diagnostic: PreviewDiagnostic | None = None,
) -> object:
    repair = _repair_module()
    return repair.PreviewRepairDate(
        repair_id="repair-1",
        tracking_date=tracking_date,
        status=repair.PreviewRepairDateStatus(status),
        started_at=started_at,
        completed_at=completed_at,
        calculation_id=calculation_id,
        calculation_completed_at=calculation_completed_at,
        rows_written=rows_written,
        failure_stage=(repair.PreviewRepairFailureStage(failure_stage) if failure_stage is not None else None),
        diagnostic=diagnostic,
    )


def _tenant_config(
    *,
    effective_start: date = date(2026, 1, 1),
    effective_end: date = date(2026, 12, 31),
    lookback_days: int = 200,
    cutoff_days: int = 5,
    retention_days: int = 250,
) -> TenantConfig:
    return TenantConfig(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        lookback_days=lookback_days,
        cutoff_days=cutoff_days,
        retention_days=retention_days,
        focus_preview=FocusPreviewTenantConfig(
            commercial_profile="direct_payg",
            effective_start_date=effective_start,
            effective_end_date=effective_end,
        ),
    )


def test_repair_status_vocabularies_include_durable_monthly_intermediate_state() -> None:
    repair = _repair_module()

    assert {status.value for status in repair.PreviewRepairStatus} == {
        "queued",
        "running",
        "completed",
        "completed_with_failures",
        "failed",
    }
    assert {status.value for status in repair.PreviewRepairDateStatus} == {
        "queued",
        "running",
        "daily_validated",
        "succeeded",
        "failed",
    }
    assert {stage.value for stage in repair.PreviewRepairFailureStage} == {
        "retained_state",
        "provider_source",
        "calculation",
        "evidence",
        "preview_validation",
        "worker",
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"repair_id": ""},
        {"started_at": CREATED_AT},
        {"completed_at": CREATED_AT},
        {"calculation_id": "fabricated"},
        {"rows_written": 0},
        {"failure_stage": "worker"},
        {"diagnostic": _diagnostic()},
    ],
)
def test_queued_date_rejects_identity_completion_or_failure_fields(kwargs: dict[str, object]) -> None:
    repair = _repair_module()
    values: dict[str, object] = {
        "repair_id": "repair-1",
        "tracking_date": START,
        "status": repair.PreviewRepairDateStatus.QUEUED,
        "started_at": None,
        "completed_at": None,
        "calculation_id": None,
        "calculation_completed_at": None,
        "rows_written": None,
        "failure_stage": None,
        "diagnostic": None,
    }
    values.update(kwargs)
    if values["failure_stage"] == "worker":
        values["failure_stage"] = repair.PreviewRepairFailureStage.WORKER

    with pytest.raises(ValueError):
        repair.PreviewRepairDate(**values)


def test_daily_validated_date_requires_real_calculation_fields_and_remains_nonterminal() -> None:
    item = _repair_date(
        START,
        status="daily_validated",
        started_at=CREATED_AT,
        calculation_id="calculation-1",
        calculation_completed_at=CREATED_AT,
        rows_written=0,
    )

    assert item.status.value == "daily_validated"  # type: ignore[attr-defined]
    assert item.completed_at is None  # type: ignore[attr-defined]
    assert item.calculation_id == "calculation-1"  # type: ignore[attr-defined]
    assert item.rows_written == 0  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "changes",
    [
        {"calculation_id": None},
        {"calculation_completed_at": None},
        {"rows_written": None},
        {"rows_written": -1},
        {"completed_at": CREATED_AT},
        {"failure_stage": "worker", "diagnostic": _diagnostic()},
    ],
)
def test_daily_validated_date_rejects_missing_result_terminal_or_failure_fields(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "status": "daily_validated",
        "started_at": CREATED_AT,
        "calculation_id": "calculation-1",
        "calculation_completed_at": CREATED_AT,
        "rows_written": 1,
    }
    values.update(changes)

    with pytest.raises(ValueError):
        _repair_date(START, **values)  # type: ignore[arg-type]


def test_failed_date_never_claims_a_calculation_result() -> None:
    item = _repair_date(
        START,
        status="failed",
        started_at=CREATED_AT,
        completed_at=CREATED_AT,
        failure_stage="provider_source",
        diagnostic=_diagnostic("focus_preview_repair_provider_history_unavailable"),
    )

    assert item.status.value == "failed"  # type: ignore[attr-defined]
    assert item.calculation_id is None  # type: ignore[attr-defined]
    assert item.calculation_completed_at is None  # type: ignore[attr-defined]
    assert item.rows_written is None  # type: ignore[attr-defined]


def test_repair_expands_exact_half_open_date_set_in_ascending_order() -> None:
    repair = _repair_module()
    items = tuple(_repair_date(day) for day in (START, date(2026, 7, 2), date(2026, 7, 3)))
    operation = repair.PreviewRepair(
        repair_id="repair-1",
        tenant_name="production",
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        start_date=START,
        end_date=END,
        status=repair.PreviewRepairStatus.QUEUED,
        created_at=CREATED_AT,
        started_at=None,
        completed_at=None,
        diagnostic=None,
        dates=items,
    )

    assert tuple(item.tracking_date for item in operation.dates) == (
        START,
        date(2026, 7, 2),
        date(2026, 7, 3),
    )


@pytest.mark.parametrize(
    "dates",
    [
        (START, date(2026, 7, 3)),
        (START, date(2026, 7, 2), date(2026, 7, 2)),
        (date(2026, 7, 2), START, date(2026, 7, 3)),
    ],
)
def test_repair_rejects_missing_duplicate_or_unsorted_date_sets(dates: tuple[date, ...]) -> None:
    repair = _repair_module()

    with pytest.raises(ValueError):
        repair.PreviewRepair(
            repair_id="repair-1",
            tenant_name="production",
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            start_date=START,
            end_date=END,
            status=repair.PreviewRepairStatus.QUEUED,
            created_at=CREATED_AT,
            started_at=None,
            completed_at=None,
            diagnostic=None,
            dates=tuple(_repair_date(day) for day in dates),
        )


@pytest.mark.parametrize(
    ("status", "started_at", "completed_at", "diagnostic"),
    [
        ("queued", CREATED_AT, None, None),
        ("running", None, None, None),
        ("running", CREATED_AT, CREATED_AT, None),
        ("completed", CREATED_AT, None, None),
        ("completed_with_failures", CREATED_AT, CREATED_AT, None),
        ("failed", CREATED_AT, CREATED_AT, None),
    ],
)
def test_operation_status_requires_exact_lifecycle_fields(
    status: str,
    started_at: datetime | None,
    completed_at: datetime | None,
    diagnostic: PreviewDiagnostic | None,
) -> None:
    repair = _repair_module()

    with pytest.raises(ValueError):
        repair.PreviewRepair(
            repair_id="repair-1",
            tenant_name="production",
            ecosystem="confluent_cloud",
            tenant_id="tenant-1",
            start_date=START,
            end_date=date(2026, 7, 2),
            status=repair.PreviewRepairStatus(status),
            created_at=CREATED_AT,
            started_at=started_at,
            completed_at=completed_at,
            diagnostic=diagnostic,
            dates=(_repair_date(START),),
        )


def test_repair_policy_intersects_effective_lookback_cutoff_and_complete_retention() -> None:
    repair = _repair_module()
    tenant = _tenant_config(
        effective_start=date(2025, 12, 1),
        effective_end=date(2026, 12, 31),
        lookback_days=200,
        cutoff_days=5,
        retention_days=180,
    )

    policy = repair.repair_policy_from_tenant_config(tenant, created_at=CREATED_AT)

    # Retention cutoff is 2026-01-23 12:30 UTC, so 2026-01-24 is the
    # first completely retained date. Cutoff excludes 2026-07-17 onward.
    assert policy.eligible_start_date == date(2026, 1, 24)
    assert policy.eligible_end_date == date(2026, 7, 17)


def test_repair_policy_uses_utc_for_retention_ceiling() -> None:
    repair = _repair_module()
    tenant = _tenant_config(
        effective_start=date(2026, 7, 1),
        effective_end=date(2026, 7, 31),
        lookback_days=20,
        cutoff_days=2,
        retention_days=10,
    )

    midnight = repair.repair_policy_from_tenant_config(
        tenant,
        created_at=datetime(2026, 7, 22, 0, 0, tzinfo=UTC),
    )
    partial = repair.repair_policy_from_tenant_config(
        tenant,
        created_at=datetime(2026, 7, 22, 0, 0, 1, tzinfo=UTC),
    )

    assert midnight.eligible_start_date == date(2026, 7, 12)
    assert partial.eligible_start_date == date(2026, 7, 13)


def test_repair_policy_rejects_disabled_or_unsupported_tenants() -> None:
    repair = _repair_module()
    disabled = _tenant_config()
    disabled.focus_preview = None
    unsupported = _tenant_config()
    unsupported.ecosystem = "self_managed_kafka"

    with pytest.raises(ValueError):
        repair.repair_policy_from_tenant_config(disabled, created_at=CREATED_AT)
    with pytest.raises(ValueError):
        repair.repair_policy_from_tenant_config(unsupported, created_at=CREATED_AT)
