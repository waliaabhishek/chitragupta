from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from importlib import import_module
from typing import Any

import pytest

SUMMARY_COLUMNS = (
    "AllocatedResourceId",
    "AllocatedResourceName",
    "AllocatedTags",
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "ChargeCategory",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "EffectiveCost",
    "ResourceId",
    "ResourceName",
    "ServiceCategory",
    "ServiceName",
    "SubAccountId",
    "SubAccountName",
    "Tags",
)


def _request_module() -> Any:
    return import_module("core.preview.request")


def _models() -> Any:
    return import_module("core.preview.models")


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _queued_request(**overrides: object) -> Any:
    models = _models()
    values: dict[str, object] = {
        "request_id": "request-1",
        "tenant_name": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "grain": "daily",
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 2),
        "column_profile": "full",
        "effective_columns": _mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        "status": models.PreviewRequestStatus.QUEUED,
        "created_at": datetime(2026, 7, 3, tzinfo=UTC),
        "started_at": None,
        "completed_at": None,
        "source_snapshot": None,
        "diagnostic": None,
        "storage_key": None,
        "package": None,
    }
    values.update(overrides)
    return models.PreviewRequest(**values)


@pytest.mark.parametrize(
    ("month", "start", "end"),
    [
        ("2024-02", date(2024, 2, 1), date(2024, 3, 1)),
        ("2026-11", date(2026, 11, 1), date(2026, 12, 1)),
        ("2026-12", date(2026, 12, 1), date(2027, 1, 1)),
        ("9999-11", date(9999, 11, 1), date(9999, 12, 1)),
    ],
)
def test_monthly_interval_accepts_exact_ascii_representable_months(
    month: str,
    start: date,
    end: date,
) -> None:
    interval = _request_module().canonicalize_monthly_interval(month=month)

    assert (interval.grain, interval.start_date, interval.end_date) == ("monthly", start, end)


@pytest.mark.parametrize(
    "month",
    [
        "0000-01",
        "2026-1",
        "2026-00",
        "2026-13",
        "9999-12",
        "２０２６-０７",
        "2026-07-01",
        " 2026-07",
    ],
)
def test_monthly_interval_rejects_noncanonical_or_unrepresentable_months(month: str) -> None:
    request = _request_module()

    with pytest.raises(request.PreviewRequestValidationError) as exc_info:
        request.canonicalize_monthly_interval(month=month)

    assert exc_info.value.detail == "month must use YYYY-MM"


def test_shared_month_boundary_and_derived_month_have_one_contract() -> None:
    models = _models()

    assert models.canonical_next_month_boundary(date(2024, 2, 1)) == date(2024, 3, 1)
    assert models.canonical_next_month_boundary(date(2026, 12, 1)) == date(2027, 1, 1)
    assert (
        models.preview_month(
            grain="monthly",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
        )
        == "2026-07"
    )
    assert (
        models.preview_month(
            grain="daily",
            start_date=date(2026, 7, 2),
            end_date=date(2026, 7, 3),
        )
        is None
    )
    with pytest.raises(ValueError):
        models.canonical_next_month_boundary(date(2026, 7, 2))


def test_every_month_bound_consumer_delegates_to_the_single_boundary_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models = _models()
    request = _request_module()
    monthly = import_module("core.preview.monthly")
    model_calls: list[date] = []
    request_calls: list[date] = []
    monthly_calls: list[date] = []
    original = models.canonical_next_month_boundary

    def model_boundary(value: date) -> date:
        model_calls.append(value)
        return original(value)

    def request_boundary(value: date) -> date:
        request_calls.append(value)
        return original(value)

    def monthly_boundary(value: date) -> date:
        monthly_calls.append(value)
        return original(value)

    monkeypatch.setattr(models, "canonical_next_month_boundary", model_boundary)
    assert models.preview_month(grain="monthly", start_date=date(2026, 7, 1), end_date=date(2026, 8, 1)) == "2026-07"
    models.resolve_monthly_evidence(
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        submitted_at=datetime(2026, 7, 15, tzinfo=UTC),
        availability_cutoff_end_date=date(2026, 7, 14),
    )
    assert model_calls == [date(2026, 7, 1), date(2026, 7, 1)]

    monkeypatch.setattr(request, "canonical_next_month_boundary", request_boundary)
    request.canonicalize_monthly_interval(month="2026-07")
    request.canonicalize_daily_interval(start_date=date(2026, 7, 31), end_date=date(2026, 8, 1))
    assert request_calls == [date(2026, 7, 1), date(2026, 7, 1)]

    monkeypatch.setattr(monthly, "canonical_next_month_boundary", monthly_boundary)
    assert (
        monthly.aggregate_monthly_full_rows(
            rows=(),
            month_start=datetime(2026, 7, 1, tzinfo=UTC),
            month_end=datetime(2026, 8, 1, tzinfo=UTC),
        )
        == ()
    )
    assert monthly_calls == [date(2026, 7, 1)]


@pytest.mark.parametrize(
    ("start", "end", "detail"),
    [
        (date(2026, 7, 2), date(2026, 7, 2), "start_date must be before end_date"),
        (date(2026, 7, 2), date(2026, 7, 1), "start_date must be before end_date"),
        (
            date(2026, 7, 31),
            date(2026, 8, 2),
            "Daily preview range must stay within one UTC calendar month",
        ),
    ],
)
def test_daily_interval_retains_exact_error_contract(start: date, end: date, detail: str) -> None:
    request = _request_module()

    with pytest.raises(request.PreviewRequestValidationError) as exc_info:
        request.canonicalize_daily_interval(start_date=start, end_date=end)

    assert exc_info.value.detail == detail


def test_daily_interval_allows_exclusive_first_day_of_next_month() -> None:
    interval = _request_module().canonicalize_daily_interval(
        start_date=date(2026, 7, 31),
        end_date=date(2026, 8, 1),
    )

    assert (interval.grain, interval.start_date, interval.end_date) == (
        "daily",
        date(2026, 7, 31),
        date(2026, 8, 1),
    )


@pytest.mark.parametrize(
    ("submitted_at", "cutoff", "stage", "effective_end"),
    [
        (datetime(2026, 6, 30, 23, 59, tzinfo=UTC), date(2026, 7, 1), "future", date(2026, 7, 1)),
        (datetime(2026, 7, 15, tzinfo=UTC), date(2026, 7, 14), "provisional", date(2026, 7, 14)),
        (datetime(2026, 8, 2, tzinfo=UTC), date(2026, 8, 1), "provisional", date(2026, 8, 1)),
        (datetime(2026, 8, 4, tzinfo=UTC), date(2026, 7, 30), "provisional", date(2026, 7, 30)),
        (
            datetime(2026, 8, 4, tzinfo=timezone(timedelta(hours=-7))),
            date(2026, 8, 1),
            "settlement_candidate",
            date(2026, 8, 1),
        ),
    ],
)
def test_monthly_evidence_uses_submission_time_72_hours_and_cutoff(
    submitted_at: datetime,
    cutoff: date,
    stage: str,
    effective_end: date,
) -> None:
    models = _models()
    resolution = models.resolve_monthly_evidence(
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        submitted_at=submitted_at,
        availability_cutoff_end_date=cutoff,
    )

    assert isinstance(resolution, models.PreviewEvidenceInterval)
    assert resolution.start_date == date(2026, 7, 1)
    assert resolution.end_date == effective_end
    assert resolution.monthly_stage == stage


def test_monthly_request_adapter_rejects_future_and_returns_same_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request_module()
    models = _models()
    expected = models.PreviewEvidenceInterval(
        start_date=date(2026, 7, 1),
        end_date=date(2026, 7, 20),
        monthly_stage="provisional",
    )
    calls: list[dict[str, object]] = []

    def resolve(**kwargs: object) -> object:
        calls.append(kwargs)
        return expected

    monkeypatch.setattr(request, "resolve_monthly_evidence", resolve)
    monthly = _queued_request(
        grain="monthly",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
    )
    policy = type("Policy", (), {"acquisition_end_date": date(2026, 7, 20)})()

    assert request.resolve_preview_evidence_interval(request=monthly, policy=policy) is expected
    assert calls == [
        {
            "start_date": date(2026, 7, 1),
            "end_date": date(2026, 8, 1),
            "submitted_at": monthly.created_at,
            "availability_cutoff_end_date": date(2026, 7, 20),
        }
    ]

    future = replace(expected, end_date=date(2026, 7, 1), monthly_stage="future")
    monkeypatch.setattr(request, "resolve_monthly_evidence", lambda **_kwargs: future)
    with pytest.raises(request.PreviewEvidencePendingError):
        request.resolve_preview_evidence_interval(request=monthly, policy=policy)


def test_full_and_summary_columns_are_exact_versioned_authorities() -> None:
    mapping = _mapping()

    assert len(mapping.LEGACY_DAILY_FULL_V4_COLUMNS) == 77
    assert mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS is mapping.LEGACY_DAILY_FULL_V4_COLUMNS
    assert mapping.FOCUS_1_4_SUMMARY_COLUMNS == SUMMARY_COLUMNS
    mapping.validate_preview_effective_columns("full", mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS)
    mapping.validate_preview_effective_columns("summary", SUMMARY_COLUMNS)


@pytest.mark.parametrize(
    ("profile", "columns"),
    [
        ("full", ("BilledCost",)),
        ("summary", tuple(reversed(SUMMARY_COLUMNS))),
        ("custom", ()),
        ("custom", ("Unknown",)),
        ("custom", ("BilledCost", "BilledCost")),
    ],
)
def test_mapping_owned_effective_column_validator_rejects_invalid_domain_tuples(
    profile: str,
    columns: tuple[str, ...],
) -> None:
    mapping = _mapping()

    with pytest.raises(mapping.PreviewEffectiveColumnsError):
        mapping.validate_preview_effective_columns(profile, columns)


def test_custom_normalization_preserves_order_and_every_ignored_occurrence() -> None:
    request = _request_module()
    selection = request.normalize_column_selection(
        profile="custom",
        requested_columns=("Unknown", "BilledCost", "Unknown", "BilledCost", "Tags", "billedcost"),
    )

    assert selection.effective_columns == ("BilledCost", "Tags")
    assert selection.ignored_unknown == ("Unknown", "Unknown", "billedcost")
    assert selection.ignored_duplicates == ("BilledCost",)


def test_request_normalization_calls_mapping_owned_validator(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request_module()
    calls: list[tuple[str, tuple[str, ...]]] = []
    original = request.validate_preview_effective_columns

    def validate(profile: str, columns: tuple[str, ...]) -> None:
        calls.append((profile, columns))
        original(profile, columns)

    monkeypatch.setattr(request, "validate_preview_effective_columns", validate)

    selection = request.normalize_column_selection(
        profile="custom",
        requested_columns=("Tags", "BilledCost"),
    )

    assert calls == [("custom", selection.effective_columns)]


@pytest.mark.parametrize("profile", ["full", "summary"])
def test_noncustom_profiles_reject_explicit_columns(profile: str) -> None:
    request = _request_module()

    with pytest.raises(request.PreviewRequestValidationError) as exc_info:
        request.normalize_column_selection(profile=profile, requested_columns=("BilledCost",))

    assert exc_info.value.detail == "columns may be supplied only when column_profile is custom"


def test_all_invalid_custom_error_retains_ignored_entries() -> None:
    request = _request_module()

    with pytest.raises(request.PreviewColumnSelectionEmptyError) as exc_info:
        request.normalize_column_selection(profile="custom", requested_columns=("Unknown", "Unknown"))

    assert exc_info.value.detail == "Custom column selection must contain at least one supported Full-profile column"
    assert exc_info.value.ignored_unknown == ("Unknown", "Unknown")
    assert exc_info.value.ignored_duplicates == ()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("created_at", datetime(2026, 7, 3), "created_at"),
        ("started_at", datetime(2026, 7, 3), "started_at"),
        ("completed_at", datetime(2026, 7, 3), "completed_at"),
        ("started_at", datetime(2026, 7, 2, tzinfo=UTC), "started_at"),
    ],
)
def test_request_lifecycle_rejects_naive_or_out_of_order_timestamps(
    field: str,
    value: datetime,
    message: str,
) -> None:
    models = _models()
    overrides: dict[str, object] = {field: value}
    if field == "started_at":
        overrides["status"] = models.PreviewRequestStatus.RUNNING
    elif field == "completed_at":
        overrides.update(
            status=models.PreviewRequestStatus.FAILED,
            diagnostic=models.PreviewDiagnostic("failed", "failed", False),
        )

    with pytest.raises(ValueError, match=message):
        _queued_request(**overrides)


def test_request_lifecycle_normalizes_aware_timestamps_to_utc() -> None:
    offset = timezone(timedelta(hours=5, minutes=30))
    request = _queued_request(created_at=datetime(2026, 7, 3, 12, tzinfo=offset))

    assert request.created_at == datetime(2026, 7, 3, 6, 30, tzinfo=UTC)


def test_models_own_no_mapping_column_authority() -> None:
    source = __import__("inspect").getsource(_models())

    assert "core.preview.mapping" not in source
    assert "FOCUS_1_4_FULL_PROFILE_COLUMNS" not in source
    assert "BilledCost" not in source
