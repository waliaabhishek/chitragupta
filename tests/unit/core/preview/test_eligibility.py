from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from importlib import import_module

import pytest

from core.config.models import TenantConfig
from core.preview.models import PreviewRequest, PreviewRequestStatus


def _eligibility() -> object:
    return import_module("core.preview.eligibility")


def _tenant(*, focus_preview: dict[str, object] | None, lookback_days: int = 200, cutoff_days: int = 5) -> TenantConfig:
    return TenantConfig.model_validate(
        {
            "ecosystem": "confluent_cloud",
            "tenant_id": "tenant-1",
            "lookback_days": lookback_days,
            "cutoff_days": cutoff_days,
            "focus_preview": focus_preview,
        }
    )


def _block(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "commercial_profile": "direct_payg",
        "billing_currency": "USD",
        "effective_start_date": "2026-01-01",
        "effective_end_date": "2027-01-01",
    }
    values.update(overrides)
    return values


def _request(**overrides: object) -> PreviewRequest:
    values: dict[str, object] = {
        "request_id": "request-1",
        "tenant_name": "production",
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
        "grain": "daily",
        "start_date": date(2026, 7, 1),
        "end_date": date(2026, 7, 2),
        "column_profile": "full",
        "effective_columns": import_module("core.preview.mapping").FOCUS_1_4_FULL_PROFILE_COLUMNS,
        "status": PreviewRequestStatus.QUEUED,
        "created_at": datetime(2026, 7, 4, tzinfo=UTC),
        "started_at": None,
        "completed_at": None,
        "source_snapshot": None,
        "diagnostic": None,
        "storage_key": None,
        "package": None,
    }
    values.update(overrides)
    return PreviewRequest(**values)  # type: ignore[arg-type]


def test_policy_uses_created_at_utc_date_and_half_open_acquisition_bounds() -> None:
    eligibility = _eligibility()
    created_at = datetime(2026, 7, 4, 23, 30, tzinfo=UTC)

    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block(), lookback_days=200, cutoff_days=5),
        created_at=created_at,
    )

    assert policy.acquisition_start_date == date(2025, 12, 16)
    assert policy.acquisition_end_date == date(2026, 6, 29)
    assert policy.commercial_profile == "direct_payg"
    assert policy.billing_currency == "USD"


def test_policy_rejects_naive_created_at() -> None:
    eligibility = _eligibility()

    with pytest.raises(ValueError, match="timezone-aware"):
        eligibility.policy_from_tenant_config(
            _tenant(focus_preview=_block()),
            created_at=datetime(2026, 7, 4),
        )


def test_missing_focus_preview_fails_closed() -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=None),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    diagnostic = eligibility.request_eligibility_diagnostic(request=_request(), policy=policy)

    assert diagnostic.code == "preview_commercial_profile_unavailable"
    assert diagnostic.message == "An explicit Direct-billed PAYG profile does not cover the requested interval."
    assert diagnostic.retryable is False


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        (date(2025, 12, 31), date(2026, 1, 2)),
        (date(2026, 12, 31), date(2027, 1, 2)),
    ],
)
def test_effective_interval_must_fully_contain_request(start_date: date, end_date: date) -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block()),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    diagnostic = eligibility.request_eligibility_diagnostic(
        request=_request(start_date=start_date, end_date=end_date),
        policy=policy,
    )

    assert diagnostic.code == "preview_commercial_profile_unavailable"


def test_effective_interval_uses_inclusive_start_and_exclusive_end() -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block()),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    assert (
        eligibility.request_eligibility_diagnostic(
            request=_request(start_date=date(2026, 1, 1), end_date=date(2027, 1, 1)),
            policy=policy,
        )
        is None
    )


def test_non_usd_policy_fails_before_source_evidence() -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block(billing_currency="EUR")),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    diagnostic = eligibility.request_eligibility_diagnostic(request=_request(), policy=policy)

    assert diagnostic.code == "preview_billing_currency_unsupported"
    assert diagnostic.message == "FOCUS Mapping Preview currently supports only USD billing currency."
    assert diagnostic.retryable is False


def test_impossible_partial_runtime_policy_fails_commercial_authority() -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block()),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    diagnostic = eligibility.request_eligibility_diagnostic(
        request=_request(),
        policy=replace(policy, effective_end_date=None),
    )

    assert diagnostic.code == "preview_commercial_profile_unavailable"


def test_impossible_runtime_policy_without_billing_currency_fails_commercial_authority() -> None:
    eligibility = _eligibility()
    policy = eligibility.policy_from_tenant_config(
        _tenant(focus_preview=_block()),
        created_at=datetime(2026, 7, 4, tzinfo=UTC),
    )

    diagnostic = eligibility.request_eligibility_diagnostic(
        request=_request(),
        policy=replace(policy, billing_currency=None),
    )

    assert diagnostic.code == "preview_commercial_profile_unavailable"


def test_public_source_correlation_is_deterministic_tenant_scoped_and_redacted() -> None:
    eligibility = _eligibility()

    first = eligibility.public_source_correlation_id(
        ecosystem="confluent_cloud",
        tenant_id="tenant-secret",
        source_record_id="provider:cost-secret",
    )
    again = eligibility.public_source_correlation_id(
        ecosystem="confluent_cloud",
        tenant_id="tenant-secret",
        source_record_id="provider:cost-secret",
    )
    other_tenant = eligibility.public_source_correlation_id(
        ecosystem="confluent_cloud",
        tenant_id="other-tenant",
        source_record_id="provider:cost-secret",
    )

    assert first == again
    assert first != other_tenant
    assert first.startswith("src:v1:")
    assert len(first) == len("src:v1:") + 64
    assert first.removeprefix("src:v1:").isalnum()
    assert "tenant-secret" not in first
    assert "cost-secret" not in first
