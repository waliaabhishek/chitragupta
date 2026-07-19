from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from core.config.models import TenantConfig


def _tenant(**overrides: object) -> TenantConfig:
    values: dict[str, object] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "tenant-1",
    }
    values.update(overrides)
    return TenantConfig.model_validate(values)


def _focus_preview(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "commercial_profile": "direct_payg",
        "effective_start_date": "2026-01-01",
        "effective_end_date": "2027-01-01",
    }
    values.update(overrides)
    return values


def test_focus_preview_block_is_optional_without_implying_eligibility() -> None:
    tenant = _tenant()

    assert tenant.focus_preview is None


def test_focus_preview_defaults_billing_currency_to_usd() -> None:
    tenant = _tenant(focus_preview=_focus_preview())

    assert tenant.focus_preview is not None
    assert tenant.focus_preview.commercial_profile == "direct_payg"
    assert tenant.focus_preview.billing_currency == "USD"
    assert tenant.focus_preview.effective_start_date == date(2026, 1, 1)
    assert tenant.focus_preview.effective_end_date == date(2027, 1, 1)


@pytest.mark.parametrize("currency", ["usd", " USD ", "uSd"])
def test_focus_preview_normalizes_currency(currency: str) -> None:
    tenant = _tenant(focus_preview=_focus_preview(billing_currency=currency))

    assert tenant.focus_preview is not None
    assert tenant.focus_preview.billing_currency == "USD"


@pytest.mark.parametrize("currency", [123, [], {}, True, None])
def test_focus_preview_rejects_non_string_currency_as_validation_error(currency: object) -> None:
    with pytest.raises(ValidationError, match="three-letter currency code"):
        _tenant(focus_preview=_focus_preview(billing_currency=currency))


@pytest.mark.parametrize("currency", ["", "US", "USDD", "U1D", "$"])
def test_focus_preview_rejects_invalid_currency_shape(currency: str) -> None:
    with pytest.raises(ValidationError, match="three-letter currency code"):
        _tenant(focus_preview=_focus_preview(billing_currency=currency))


def test_focus_preview_accepts_non_usd_for_asynchronous_eligibility_failure() -> None:
    tenant = _tenant(focus_preview=_focus_preview(billing_currency="eur"))

    assert tenant.focus_preview is not None
    assert tenant.focus_preview.billing_currency == "EUR"


@pytest.mark.parametrize("profile", [None, "marketplace", "commitment", "negotiated"])
def test_focus_preview_requires_direct_payg_profile(profile: str | None) -> None:
    block = _focus_preview()
    if profile is None:
        del block["commercial_profile"]
    else:
        block["commercial_profile"] = profile

    with pytest.raises(ValidationError):
        _tenant(focus_preview=block)


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-01-01", "2026-01-01"), ("2026-01-02", "2026-01-01")],
)
def test_focus_preview_requires_ordered_half_open_effective_interval(start: str, end: str) -> None:
    with pytest.raises(ValidationError, match="effective_start_date must be before effective_end_date"):
        _tenant(
            focus_preview=_focus_preview(
                effective_start_date=start,
                effective_end_date=end,
            )
        )


def test_focus_preview_does_not_change_lookback_maximum() -> None:
    with pytest.raises(ValidationError):
        _tenant(lookback_days=365, focus_preview=_focus_preview())
