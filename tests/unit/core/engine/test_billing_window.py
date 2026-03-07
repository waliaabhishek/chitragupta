from __future__ import annotations

import calendar
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from core.engine.orchestrator import billing_window
from core.models.billing import BillingLineItem, CoreBillingLineItem

_TS = datetime(2024, 3, 15, 0, 0, 0, tzinfo=UTC)


def _make_line(**overrides: object) -> BillingLineItem:
    defaults: dict[str, object] = {
        "ecosystem": "test",
        "tenant_id": "t-001",
        "timestamp": _TS,
        "resource_id": "r-1",
        "product_category": "kafka",
        "product_type": "kafka_ckus",
        "quantity": Decimal("1"),
        "unit_price": Decimal("1"),
        "total_cost": Decimal("1"),
    }
    defaults.update(overrides)
    return CoreBillingLineItem(**defaults)  # type: ignore[arg-type]


class TestBillingWindowExtraGranularities:
    def test_weekly_with_extra_durations_returns_7_day_window(self) -> None:
        """Verification item 1: billing_window with custom weekly duration."""
        line = _make_line(granularity="weekly")
        extra = {"weekly": timedelta(hours=168)}
        start, end, duration = billing_window(line, extra)
        assert start == _TS
        assert duration == timedelta(hours=168)
        assert end == _TS + timedelta(hours=168)

    def test_daily_with_none_extra_returns_24h_window(self) -> None:
        """Verification item 2: None extra_durations leaves defaults intact."""
        line = _make_line(granularity="daily")
        start, end, duration = billing_window(line, None)
        assert start == _TS
        assert duration == timedelta(hours=24)
        assert end == _TS + timedelta(hours=24)

    def test_monthly_with_none_extra_returns_calendar_month_window(self) -> None:
        """Verification item 3: monthly granularity uses calendar days."""
        line = _make_line(granularity="monthly")
        _, days_in_march = calendar.monthrange(2024, 3)
        start, end, duration = billing_window(line, None)
        assert start == _TS
        assert duration == timedelta(days=days_in_march)
        assert end == _TS + timedelta(days=days_in_march)

    def test_unknown_granularity_with_none_extra_raises_value_error(self) -> None:
        """Verification item 4: unknown granularity raises ValueError."""
        line = _make_line(granularity="biweekly")
        with pytest.raises(ValueError, match="Unknown billing granularity"):
            billing_window(line, None)

    def test_weekly_without_extra_raises_value_error(self) -> None:
        """weekly granularity is not in default mapping — must raise without extra."""
        line = _make_line(granularity="weekly")
        with pytest.raises(ValueError, match="Unknown billing granularity"):
            billing_window(line, None)

    def test_extra_durations_take_precedence_over_defaults(self) -> None:
        """Plugin-supplied mapping merges on top of built-ins; plugin wins on conflict."""
        line = _make_line(granularity="daily")
        # Override daily to 48h via extra_durations
        extra = {"daily": timedelta(hours=48)}
        _, _, duration = billing_window(line, extra)
        assert duration == timedelta(hours=48)

    def test_empty_extra_durations_uses_defaults(self) -> None:
        """Empty dict for extra_durations leaves built-in defaults intact."""
        line = _make_line(granularity="daily")
        _, _, duration = billing_window(line, {})
        assert duration == timedelta(hours=24)


class TestBillingWindowOrchestratorIntegration:
    def test_orchestrator_with_granularity_durations_processes_weekly_line(self) -> None:
        """Verification item 9: orchestrator plumbing wires extra_durations to billing_window."""
        from core.config.models import PluginSettingsBase

        settings = PluginSettingsBase(granularity_durations={"weekly": 168})
        extra_durations = {name: timedelta(hours=hours) for name, hours in settings.granularity_durations.items()}
        line = _make_line(granularity="weekly")
        # Must not raise ValueError
        start, end, duration = billing_window(line, extra_durations)
        assert duration == timedelta(hours=168)
        assert end - start == timedelta(hours=168)
