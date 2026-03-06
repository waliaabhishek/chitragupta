from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.config.models import PluginSettingsBase


class TestPluginSettingsBaseGranularityDurations:
    def test_zero_hours_raises_validation_error(self) -> None:
        """Verification item 5: realtime: 0 is rejected at config load."""
        with pytest.raises(ValidationError):
            PluginSettingsBase(granularity_durations={"realtime": 0})

    def test_negative_hours_raises_validation_error(self) -> None:
        """Verification item 6: negative duration is rejected."""
        with pytest.raises(ValidationError):
            PluginSettingsBase(granularity_durations={"sub": -1})

    def test_valid_weekly_168_instantiates_without_error(self) -> None:
        """Verification item 7: weekly: 168 is valid (7*24)."""
        cfg = PluginSettingsBase(granularity_durations={"weekly": 168})
        assert cfg.granularity_durations == {"weekly": 168}

    def test_default_is_empty_dict(self) -> None:
        """Verification item 8: default granularity_durations is empty — no behaviour change."""
        cfg = PluginSettingsBase()
        assert cfg.granularity_durations == {}

    def test_multiple_valid_entries_accepted(self) -> None:
        """Multiple custom granularities all ≥ 1h accepted."""
        cfg = PluginSettingsBase(granularity_durations={"weekly": 168, "biweekly": 336, "4hourly": 4})
        assert cfg.granularity_durations["weekly"] == 168
        assert cfg.granularity_durations["biweekly"] == 336
        assert cfg.granularity_durations["4hourly"] == 4

    def test_minimum_valid_value_is_1(self) -> None:
        """Exactly 1 hour is accepted (minimum boundary)."""
        cfg = PluginSettingsBase(granularity_durations={"custom": 1})
        assert cfg.granularity_durations["custom"] == 1

    def test_mixed_valid_invalid_raises(self) -> None:
        """A dict with one invalid entry among valid entries still raises."""
        with pytest.raises(ValidationError):
            PluginSettingsBase(granularity_durations={"weekly": 168, "realtime": 0})

    def test_existing_fields_unaffected_by_empty_granularity_durations(self) -> None:
        """Adding granularity_durations field doesn't break existing PluginSettingsBase fields."""
        cfg = PluginSettingsBase(
            allocator_params={"ratio": 0.5},
            min_refresh_gap_seconds=900,
            granularity_durations={},
        )
        assert cfg.allocator_params == {"ratio": 0.5}
        assert cfg.min_refresh_gap_seconds == 900
        assert cfg.granularity_durations == {}
