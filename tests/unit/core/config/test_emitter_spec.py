from __future__ import annotations

import pytest
from pydantic import ValidationError


class TestEmitterSpecValid:
    def test_instantiates_with_all_fields(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="csv", aggregation="daily", params={"output_dir": "/tmp"})
        assert spec.type == "csv"
        assert spec.aggregation == "daily"
        assert spec.params == {"output_dir": "/tmp"}

    def test_instantiates_with_minimal_fields(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="csv")
        assert spec.type == "csv"
        assert spec.aggregation is None
        assert spec.params == {}

    def test_aggregation_hourly_valid(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="csv", aggregation="hourly")
        assert spec.aggregation == "hourly"

    def test_aggregation_monthly_valid(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="csv", aggregation="monthly")
        assert spec.aggregation == "monthly"

    def test_aggregation_none_valid(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="webhook", aggregation=None)
        assert spec.aggregation is None

    def test_params_passed_through(self) -> None:
        from core.config.models import EmitterSpec

        spec = EmitterSpec(type="csv", params={"output_dir": "/tmp", "extra": "val"})
        assert spec.params["output_dir"] == "/tmp"
        assert spec.params["extra"] == "val"


class TestEmitterSpecInvalidAggregation:
    def test_invalid_aggregation_raises_validation_error(self) -> None:
        from core.config.models import EmitterSpec

        with pytest.raises(ValidationError):
            EmitterSpec(type="csv", aggregation="hourly_invalid")  # type: ignore[arg-type]

    def test_weekly_aggregation_raises_validation_error(self) -> None:
        from core.config.models import EmitterSpec

        with pytest.raises(ValidationError):
            EmitterSpec(type="csv", aggregation="weekly")  # type: ignore[arg-type]

    def test_empty_string_aggregation_raises_validation_error(self) -> None:
        from core.config.models import EmitterSpec

        with pytest.raises(ValidationError):
            EmitterSpec(type="csv", aggregation="")  # type: ignore[arg-type]


class TestPluginSettingsBaseEmittersField:
    def test_emitters_default_is_empty_list(self) -> None:
        from core.config.models import PluginSettingsBase

        cfg = PluginSettingsBase()
        assert cfg.emitters == []

    def test_chargeback_granularity_default_is_daily(self) -> None:
        from core.config.models import PluginSettingsBase

        cfg = PluginSettingsBase()
        assert cfg.chargeback_granularity == "daily"

    def test_emitters_with_csv_spec(self) -> None:
        from core.config.models import EmitterSpec, PluginSettingsBase

        cfg = PluginSettingsBase(emitters=[EmitterSpec(type="csv", aggregation="daily", params={"output_dir": "/tmp"})])
        assert len(cfg.emitters) == 1
        assert cfg.emitters[0].type == "csv"

    def test_chargeback_granularity_hourly(self) -> None:
        from core.config.models import PluginSettingsBase

        cfg = PluginSettingsBase(chargeback_granularity="hourly")
        assert cfg.chargeback_granularity == "hourly"

    def test_chargeback_granularity_monthly(self) -> None:
        from core.config.models import PluginSettingsBase

        cfg = PluginSettingsBase(chargeback_granularity="monthly")
        assert cfg.chargeback_granularity == "monthly"

    def test_chargeback_granularity_invalid_raises(self) -> None:
        from core.config.models import PluginSettingsBase

        with pytest.raises(ValidationError):
            PluginSettingsBase(chargeback_granularity="weekly")  # type: ignore[arg-type]
