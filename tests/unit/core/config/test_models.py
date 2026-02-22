from __future__ import annotations

import pytest
from pydantic import ValidationError

from core.config.models import (
    ApiConfig,
    AppSettings,
    FeaturesConfig,
    LoggingConfig,
    StorageConfig,
    TenantConfig,
)


class TestLoggingConfig:
    def test_defaults(self) -> None:
        cfg = LoggingConfig()
        assert cfg.level == "INFO"
        assert "%(asctime)s" in cfg.format

    def test_all_fields(self) -> None:
        cfg = LoggingConfig(level="debug", format="%(message)s")
        assert cfg.level == "DEBUG"
        assert cfg.format == "%(message)s"

    def test_valid_levels(self) -> None:
        for level in ("critical", "error", "WARNING", "Info", "DEBUG"):
            cfg = LoggingConfig(level=level)
            assert cfg.level == level.upper()

    def test_invalid_level_raises(self) -> None:
        with pytest.raises(ValidationError, match="level must be one of"):
            LoggingConfig(level="TRACE")

    def test_empty_string_level_defaults_to_info(self) -> None:
        cfg = LoggingConfig(level="")
        assert cfg.level == "INFO"


class TestFeaturesConfig:
    def test_defaults(self) -> None:
        cfg = FeaturesConfig()
        assert cfg.enable_periodic_refresh is True
        assert cfg.refresh_interval == 1800

    def test_all_fields(self) -> None:
        cfg = FeaturesConfig(enable_periodic_refresh=False, refresh_interval=60)
        assert cfg.enable_periodic_refresh is False
        assert cfg.refresh_interval == 60

    def test_refresh_interval_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            FeaturesConfig(refresh_interval=0)

    def test_refresh_interval_negative_raises(self) -> None:
        with pytest.raises(ValidationError):
            FeaturesConfig(refresh_interval=-1)


class TestApiConfig:
    def test_defaults(self) -> None:
        cfg = ApiConfig()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8080

    def test_all_fields(self) -> None:
        cfg = ApiConfig(host="127.0.0.1", port=9090)
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 9090

    def test_port_zero_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ApiConfig(port=0)

    def test_port_max_accepted(self) -> None:
        cfg = ApiConfig(port=65535)
        assert cfg.port == 65535

    def test_port_above_max_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ApiConfig(port=65536)


class TestStorageConfig:
    def test_defaults(self) -> None:
        cfg = StorageConfig()
        assert cfg.backend == "sqlmodel"
        assert "sqlite" in cfg.connection_string

    def test_all_fields(self) -> None:
        cfg = StorageConfig(backend="postgres", connection_string="postgresql://localhost/db")
        assert cfg.backend == "postgres"


class TestTenantConfig:
    def test_minimal(self) -> None:
        cfg = TenantConfig(ecosystem="confluent_cloud", tenant_id="org-123")
        assert cfg.lookback_days == 200
        assert cfg.cutoff_days == 5
        assert cfg.retention_days == 250
        assert cfg.plugin_settings == {}
        assert cfg.storage.backend == "sqlmodel"

    def test_all_fields(self) -> None:
        cfg = TenantConfig(
            ecosystem="self_managed_kafka",
            tenant_id="t-1",
            lookback_days=100,
            cutoff_days=3,
            retention_days=400,
            storage=StorageConfig(backend="postgres", connection_string="pg://localhost/db"),
            plugin_settings={"cost_model": "constructed"},
        )
        assert cfg.ecosystem == "self_managed_kafka"
        assert cfg.retention_days == 400
        assert cfg.plugin_settings["cost_model"] == "constructed"

    def test_lookback_must_exceed_cutoff(self) -> None:
        with pytest.raises(ValidationError, match="lookback_days must be > cutoff_days"):
            TenantConfig(ecosystem="x", tenant_id="t", lookback_days=5, cutoff_days=5)

    def test_lookback_less_than_cutoff_raises(self) -> None:
        with pytest.raises(ValidationError, match="lookback_days must be > cutoff_days"):
            TenantConfig(ecosystem="x", tenant_id="t", lookback_days=3, cutoff_days=5)

    def test_lookback_upper_bound(self) -> None:
        with pytest.raises(ValidationError):
            TenantConfig(ecosystem="x", tenant_id="t", lookback_days=365)

    def test_cutoff_upper_bound(self) -> None:
        with pytest.raises(ValidationError):
            TenantConfig(ecosystem="x", tenant_id="t", cutoff_days=31)


class TestAppSettings:
    def test_defaults(self) -> None:
        cfg = AppSettings()
        assert cfg.logging.level == "INFO"
        assert cfg.features.refresh_interval == 1800
        assert cfg.api.port == 8080
        assert cfg.tenants == {}

    def test_empty_tenants_valid(self) -> None:
        cfg = AppSettings(tenants={})
        assert cfg.tenants == {}

    def test_multiple_tenants(self) -> None:
        cfg = AppSettings(
            tenants={
                "org-a": TenantConfig(ecosystem="confluent_cloud", tenant_id="a"),
                "org-b": TenantConfig(ecosystem="self_managed_kafka", tenant_id="b"),
            }
        )
        assert len(cfg.tenants) == 2
        assert cfg.tenants["org-a"].ecosystem == "confluent_cloud"
        assert cfg.tenants["org-b"].ecosystem == "self_managed_kafka"

    def test_from_dict(self) -> None:
        data = {
            "logging": {"level": "debug"},
            "api": {"port": 9000},
            "tenants": {
                "t1": {"ecosystem": "cc", "tenant_id": "id1"},
            },
        }
        cfg = AppSettings.model_validate(data)
        assert cfg.logging.level == "DEBUG"
        assert cfg.api.port == 9000
        assert cfg.tenants["t1"].tenant_id == "id1"
