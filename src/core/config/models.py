from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    @field_validator("level", mode="before")
    @classmethod
    def normalize_and_validate_level(cls, v: str) -> str:
        upper = v.upper() if v else "INFO"
        if upper not in _VALID_LOG_LEVELS:
            raise ValueError(f"level must be one of {sorted(_VALID_LOG_LEVELS)}, got {v!r}")
        return upper


class FeaturesConfig(BaseModel):
    enable_periodic_refresh: bool = True
    refresh_interval: int = Field(default=1800, gt=0)


class ApiConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8080, gt=0, le=65535)


class StorageConfig(BaseModel):
    backend: str = "sqlmodel"
    connection_string: str = "sqlite:///data/chargeback.db"


class TenantConfig(BaseModel):
    ecosystem: str
    tenant_id: str
    lookback_days: int = Field(default=200, gt=0, le=364)
    cutoff_days: int = Field(default=5, gt=0, le=30)
    retention_days: int = Field(default=250, gt=0, le=730)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    plugin_settings: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_lookback_gt_cutoff(self) -> TenantConfig:
        if self.lookback_days <= self.cutoff_days:
            raise ValueError("lookback_days must be > cutoff_days")
        return self


class AppSettings(BaseModel):
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    tenants: dict[str, TenantConfig] = Field(default_factory=dict)
