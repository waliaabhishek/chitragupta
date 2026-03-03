from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    per_module_levels: dict[str, str] = Field(default_factory=dict)

    @field_validator("level", mode="before")
    @classmethod
    def normalize_and_validate_level(cls, v: str) -> str:
        upper = v.upper() if v else "INFO"
        if upper not in _VALID_LOG_LEVELS:
            raise ValueError(f"level must be one of {sorted(_VALID_LOG_LEVELS)}, got {v!r}")
        return upper

    @field_validator("per_module_levels", mode="before")
    @classmethod
    def normalize_per_module_levels(cls, v: dict[str, str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for module, level in v.items():
            upper = level.upper()
            if upper not in _VALID_LOG_LEVELS:
                raise ValueError(
                    f"per_module_levels[{module!r}]: level must be one of {sorted(_VALID_LOG_LEVELS)}, got {level!r}"
                )
            result[module] = upper
        return result


class FeaturesConfig(BaseModel):
    enable_periodic_refresh: bool = True
    refresh_interval: int = Field(default=1800, gt=0)
    max_parallel_tenants: int = Field(default=4, gt=0, le=64)


class ApiConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8080, gt=0, le=65535)
    enable_cors: bool = False
    cors_origins: list[str] = Field(default_factory=list)
    request_timeout_seconds: int = Field(default=30, gt=0, le=300)


class StorageConfig(BaseModel):
    backend: str = "sqlmodel"
    connection_string: str = "sqlite:///data/chargeback.db"


class PluginSettingsBase(BaseModel):
    """Orchestrator-consumed plugin settings. All plugin configs must extend this."""

    model_config = ConfigDict(extra="allow")  # plugin-specific fields pass through

    allocator_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    allocator_overrides: dict[str, str] = Field(default_factory=dict)
    identity_resolution_overrides: dict[str, str] = Field(default_factory=dict)
    min_refresh_gap_seconds: int = Field(default=1800, ge=0)


class TenantConfig(BaseModel):
    ecosystem: str
    tenant_id: str
    lookback_days: int = Field(default=200, gt=0, le=364)
    cutoff_days: int = Field(default=5, gt=0, le=30)
    retention_days: int = Field(default=250, gt=0, le=730)
    allocation_retry_limit: int = Field(default=3, gt=0, le=10)
    max_dates_per_run: int = Field(default=15, gt=0, le=365)
    zero_gather_deletion_threshold: int = Field(default=-1, ge=-1)
    gather_failure_threshold: int = Field(default=5, gt=0)
    tenant_execution_timeout_seconds: int = Field(default=3600, ge=0)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    plugin_settings: PluginSettingsBase = Field(default_factory=PluginSettingsBase)

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

    @model_validator(mode="after")
    def validate_unique_connection_strings(self) -> AppSettings:
        seen: dict[str, str] = {}  # connection_string → tenant_name
        for name, config in self.tenants.items():
            conn = config.storage.connection_string
            if conn in seen:
                raise ValueError(
                    f"tenants {seen[conn]!r} and {name!r} share storage "
                    f"connection_string {conn!r} — each tenant must use a "
                    f"separate database until full tenant isolation is implemented"
                )
            seen[conn] = name
        return self
