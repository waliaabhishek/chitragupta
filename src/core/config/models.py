from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

logger = logging.getLogger(__name__)

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
    connection_string: SecretStr = Field(default=SecretStr("sqlite:///data/chargeback.db"))


class EmitterSpec(BaseModel):
    """Spec for one emitter entry in YAML config.

    ``type`` is a human-friendly registered name (e.g. ``"csv"``).
    The registry maps names to factory callables; ``params`` are forwarded to
    the factory as keyword arguments.

    ``aggregation`` controls row aggregation before emitting:
    - ``None`` (default) — pass rows as-is, no aggregation
    - ``"hourly"`` — rows grouped/summed by hour (only valid when chargeback granularity is hourly)
    - ``"daily"`` — rows grouped/summed by calendar day
    - ``"monthly"`` — rows grouped/summed by calendar month (queries full month per run)
    """

    type: str
    name: str = ""  # unique state key (defaults to type if empty)
    aggregation: Literal["hourly", "daily", "monthly"] | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    lookback_days: int | None = None  # None = all history, int = bounded backfill

    @model_validator(mode="after")
    def _default_name(self) -> EmitterSpec:
        if not self.name:
            self.name = self.type
        return self


_MIN_GRANULARITY_HOURS = 1  # sub-hour cadences not supported


class PluginSettingsBase(BaseModel):
    """Orchestrator-consumed plugin settings. All plugin configs must extend this."""

    model_config = ConfigDict(extra="allow")  # plugin-specific fields pass through

    allocator_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    allocator_overrides: dict[str, str] = Field(default_factory=dict)
    identity_resolution_overrides: dict[str, str] = Field(default_factory=dict)
    min_refresh_gap_seconds: int = Field(default=1800, ge=0)
    granularity_durations: dict[str, int] = Field(
        default_factory=dict,
        description="Custom granularity name → duration in whole hours (minimum 1).",
    )

    @field_validator("granularity_durations")
    @classmethod
    def validate_granularity_hours(cls, v: dict[str, int]) -> dict[str, int]:
        for name, hours in v.items():
            if hours < _MIN_GRANULARITY_HOURS:
                raise ValueError(f"granularity_durations[{name!r}]: minimum is 1 hour, got {hours}")
        return v

    metrics_step_seconds: int = Field(
        default=3600,
        gt=0,
        description=(
            "Prometheus range query step in seconds. Controls billing granularity and Prometheus server load."
            " Default 3600 = 1-hour resolution. Lower values (e.g. 1800) give finer-grained cost data at"
            " higher Prometheus load."
        ),
    )
    emitters: list[EmitterSpec] = Field(default_factory=list)
    chargeback_granularity: Literal["hourly", "daily", "monthly"] = Field(
        default="daily",
        description=(
            "Granularity of billing data produced by the plugin's CostInput. "
            "Controls emitter aggregation validation — emitters may not request "
            "finer granularity than the chargeback data provides."
        ),
    )


class TenantConfig(BaseModel):
    ecosystem: str
    tenant_id: str = Field(
        description=(
            "Unique partition key for DB records and storage. "
            "Not sent to any external API — CCloud APIs are scoped by credentials. "
            "Can be any string (e.g. 'prod', 'acme-corp'). "
            "The dict key under 'tenants:' is a separate human-friendly label for logs/filenames."
        ),
    )
    lookback_days: int = Field(default=200, gt=0, le=364)
    cutoff_days: int = Field(default=5, gt=0, le=30)
    retention_days: int = Field(default=250, gt=0, le=730)
    allocation_retry_limit: int = Field(default=3, gt=0, le=10)
    topic_attribution_retry_limit: int = Field(default=3, gt=0, le=10)
    zero_gather_deletion_threshold: int = Field(default=-1, ge=-1)
    gather_failure_threshold: int = Field(default=5, gt=0)
    tenant_execution_timeout_seconds: int = Field(default=3600, ge=0)
    metrics_prefetch_workers: int = Field(default=4, ge=1, le=20)
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
    plugins_path: Path | None = Field(
        default=None,
        description=(
            "Path to plugin directory. "
            "Absolute paths used as-is. "
            "Relative paths resolved against CWD. "
            "Defaults to the 'plugins/' sibling of the src/ package root."
        ),
    )

    @model_validator(mode="after")
    def validate_unique_connection_strings(self) -> AppSettings:
        seen: dict[str, str] = {}  # raw_conn -> tenant_name
        for name, config in self.tenants.items():
            conn = config.storage.connection_string.get_secret_value()
            if conn in seen:
                raise ValueError(
                    f"tenants {seen[conn]!r} and {name!r} share the same "
                    f"storage connection_string — each tenant must use a "
                    f"separate database until full tenant isolation is implemented"
                )
            seen[conn] = name
        return self
