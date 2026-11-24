from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    SecretStr,
    Tag,
    field_validator,
    model_validator,
)

logger = logging.getLogger(__name__)

_VALID_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"})
MAX_SOURCE_RETENTION_DAYS = 730


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


class PreviewConfig(BaseModel):
    artifact_root: Path = Path("data/focus-preview")
    max_workers: int = Field(default=2, gt=0, le=16, strict=True)
    max_queued_repairs: int = Field(default=8, ge=0, strict=True)
    max_queued_generations: int = Field(default=8, ge=0, strict=True)
    max_running_generations_per_tenant: int = Field(default=1, gt=0, strict=True)
    max_queued_generations_per_tenant: int = Field(default=2, ge=0, strict=True)
    max_generation_spool_bytes: int = Field(default=2_147_483_648, gt=0, strict=True)
    max_csv_file_bytes: int | None = Field(default=None, gt=0, strict=True)

    @model_validator(mode="after")
    def validate_capacity_limits(self) -> PreviewConfig:
        if self.max_running_generations_per_tenant > self.max_workers:
            raise ValueError("preview per-tenant running limit must not exceed max_workers")

        global_queued = self.max_queued_generations
        tenant_queued = self.max_queued_generations_per_tenant
        if (global_queued == 0) != (tenant_queued == 0):
            raise ValueError("preview global and per-tenant queued limits must both be zero or both be positive")
        if global_queued > 0 and tenant_queued >= global_queued:
            raise ValueError("preview per-tenant queued limit must be lower than the global queued limit")
        return self


class FocusPreviewTenantConfig(BaseModel):
    commercial_profile: Literal["direct_payg"]
    billing_currency: str = "USD"
    effective_start_date: date
    effective_end_date: date | None = None

    @field_validator("billing_currency", mode="before")
    @classmethod
    def normalize_billing_currency(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("focus_preview.billing_currency must be a three-letter currency code")
        normalized = value.strip().upper()
        if re.fullmatch(r"[A-Z]{3}", normalized) is None:
            raise ValueError("focus_preview.billing_currency must be a three-letter currency code")
        return normalized

    @model_validator(mode="after")
    def validate_effective_interval(self) -> FocusPreviewTenantConfig:
        if self.effective_end_date is not None and self.effective_start_date >= self.effective_end_date:
            raise ValueError("focus_preview.effective_start_date must be before effective_end_date")
        return self


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


def _validate_iceberg_endpoint(value: str, schemes: tuple[str, ...]) -> str:
    message = f"endpoint must be a {', '.join(schemes)} URL without credentials, query, or fragment"
    if any(char.isspace() or ord(char) < 32 for char in value) or "?" in value or "#" in value:
        raise ValueError(message)
    try:
        endpoint = urlsplit(value)
        port = endpoint.port
    except ValueError:
        raise ValueError(message) from None
    if endpoint.scheme not in schemes or not endpoint.hostname or endpoint.username is not None or port == 0:
        raise ValueError(message)
    return value


def _nonempty_secret(value: SecretStr) -> SecretStr:
    if not value.get_secret_value().strip():
        raise ValueError("credential must not be empty")
    return value


_IcebergCredential = Annotated[SecretStr, AfterValidator(_nonempty_secret)]


class IcebergS3Config(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    endpoint: str | None = None
    region: str | None = Field(default=None, min_length=1)
    access_key_id: _IcebergCredential | None = None
    secret_access_key: _IcebergCredential | None = None
    session_token: _IcebergCredential | None = None

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str | None) -> str | None:
        return _validate_iceberg_endpoint(value, ("https", "http")) if value is not None else None

    @model_validator(mode="after")
    def validate_inline_credentials(self) -> IcebergS3Config:
        if any((self.access_key_id, self.secret_access_key, self.session_token)) and not (
            self.access_key_id and self.secret_access_key
        ):
            raise ValueError("inline S3 credentials require access_key_id and secret_access_key")
        return self


class IcebergCatalogConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["rest"]
    uri: str
    token: _IcebergCredential
    warehouse: str | None = Field(default=None, min_length=1)
    ca_file: str | None = Field(default=None, min_length=1)
    s3: IcebergS3Config | None = None

    @field_validator("uri")
    @classmethod
    def validate_uri(cls, value: str) -> str:
        return _validate_iceberg_endpoint(value, ("https",))

    @field_validator("token")
    @classmethod
    def validate_bearer_token(cls, value: SecretStr) -> SecretStr:
        if any(char.isspace() or ord(char) < 32 for char in value.get_secret_value()):
            raise ValueError("bearer token must not contain whitespace or control characters")
        return value


class IcebergEmitterParams(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    catalog: IcebergCatalogConfig
    table: str = Field(pattern=r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$")


class IcebergEmitterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    type: Literal["iceberg"] = "iceberg"
    name: str = "iceberg"
    aggregation: None = None
    params: IcebergEmitterParams
    lookback_days: int | None = Field(default=None, gt=0)

    @field_validator("lookback_days", mode="before")
    @classmethod
    def validate_lookback_days(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("lookback_days must be a positive number of days")
        return value


def _chargeback_emitter_type(value: object) -> str:
    emitter_type = value.get("type") if isinstance(value, Mapping) else getattr(value, "type", None)
    return "iceberg" if emitter_type == "iceberg" else "emitter"


_ChargebackEmitterSpec = Annotated[
    Annotated[IcebergEmitterSpec, Tag("iceberg")] | Annotated[EmitterSpec, Tag("emitter")],
    Discriminator(_chargeback_emitter_type),
]


_MIN_GRANULARITY_HOURS = 1  # sub-hour cadences not supported


class PluginSettingsBase(BaseModel):
    """Orchestrator-consumed plugin settings. All plugin configs must extend this."""

    model_config = ConfigDict(extra="allow", hide_input_in_errors=True)  # plugin-specific fields pass through

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
    emitters: list[_ChargebackEmitterSpec] = Field(default_factory=list)
    chargeback_granularity: Literal["hourly", "daily", "monthly"] = Field(
        default="daily",
        description=(
            "Granularity of billing data produced by the plugin's CostInput. "
            "Controls emitter aggregation validation — emitters may not request "
            "finer granularity than the chargeback data provides."
        ),
    )

    @field_validator("emitters")
    @classmethod
    def validate_iceberg_count(cls, specs: list[_ChargebackEmitterSpec]) -> list[_ChargebackEmitterSpec]:
        if sum(isinstance(spec, IcebergEmitterSpec) for spec in specs) > 1:
            raise ValueError("only one Iceberg emitter is allowed per tenant")
        return specs

    @model_validator(mode="before")
    @classmethod
    def validate_topic_attribution_emitters(cls, data: object) -> object:
        if isinstance(data, Mapping):
            topic = data.get("topic_attribution")
            emitters = topic.get("emitters") if isinstance(topic, Mapping) else getattr(topic, "emitters", None)
            if isinstance(emitters, (list, tuple)) and any(_chargeback_emitter_type(e) == "iceberg" for e in emitters):
                raise ValueError("topic_attribution.emitters does not support Iceberg")
        return data

    @property
    def iceberg_emitter(self) -> IcebergEmitterSpec | None:
        return next((spec for spec in self.emitters if isinstance(spec, IcebergEmitterSpec)), None)

    @property
    def ordinary_emitters(self) -> list[EmitterSpec]:
        return [spec for spec in self.emitters if isinstance(spec, EmitterSpec)]

    def plugin_payload(self) -> dict[str, Any]:
        iceberg_indices = {index for index, spec in enumerate(self.emitters) if isinstance(spec, IcebergEmitterSpec)}
        return self.model_dump(exclude={"emitters": iceberg_indices})


class TenantConfig(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

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
    retention_days: int = Field(default=250, gt=0, le=MAX_SOURCE_RETENTION_DAYS)
    allocation_retry_limit: int = Field(default=3, gt=0, le=10)
    topic_attribution_retry_limit: int = Field(default=3, gt=0, le=10)
    zero_gather_deletion_threshold: int = Field(default=-1, ge=-1)
    gather_failure_threshold: int = Field(default=5, gt=0)
    tenant_execution_timeout_seconds: int = Field(default=3600, ge=0)
    metrics_prefetch_workers: int = Field(default=4, ge=1, le=20)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    plugin_settings: PluginSettingsBase = Field(default_factory=PluginSettingsBase)
    focus_preview: FocusPreviewTenantConfig | None = None

    @property
    def focus_preview_enabled(self) -> bool:
        return self.ecosystem == "confluent_cloud" and self.focus_preview is not None

    @model_validator(mode="after")
    def validate_lookback_gt_cutoff(self) -> TenantConfig:
        if self.lookback_days <= self.cutoff_days:
            raise ValueError("lookback_days must be > cutoff_days")
        return self


class AppSettings(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    preview: PreviewConfig = Field(default_factory=PreviewConfig)
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

    @property
    def focus_preview_enabled(self) -> bool:
        return any(tenant.focus_preview_enabled for tenant in self.tenants.values())

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
