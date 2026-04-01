from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator

from core.config.models import EmitterSpec, PluginSettingsBase
from core.metrics.config import MetricsConnectionConfig  # noqa: TC001 — Pydantic evaluates field annotations at runtime
from plugins.confluent_cloud.allocation_models import (
    _DEFAULT_CKU_SHARED_RATIO,
    _DEFAULT_CKU_USAGE_RATIO,
)

logger = logging.getLogger(__name__)

_CKU_RATIO_SUM_TOLERANCE = Decimal("0.0001")  # must match CompositionModel.__post_init__ tolerance


class CCloudCredentials(BaseModel):
    """API key/secret pair for CCloud authentication."""

    key: str
    secret: SecretStr


class CCloudBillingConfig(BaseModel):
    """Configuration for CCloud billing API."""

    days_per_query: int = Field(default=15, gt=0, le=30)


class CCloudFlinkRegionConfig(BaseModel):
    """Per-region Flink API credentials."""

    region_id: str
    key: str
    secret: SecretStr


class TopicAttributionConfig(BaseModel):
    """Configuration for topic attribution overlay."""

    enabled: bool = False
    exclude_topic_patterns: list[str] = Field(
        default_factory=lambda: ["__consumer_offsets", "_schemas", "_confluent-*"],
    )
    missing_metrics_behavior: Literal["even_split", "skip"] = "even_split"
    cost_mapping_overrides: dict[str, str] = Field(default_factory=dict)
    metric_name_overrides: dict[str, str] = Field(default_factory=dict)
    retention_days: int = Field(default=90, gt=0, le=365)
    emitters: list[EmitterSpec] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _apply_emitter_defaults(cls, data: Any) -> Any:
        """Inject TA-specific defaults into emitter params before validation.

        Preserves backwards compatibility: users who relied on the old
        TopicAttributionCsvEmitter filename and output_dir defaults get
        identical behavior without specifying these in their YAML config.
        Users can override via topic_attribution.emitters[].params.
        """
        if isinstance(data, dict) and "emitters" in data:
            for spec in data["emitters"]:
                if isinstance(spec, dict) and spec.get("type") == "csv":
                    params = spec.setdefault("params", {})
                    params.setdefault("output_dir", "/tmp/topic_attribution")
                    params.setdefault("filename_template", "topic_attr_{tenant_id}_{date}.csv")
        return data

    @field_validator("missing_metrics_behavior", mode="before")
    @classmethod
    def normalize_missing_metrics_behavior(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("cost_mapping_overrides", mode="before")
    @classmethod
    def normalize_and_validate_cost_mappings(cls, v: dict[str, str]) -> dict[str, str]:
        valid_methods = {"bytes_ratio", "retained_bytes_ratio", "even_split", "disabled"}
        normalized: dict[str, str] = {}
        for product_type, method in v.items():
            pt = product_type.strip().upper()
            m = method.strip().lower()
            if m not in valid_methods:
                raise ValueError(f"cost_mapping_overrides[{pt!r}]: must be one of {valid_methods}, got {method!r}")
            normalized[pt] = m
        return normalized

    @field_validator("metric_name_overrides", mode="before")
    @classmethod
    def normalize_and_validate_metric_overrides(cls, v: dict[str, str]) -> dict[str, str]:
        valid_keys = {"topic_bytes_in", "topic_bytes_out", "topic_retained_bytes"}
        normalized: dict[str, str] = {}
        for key, metric_name in v.items():
            k = key.strip().lower()
            mn = metric_name.strip()
            if k not in valid_keys:
                raise ValueError(f"metric_name_overrides: unknown key {key!r}, valid keys are {valid_keys}")
            if not mn:
                raise ValueError(f"metric_name_overrides[{k!r}]: metric name cannot be empty")
            normalized[k] = mn
        return normalized

    @field_validator("exclude_topic_patterns", mode="before")
    @classmethod
    def normalize_exclude_patterns(cls, v: list[str]) -> list[str]:
        return [p.strip() for p in v if p.strip()]


class CCloudPluginConfig(PluginSettingsBase):
    """Validates TenantConfig.plugin_settings for ecosystem='confluent_cloud'."""

    ccloud_api: CCloudCredentials
    billing_api: CCloudBillingConfig = Field(default_factory=CCloudBillingConfig)
    metrics: MetricsConnectionConfig | None = None
    flink: list[CCloudFlinkRegionConfig] | None = None
    topic_attribution: TopicAttributionConfig = Field(default_factory=TopicAttributionConfig)

    @model_validator(mode="after")
    def validate_allocator_params(self) -> CCloudPluginConfig:
        """Validate that ratio params are numeric and CKU ratios sum to 1.0."""
        for key, value in self.allocator_params.items():
            if key.endswith("_ratio") and not isinstance(value, (int, float)):
                raise ValueError(f"allocator_params.{key} must be numeric, got {type(value).__name__}")
        if "kafka_cku_usage_ratio" in self.allocator_params or "kafka_cku_shared_ratio" in self.allocator_params:
            usage = Decimal(str(self.allocator_params.get("kafka_cku_usage_ratio", _DEFAULT_CKU_USAGE_RATIO)))
            shared = Decimal(str(self.allocator_params.get("kafka_cku_shared_ratio", _DEFAULT_CKU_SHARED_RATIO)))
            if abs(usage + shared - Decimal("1")) > _CKU_RATIO_SUM_TOLERANCE:
                total = usage + shared
                raise ValueError(
                    f"allocator_params kafka_cku_usage_ratio + kafka_cku_shared_ratio must sum to 1.0, got {total}"
                )
        return self

    @model_validator(mode="after")
    def validate_topic_attribution_requires_metrics(self) -> CCloudPluginConfig:
        """Topic attribution requires a configured metrics source."""
        if self.topic_attribution.enabled and self.metrics is None:
            raise ValueError(
                "topic_attribution.enabled=True requires a configured metrics source; "
                "set plugin_settings.metrics or disable topic_attribution"
            )
        return self

    @classmethod
    def from_plugin_settings(cls, settings: dict[str, Any]) -> CCloudPluginConfig:
        """Validate and parse plugin_settings dict."""
        logger.debug("Validating CCloudPluginConfig")
        try:
            return cls.model_validate(settings)
        except Exception:
            logger.exception("CCloudPluginConfig validation failed")
            raise
