from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, SecretStr, model_validator

from core.config.models import PluginSettingsBase
from core.metrics.config import MetricsConnectionConfig  # noqa: TC001 — Pydantic evaluates field annotations at runtime


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


class CCloudPluginConfig(PluginSettingsBase):
    """Validates TenantConfig.plugin_settings for ecosystem='confluent_cloud'."""

    ccloud_api: CCloudCredentials
    billing_api: CCloudBillingConfig = Field(default_factory=CCloudBillingConfig)
    metrics: MetricsConnectionConfig | None = None
    flink: list[CCloudFlinkRegionConfig] | None = None

    @model_validator(mode="after")
    def validate_allocator_params(self) -> CCloudPluginConfig:
        """Validate that ratio params are numeric."""
        for key, value in self.allocator_params.items():
            if key.endswith("_ratio") and not isinstance(value, (int, float)):
                raise ValueError(f"allocator_params.{key} must be numeric, got {type(value).__name__}")
        return self

    @classmethod
    def from_plugin_settings(cls, settings: dict[str, Any]) -> CCloudPluginConfig:
        """Validate and parse plugin_settings dict."""
        return cls.model_validate(settings)
