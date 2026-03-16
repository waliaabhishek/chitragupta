from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, SecretStr, model_validator

from core.config.models import PluginSettingsBase
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


class CCloudPluginConfig(PluginSettingsBase):
    """Validates TenantConfig.plugin_settings for ecosystem='confluent_cloud'."""

    ccloud_api: CCloudCredentials
    billing_api: CCloudBillingConfig = Field(default_factory=CCloudBillingConfig)
    metrics: MetricsConnectionConfig | None = None
    flink: list[CCloudFlinkRegionConfig] | None = None

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

    @classmethod
    def from_plugin_settings(cls, settings: dict[str, Any]) -> CCloudPluginConfig:
        """Validate and parse plugin_settings dict."""
        logger.debug("Validating CCloudPluginConfig")
        try:
            return cls.model_validate(settings)
        except Exception:
            logger.exception("CCloudPluginConfig validation failed")
            raise
