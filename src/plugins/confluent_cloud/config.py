from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, model_validator


class CCloudCredentials(BaseModel):
    """API key/secret pair for CCloud authentication."""

    key: str
    secret: SecretStr


class CCloudBillingConfig(BaseModel):
    """Configuration for CCloud billing API."""

    days_per_query: int = Field(default=15, gt=0, le=30)


class CCloudMetricsConfig(BaseModel):
    """Configuration for metrics source (Prometheus)."""

    type: Literal["prometheus"] = "prometheus"
    url: str
    auth_type: Literal["basic", "bearer", "none"] = "none"
    username: str | None = None
    password: SecretStr | None = None
    bearer_token: SecretStr | None = None

    @model_validator(mode="after")
    def validate_auth_credentials(self) -> CCloudMetricsConfig:
        if self.auth_type == "basic":
            if not self.username or not self.password:
                raise ValueError("username and password required for basic auth")
        elif self.auth_type == "bearer":
            if not self.bearer_token:
                raise ValueError("bearer_token required for bearer auth")
        elif self.auth_type == "none" and (self.username or self.password or self.bearer_token):
            raise ValueError("credentials provided but auth_type is 'none'")
        return self


class CCloudFlinkRegionConfig(BaseModel):
    """Per-region Flink API credentials."""

    region_id: str
    key: str
    secret: SecretStr


class CCloudPluginConfig(BaseModel):
    """Validates TenantConfig.plugin_settings for ecosystem='confluent_cloud'."""

    ccloud_api: CCloudCredentials
    billing_api: CCloudBillingConfig = Field(default_factory=CCloudBillingConfig)
    metrics: CCloudMetricsConfig | None = None
    flink: list[CCloudFlinkRegionConfig] | None = None
    allocator_params: dict[str, float | int | str | bool] = Field(default_factory=dict)

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
