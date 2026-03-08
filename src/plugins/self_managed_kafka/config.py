"""Configuration models for the self-managed Kafka plugin."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, model_validator

from core.config.models import PluginSettingsBase
from core.metrics.config import MetricsConnectionConfig  # noqa: TC001 — Pydantic evaluates field annotations at runtime

logger = logging.getLogger(__name__)


class CostRateOverride(BaseModel):
    """Override cost rates for a specific region."""

    compute_hourly_rate: Decimal | None = None
    storage_per_gib_hourly: Decimal | None = None
    network_ingress_per_gib: Decimal | None = None
    network_egress_per_gib: Decimal | None = None


class CostModelConfig(BaseModel):
    """Infrastructure cost model for self-managed Kafka."""

    compute_hourly_rate: Decimal  # Per broker-hour
    storage_per_gib_hourly: Decimal  # Per GiB-hour (1 GiB = 2^30 bytes)
    network_ingress_per_gib: Decimal  # Per GiB
    network_egress_per_gib: Decimal  # Per GiB
    region_overrides: dict[str, CostRateOverride] = {}


class StaticIdentityConfig(BaseModel):
    """A statically-defined identity (no Prometheus discovery needed)."""

    identity_id: str  # e.g., "User:alice" or "team-data-eng"
    identity_type: str  # "principal", "team", "service_account"
    display_name: str | None = None
    team: str | None = None  # Optional team mapping


class IdentitySourceConfig(BaseModel):
    """Configuration for identity discovery."""

    source: Literal["prometheus", "static", "both"] = "prometheus"
    principal_to_team: dict[str, str] = {}  # "User:alice" → "team-data"
    default_team: str = "UNASSIGNED"
    static_identities: list[StaticIdentityConfig] = []  # For source="static" or "both"


class ResourceSourceConfig(BaseModel):
    """Configuration for resource discovery."""

    source: Literal["prometheus", "admin_api"] = "prometheus"
    # Admin API settings (only used if source="admin_api")
    bootstrap_servers: str | None = None
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"

    @model_validator(mode="after")
    def validate_admin_api_settings(self) -> ResourceSourceConfig:
        """If source=admin_api, require bootstrap_servers."""
        if self.source == "admin_api" and not self.bootstrap_servers:
            raise ValueError("bootstrap_servers required when source='admin_api'")
        return self


class SelfManagedKafkaConfig(PluginSettingsBase):
    """Validates plugin_settings for ecosystem='self_managed_kafka'."""

    cluster_id: str  # Logical identifier for this cluster (used as resource_id in billing)
    broker_count: int = Field(gt=0)  # Used for compute cost calculation
    region: str | None = None  # Optional region for cost overrides
    cost_model: CostModelConfig
    identity_source: IdentitySourceConfig = Field(default_factory=IdentitySourceConfig)
    resource_source: ResourceSourceConfig = Field(default_factory=ResourceSourceConfig)
    metrics: MetricsConnectionConfig  # Required for cost construction + allocation
    discovery_window_hours: int = Field(default=1, gt=0)

    @classmethod
    def from_plugin_settings(cls, settings: dict[str, Any]) -> SelfManagedKafkaConfig:
        """Validate and parse plugin_settings dict."""
        return cls.model_validate(settings)

    def get_effective_cost_model(self) -> CostModelConfig:
        """Return cost model with region overrides applied, if applicable."""
        if self.region is None or self.region not in self.cost_model.region_overrides:
            return self.cost_model

        override = self.cost_model.region_overrides[self.region]
        return CostModelConfig(
            compute_hourly_rate=override.compute_hourly_rate
            if override.compute_hourly_rate is not None
            else self.cost_model.compute_hourly_rate,
            storage_per_gib_hourly=override.storage_per_gib_hourly
            if override.storage_per_gib_hourly is not None
            else self.cost_model.storage_per_gib_hourly,
            network_ingress_per_gib=override.network_ingress_per_gib
            if override.network_ingress_per_gib is not None
            else self.cost_model.network_ingress_per_gib,
            network_egress_per_gib=override.network_egress_per_gib
            if override.network_egress_per_gib is not None
            else self.cost_model.network_egress_per_gib,
        )
