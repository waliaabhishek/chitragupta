from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

from core.config.models import PluginSettingsBase
from core.metrics.config import MetricsConnectionConfig  # noqa: TC001 — Pydantic evaluates field annotations at runtime


class CostQuantityFixed(BaseModel):
    """Fixed instance count — e.g., 3 broker-hours per hour."""

    type: Literal["fixed"]
    count: int = Field(gt=0)


class CostQuantityStorageGib(BaseModel):
    """Average bytes from PromQL -> GiB-hours. Use for storage costs."""

    type: Literal["storage_gib"]
    query: str  # cluster-wide PromQL, no {} placeholder needed


class CostQuantityNetworkGib(BaseModel):
    """Sum of bytes from PromQL -> GiB. Use for ingress/egress costs."""

    type: Literal["network_gib"]
    query: str  # cluster-wide PromQL, no {} placeholder needed


CostQuantityConfig = Annotated[
    CostQuantityFixed | CostQuantityStorageGib | CostQuantityNetworkGib,
    Field(discriminator="type"),
]


class CostTypeConfig(BaseModel):
    """One billable cost type: name, rate, quantity source, allocation strategy."""

    name: str  # product_type in billing lines, e.g. "PG_COMPUTE"
    product_category: str  # e.g. "postgres"
    rate: Decimal  # unit price (Decimal for precision)
    cost_quantity: CostQuantityConfig
    allocation_strategy: Literal["even_split", "usage_ratio"]
    # Required when allocation_strategy="usage_ratio":
    allocation_query: str | None = None  # per-identity PromQL
    allocation_label: str | None = None  # label to extract from allocation_query rows

    @model_validator(mode="after")
    def validate_usage_ratio_fields(self) -> CostTypeConfig:
        if self.allocation_strategy == "usage_ratio":
            if not self.allocation_query:
                msg = "allocation_query required when allocation_strategy='usage_ratio'"
                raise ValueError(msg)
            if not self.allocation_label:
                msg = "allocation_label required when allocation_strategy='usage_ratio'"
                raise ValueError(msg)
        return self


class StaticIdentityConfig(BaseModel):
    identity_id: str
    identity_type: str
    display_name: str | None = None
    team: str | None = None


class GenericIdentitySourceConfig(BaseModel):
    source: Literal["prometheus", "static", "both"] = "prometheus"
    label: str = "principal"  # Prometheus label used as identity ID
    discovery_query: str | None = None  # PromQL whose results have `label` in labels
    principal_to_team: dict[str, str] = {}
    default_team: str = "UNASSIGNED"
    static_identities: list[StaticIdentityConfig] = []

    @model_validator(mode="after")
    def validate_discovery_query(self) -> GenericIdentitySourceConfig:
        if self.source in ("prometheus", "both") and not self.discovery_query:
            msg = "discovery_query required when source includes 'prometheus'"
            raise ValueError(msg)
        return self


class GenericMetricsOnlyConfig(PluginSettingsBase):
    """YAML config for GenericMetricsOnlyPlugin.

    ecosystem_name is used as the ecosystem label in all billing data.
    The PluginRegistry key is always "generic_metrics_only".
    """

    ecosystem_name: str  # e.g. "self_managed_postgres" -- used in billing lines
    cluster_id: str  # resource_id for the cluster resource
    display_name: str | None = None
    metrics: MetricsConnectionConfig
    identity_source: GenericIdentitySourceConfig = Field(default_factory=GenericIdentitySourceConfig)
    cost_types: list[CostTypeConfig] = Field(min_length=1)

    @classmethod
    def from_plugin_settings(cls, settings: dict[str, Any]) -> GenericMetricsOnlyConfig:
        return cls.model_validate(settings)
