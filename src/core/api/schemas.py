from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# --- Pagination ---


class PaginatedResponse[T](BaseModel):
    """Generic paginated response."""

    items: list[T]
    total: int
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=1000)
    pages: int = Field(ge=0)

    model_config = ConfigDict(from_attributes=True)


# --- Tenant ---


class TenantStatusSummary(BaseModel):
    """Summary of a tenant's pipeline status."""

    tenant_name: str
    tenant_id: str
    ecosystem: str
    dates_pending: int
    dates_calculated: int
    last_calculated_date: date | None


class TenantListResponse(BaseModel):
    """Response for listing all tenants."""

    tenants: list[TenantStatusSummary]


class PipelineStateResponse(BaseModel):
    """Pipeline state for a single date."""

    tracking_date: date
    billing_gathered: bool
    resources_gathered: bool
    chargeback_calculated: bool


class TenantStatusDetailResponse(BaseModel):
    """Detailed pipeline status for a tenant."""

    tenant_name: str
    tenant_id: str
    ecosystem: str
    states: list[PipelineStateResponse]


# --- Resource ---


class ResourceResponse(BaseModel):
    """Response for a single resource."""

    ecosystem: str
    tenant_id: str
    resource_id: str
    resource_type: str
    display_name: str | None
    parent_id: str | None
    owner_id: str | None
    status: str
    created_at: datetime | None
    deleted_at: datetime | None
    last_seen_at: datetime | None
    metadata: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


# --- Identity ---


class IdentityResponse(BaseModel):
    """Response for a single identity."""

    ecosystem: str
    tenant_id: str
    identity_id: str
    identity_type: str
    display_name: str | None
    created_at: datetime | None
    deleted_at: datetime | None
    last_seen_at: datetime | None
    metadata: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


# --- Billing ---


class BillingLineResponse(BaseModel):
    """Response for a single billing line item."""

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    resource_id: str
    product_category: str
    product_type: str
    quantity: Decimal
    unit_price: Decimal
    total_cost: Decimal
    currency: str
    granularity: str
    metadata: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


# --- Chargeback ---


class ChargebackResponse(BaseModel):
    """Response for a single chargeback row."""

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    resource_id: str | None
    product_category: str
    product_type: str
    identity_id: str
    cost_type: str
    amount: Decimal
    allocation_method: str | None
    allocation_detail: str | None
    tags: list[str]
    metadata: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


# --- Health ---


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
