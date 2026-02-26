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

    dimension_id: int | None
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


# --- Tag ---


class TagResponse(BaseModel):
    """Response for a single custom tag."""

    tag_id: int
    dimension_id: int
    tag_key: str
    tag_value: str
    display_name: str
    created_by: str
    created_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class TagWithDimensionResponse(BaseModel):
    """Extended tag response with denormalized dimension context for display."""

    tag_id: int
    dimension_id: int
    tag_key: str
    tag_value: str
    display_name: str
    created_by: str
    created_at: datetime | None
    # Denormalized dimension context
    identity_id: str
    product_type: str
    resource_id: str | None


class TagCreateRequest(BaseModel):
    """Request to create a custom tag. Backend auto-generates tag_value = uuid4()."""

    tag_key: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=500)
    created_by: str = Field(min_length=1, max_length=100)


class TagUpdateRequest(BaseModel):
    """Request to update a tag's display name."""

    display_name: str = Field(min_length=1, max_length=500)


class BulkTagRequest(BaseModel):
    """Request to bulk-add tags by dimension IDs."""

    dimension_ids: list[int] = Field(min_length=1, max_length=1000)
    tag_key: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=500)
    created_by: str = Field(min_length=1, max_length=100)
    override_existing: bool = False


class BulkTagByFilterRequest(BaseModel):
    """Request to bulk-add tags by chargeback filters."""

    start_date: date | None = None
    end_date: date | None = None
    identity_id: str | None = None
    product_type: str | None = None
    resource_id: str | None = None
    cost_type: str | None = None
    tag_key: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=500)
    created_by: str = Field(min_length=1, max_length=100)
    override_existing: bool = False


class BulkTagResponse(BaseModel):
    """Result of a bulk tag operation."""

    created_count: int
    updated_count: int
    skipped_count: int
    errors: list[str]


# --- Pipeline ---


class PipelineResultSummary(BaseModel):
    """Summary of a completed pipeline run."""

    dates_gathered: int
    dates_calculated: int
    chargeback_rows_written: int
    errors: list[str]
    completed_at: datetime


class PipelineRunResponse(BaseModel):
    """Response when triggering a pipeline run."""

    tenant_name: str
    status: str
    message: str


class PipelineStatusResponse(BaseModel):
    """Response for pipeline run status."""

    tenant_name: str
    is_running: bool
    last_run: datetime | None
    last_result: PipelineResultSummary | None


# --- Aggregation ---


class AggregationBucket(BaseModel):
    """A single bucket in an aggregation response."""

    dimensions: dict[str, str]
    time_bucket: str
    total_amount: Decimal
    row_count: int


class AggregationResponse(BaseModel):
    """Response for server-side aggregation."""

    buckets: list[AggregationBucket]
    total_amount: Decimal
    total_rows: int


# --- Export ---


class ExportRequest(BaseModel):
    """Request for CSV export."""

    start_date: date | None = None
    end_date: date | None = None
    columns: list[str] | None = None
    filters: dict[str, str] | None = None


# --- Chargeback Dimension ---


class ChargebackDimensionResponse(BaseModel):
    """Response for a chargeback dimension."""

    dimension_id: int
    ecosystem: str
    tenant_id: str
    resource_id: str | None
    product_category: str
    product_type: str
    identity_id: str
    cost_type: str
    allocation_method: str | None
    allocation_detail: str | None
    tags: list[TagResponse]

    model_config = ConfigDict(from_attributes=True)


class ChargebackDimensionUpdateRequest(BaseModel):
    """Request to update tags/annotations on a chargeback dimension."""

    tags: list[TagCreateRequest] | None = None
    add_tags: list[TagCreateRequest] | None = None
    remove_tag_ids: list[int] | None = None


# --- Health ---


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
