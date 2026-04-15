from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from core.models.counts import TypeStatusCounts  # noqa: TC001

logger = logging.getLogger(__name__)

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
    topic_attribution_status: Literal["disabled", "enabled", "config_error"]
    topic_attribution_error: str | None = None


class TenantListResponse(BaseModel):
    """Response for listing all tenants."""

    tenants: list[TenantStatusSummary]


class PipelineStateResponse(BaseModel):
    """Pipeline state for a single date."""

    tracking_date: date
    billing_gathered: bool
    resources_gathered: bool
    chargeback_calculated: bool
    topic_overlay_gathered: bool = False
    topic_attribution_calculated: bool = False


class TenantStatusDetailResponse(BaseModel):
    """Detailed pipeline status for a tenant."""

    tenant_name: str
    tenant_id: str
    ecosystem: str
    states: list[PipelineStateResponse]
    topic_attribution_status: Literal["disabled", "enabled", "config_error"]
    topic_attribution_error: str | None = None


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
    tags: dict[str, str]
    metadata: dict[str, Any]

    model_config = ConfigDict(from_attributes=True)


# --- Entity Tag ---


class EntityTagResponse(BaseModel):
    tag_id: int
    tenant_id: str
    entity_type: str
    entity_id: str
    tag_key: str
    tag_value: str
    created_by: str
    created_at: datetime | None

    model_config = ConfigDict(from_attributes=True)


class EntityTagCreateRequest(BaseModel):
    tag_key: str = Field(min_length=1, max_length=100)
    tag_value: str = Field(min_length=1, max_length=500)
    created_by: str = Field(min_length=1, max_length=100)


class EntityTagUpdateRequest(BaseModel):
    tag_value: str = Field(min_length=1, max_length=500)


class BulkTagItem(BaseModel):
    entity_type: str
    entity_id: str
    tag_key: str = Field(min_length=1, max_length=100)
    tag_value: str = Field(min_length=1, max_length=500)


class BulkEntityTagRequest(BaseModel):
    items: list[BulkTagItem] = Field(min_length=1, max_length=10_000)
    override_existing: bool = False
    created_by: str = Field(min_length=1, max_length=100)


class BulkEntityTagResponse(BaseModel):
    created_count: int
    updated_count: int
    skipped_count: int


class BulkTagByFilterRequest(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    timezone: str | None = None
    identity_id: str | None = None
    tag_key: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=500)
    created_by: str = Field(min_length=1, max_length=100)
    override_existing: bool = False


class BulkTagByFilterResponse(BaseModel):
    created_count: int
    updated_count: int
    skipped_count: int
    errors: list[str] = Field(default_factory=list)


class TagKeysResponse(BaseModel):
    keys: list[str]


class TagValuesResponse(BaseModel):
    values: list[str]


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
    usage_amount: Decimal
    shared_amount: Decimal
    row_count: int


class AggregationResponse(BaseModel):
    """Response for server-side aggregation."""

    buckets: list[AggregationBucket]
    total_amount: Decimal
    usage_amount: Decimal
    shared_amount: Decimal
    total_rows: int


class ChargebackDatesResponse(BaseModel):
    """Response for the data availability endpoint."""

    dates: list[date]


class AllocationIssueResponse(BaseModel):
    """Aggregated row where cost allocation failed."""

    ecosystem: str
    env_id: str
    resource_id: str | None
    product_type: str
    identity_id: str
    allocation_detail: str
    row_count: int
    usage_cost: Decimal
    shared_cost: Decimal
    total_cost: Decimal

    model_config = ConfigDict(from_attributes=True)


# --- Export ---


class ExportRequest(BaseModel):
    """Request for CSV export."""

    start_date: date | None = None
    end_date: date | None = None
    timezone: str | None = None
    columns: list[str] | None = None
    filters: dict[str, str] | None = None


# --- Chargeback Dimension ---


class ChargebackDimensionResponse(BaseModel):
    """Response for a chargeback dimension."""

    dimension_id: int
    ecosystem: str
    env_id: str
    tenant_id: str
    resource_id: str | None
    product_category: str
    product_type: str
    identity_id: str
    cost_type: str
    allocation_method: str | None
    allocation_detail: str | None
    tags: dict[str, str]

    model_config = ConfigDict(from_attributes=True)


# --- Inventory ---


class InventorySummaryResponse(BaseModel):
    """Counts of resources and identities grouped by type for a tenant."""

    resource_counts: dict[str, TypeStatusCounts]
    identity_counts: dict[str, TypeStatusCounts]


# --- Health ---


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str


# --- Readiness ---


class TenantReadiness(BaseModel):
    """Per-tenant readiness state for the readiness endpoint."""

    tenant_name: str
    tables_ready: bool
    has_data: bool
    pipeline_running: bool
    pipeline_stage: str | None
    pipeline_current_date: date | None
    last_run_status: str | None
    last_run_at: datetime | None
    permanent_failure: str | None
    topic_attribution_status: Literal["disabled", "enabled", "config_error"]
    topic_attribution_error: str | None = None


class ReadinessResponse(BaseModel):
    """Application readiness response."""

    status: str  # "ready" | "initializing" | "no_data" | "error"
    version: str
    mode: str
    tenants: list[TenantReadiness]


# --- Topic Attribution ---


class TopicAttributionResponse(BaseModel):
    """Single topic attribution row response."""

    dimension_id: int | None
    ecosystem: str
    tenant_id: str
    timestamp: datetime
    env_id: str
    cluster_resource_id: str
    topic_name: str
    product_category: str
    product_type: str
    attribution_method: str
    amount: Decimal

    model_config = ConfigDict(from_attributes=True)


class TopicAttributionAggregationBucket(BaseModel):
    dimensions: dict[str, str]
    time_bucket: str
    total_amount: Decimal
    row_count: int


class TopicAttributionAggregationResponse(BaseModel):
    buckets: list[TopicAttributionAggregationBucket]
    total_amount: Decimal
    total_rows: int


class TopicAttributionDatesResponse(BaseModel):
    dates: list[date]


class CrossReferenceItemSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    resource_type: str
    display_name: str | None
    cost: Decimal


class CrossReferenceGroupSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    resource_type: str
    items: list[CrossReferenceItemSchema]
    total_count: int


class GraphNode(BaseModel):
    id: str
    resource_type: str
    display_name: str | None
    cost: Decimal
    created_at: datetime | None
    deleted_at: datetime | None
    tags: dict[str, str]
    parent_id: str | None
    cloud: str | None
    region: str | None
    status: str
    cross_references: list[CrossReferenceGroupSchema]
    child_count: int | None = None
    child_total_cost: Decimal | None = None

    model_config = ConfigDict(from_attributes=True)


class GraphEdge(BaseModel):
    source: str
    target: str
    relationship_type: str  # EdgeType.value — "parent" | "charge" | "attribution"
    cost: Decimal | None = None

    model_config = ConfigDict(from_attributes=True)


class GraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]

    model_config = ConfigDict(from_attributes=True)


class GraphSearchResult(BaseModel):
    id: str
    resource_type: str
    display_name: str | None
    parent_id: str | None
    parent_display_name: str | None
    status: str

    model_config = ConfigDict(from_attributes=True)


class GraphSearchResponse(BaseModel):
    results: list[GraphSearchResult]

    model_config = ConfigDict(from_attributes=True)


class GraphDiffNode(BaseModel):
    id: str
    resource_type: str
    display_name: str | None
    parent_id: str | None
    cost_before: Decimal
    cost_after: Decimal
    cost_delta: Decimal
    pct_change: Decimal | None
    status: str  # "new" | "deleted" | "changed" | "unchanged"

    model_config = ConfigDict(from_attributes=True)


class GraphDiffResponse(BaseModel):
    nodes: list[GraphDiffNode]

    model_config = ConfigDict(from_attributes=True)


class GraphTimelinePoint(BaseModel):
    date: date
    cost: Decimal

    model_config = ConfigDict(from_attributes=True)


class GraphTimelineResponse(BaseModel):
    entity_id: str
    points: list[GraphTimelinePoint]

    model_config = ConfigDict(from_attributes=True)
