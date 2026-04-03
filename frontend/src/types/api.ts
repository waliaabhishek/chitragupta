/**
 * TypeScript types matching src/core/api/schemas.py
 *
 * TD-040: These types are manually maintained. To auto-generate from OpenAPI:
 *   1. Start the backend: `uv run python -m main --mode api`
 *   2. Run: `npm run generate:types`
 *   3. Generated types will be in `src/types/api.generated.ts`
 */

// --- Pagination ---

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  pages: number;
}

// --- Tenant ---

export interface TenantStatusSummary {
  tenant_name: string;
  tenant_id: string;
  ecosystem: string;
  dates_pending: number;
  dates_calculated: number;
  last_calculated_date: string | null;
  topic_attribution_enabled: boolean;
}

export interface TenantListResponse {
  tenants: TenantStatusSummary[];
}

export interface PipelineStateResponse {
  tracking_date: string;
  billing_gathered: boolean;
  resources_gathered: boolean;
  chargeback_calculated: boolean;
  topic_overlay_gathered?: boolean; // absent for tenants without topic attribution
  topic_attribution_calculated?: boolean; // absent for tenants without topic attribution
}

export interface TenantStatusDetailResponse {
  tenant_name: string;
  tenant_id: string;
  ecosystem: string;
  states: PipelineStateResponse[];
  topic_attribution_enabled: boolean;
}

// --- Resource ---

export interface ResourceResponse {
  ecosystem: string;
  tenant_id: string;
  resource_id: string;
  resource_type: string;
  display_name: string | null;
  parent_id: string | null;
  owner_id: string | null;
  status: string;
  created_at: string | null;
  deleted_at: string | null;
  last_seen_at: string | null;
  metadata: Record<string, unknown>;
}

// --- Identity ---

export interface IdentityResponse {
  ecosystem: string;
  tenant_id: string;
  identity_id: string;
  identity_type: string;
  display_name: string | null;
  created_at: string | null;
  deleted_at: string | null;
  last_seen_at: string | null;
  metadata: Record<string, unknown>;
}

// --- Billing ---

export interface BillingLineResponse {
  ecosystem: string;
  tenant_id: string;
  timestamp: string;
  resource_id: string;
  product_category: string;
  product_type: string;
  quantity: string;
  unit_price: string;
  total_cost: string;
  currency: string;
  granularity: string;
  metadata: Record<string, unknown>;
}

// --- Chargeback ---

export interface ChargebackResponse {
  dimension_id: number | null;
  ecosystem: string;
  tenant_id: string;
  timestamp: string;
  resource_id: string | null;
  product_category: string;
  product_type: string;
  identity_id: string;
  cost_type: string;
  amount: string;
  allocation_method: string | null;
  allocation_detail: string | null;
  tags: Record<string, string>;
  metadata: Record<string, unknown>;
}

// --- Data Availability ---

export interface ChargebackDatesResponse {
  dates: string[];
}

// --- Allocation Issues ---

export interface AllocationIssueResponse {
  ecosystem: string;
  resource_id: string | null;
  product_type: string;
  identity_id: string;
  allocation_detail: string;
  row_count: number;
  usage_cost: string;
  shared_cost: string;
  total_cost: string;
}

// --- Entity Tag ---

export interface EntityTagResponse {
  tag_id: number;
  tenant_id: string;
  entity_type: string;
  entity_id: string;
  tag_key: string;
  tag_value: string;
  created_by: string;
  created_at: string | null;
}

export interface EntityTagCreateRequest {
  tag_key: string;
  tag_value: string;
  created_by: string;
}

export interface EntityTagUpdateRequest {
  tag_value: string;
}

// --- Pipeline ---

export interface PipelineResultSummary {
  dates_gathered: number;
  dates_calculated: number;
  chargeback_rows_written: number;
  errors: string[];
  completed_at: string;
}

export interface PipelineRunResponse {
  tenant_name: string;
  status: string;
  message: string;
}

export interface PipelineStatusResponse {
  tenant_name: string;
  is_running: boolean;
  last_run: string | null;
  last_result: PipelineResultSummary | null;
}

// --- Aggregation ---

export interface AggregationBucket {
  dimensions: Record<string, string>;
  time_bucket: string;
  total_amount: string;
  usage_amount: string;
  shared_amount: string;
  row_count: number;
}

export interface AggregationResponse {
  buckets: AggregationBucket[];
  total_amount: string;
  usage_amount: string;
  shared_amount: string;
  total_rows: number;
}

// --- Topic Attribution ---

export interface TopicAttributionResponse {
  dimension_id: number | null;
  ecosystem: string;
  tenant_id: string;
  timestamp: string; // ISO datetime
  env_id: string;
  cluster_resource_id: string;
  topic_name: string;
  product_category: string;
  product_type: string;
  attribution_method: string;
  amount: string; // Decimal as string
}

export interface TopicAttributionAggregationBucket {
  dimensions: Record<string, string>;
  time_bucket: string; // "2026-01-01" (day) | "2026-W01" (week) | "2026-01" (month)
  total_amount: string; // Decimal as string
  row_count: number;
}

export interface TopicAttributionAggregationResponse {
  buckets: TopicAttributionAggregationBucket[];
  total_amount: string;
  total_rows: number;
}

// --- Inventory ---

export interface TypeStatusCounts {
  total: number;
  active: number;
  deleted: number;
}

export interface InventorySummaryResponse {
  resource_counts: Record<string, TypeStatusCounts>;
  identity_counts: Record<string, TypeStatusCounts>;
}

// --- Chargeback Dimension ---

export interface ChargebackDimensionResponse {
  dimension_id: number;
  ecosystem: string;
  tenant_id: string;
  resource_id: string | null;
  product_category: string;
  product_type: string;
  identity_id: string;
  cost_type: string;
  allocation_method: string | null;
  allocation_detail: string | null;
  tags: Record<string, string>;
}

// --- Health ---

export interface HealthResponse {
  status: string;
  version: string;
}

// --- Readiness ---

export interface TenantReadiness {
  tenant_name: string;
  tables_ready: boolean;
  has_data: boolean;
  pipeline_running: boolean;
  pipeline_stage: string | null;
  pipeline_current_date: string | null;
  last_run_status: string | null;
  last_run_at: string | null;
  permanent_failure: string | null;
  topic_attribution_enabled: boolean;
}

export interface ReadinessResponse {
  status: "ready" | "initializing" | "no_data" | "error";
  version: string;
  mode: string;
  tenants: TenantReadiness[];
}
