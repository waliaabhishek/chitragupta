// TypeScript types matching src/core/api/schemas.py

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
}

export interface TenantListResponse {
  tenants: TenantStatusSummary[];
}

export interface PipelineStateResponse {
  tracking_date: string;
  billing_gathered: boolean;
  resources_gathered: boolean;
  chargeback_calculated: boolean;
}

export interface TenantStatusDetailResponse {
  tenant_name: string;
  tenant_id: string;
  ecosystem: string;
  states: PipelineStateResponse[];
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
  tags: string[];
  metadata: Record<string, unknown>;
}

// --- Tag ---

export interface TagResponse {
  tag_id: number;
  dimension_id: number;
  tag_key: string;
  tag_value: string;
  created_by: string;
  created_at: string | null;
}

export interface TagCreateRequest {
  tag_key: string;
  tag_value: string;
  created_by: string;
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
  row_count: number;
}

export interface AggregationResponse {
  buckets: AggregationBucket[];
  total_amount: string;
  total_rows: number;
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
  tags: TagResponse[];
}

export interface ChargebackDimensionUpdateRequest {
  tags?: TagCreateRequest[];
  add_tags?: TagCreateRequest[];
  remove_tag_ids?: number[];
}

// --- Health ---

export interface HealthResponse {
  status: string;
  version: string;
}
