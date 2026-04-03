import { API_URL } from "../config";
import type {
  PaginatedResponse,
  TopicAttributionResponse,
  TopicAttributionAggregationResponse,
} from "../types/api";

// --- Parameter types ---

export interface FetchTopicAttributionsParams {
  page?: number;
  page_size?: number;
  start_date?: string;
  end_date?: string;
  timezone?: string;
  cluster_resource_id?: string;
  topic_name?: string;
  product_type?: string;
  attribution_method?: string;
}

export interface FetchTopicAttributionAggregationParams {
  group_by: string[];
  time_bucket: "day" | "week" | "month";
  start_date: string;
  end_date: string;
  timezone?: string;
  cluster_resource_id?: string;
  topic_name?: string;
  product_type?: string;
}

export interface ExportTopicAttributionsParams {
  start_date?: string;
  end_date?: string;
  timezone?: string;
}

export interface TopicAttributionDatesResponse {
  dates: string[];
}

// --- Functions ---

/** 1. Paginated list — used by TopicAttributionGrid datasource */
export async function fetchTopicAttributions(
  tenantName: string,
  params: FetchTopicAttributionsParams,
  signal?: AbortSignal,
): Promise<PaginatedResponse<TopicAttributionResponse>> {
  const qs = new URLSearchParams();
  if (params.page !== undefined) qs.set("page", String(params.page));
  if (params.page_size !== undefined)
    qs.set("page_size", String(params.page_size));
  if (params.start_date) qs.set("start_date", params.start_date);
  if (params.end_date) qs.set("end_date", params.end_date);
  if (params.timezone) qs.set("timezone", params.timezone);
  if (params.cluster_resource_id)
    qs.set("cluster_resource_id", params.cluster_resource_id);
  if (params.topic_name) qs.set("topic_name", params.topic_name);
  if (params.product_type) qs.set("product_type", params.product_type);
  if (params.attribution_method)
    qs.set("attribution_method", params.attribution_method);

  const response = await fetch(
    `${API_URL}/tenants/${tenantName}/topic-attributions?${qs.toString()}`,
    { signal },
  );
  if (!response.ok)
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  return response.json() as Promise<
    PaginatedResponse<TopicAttributionResponse>
  >;
}

/** 2. Aggregation — used by useTopicAttributionAggregation hook */
export async function fetchTopicAttributionAggregation(
  tenantName: string,
  params: FetchTopicAttributionAggregationParams,
  signal?: AbortSignal,
): Promise<TopicAttributionAggregationResponse> {
  const qs = new URLSearchParams();
  for (const g of params.group_by) {
    qs.append("group_by", g);
  }
  qs.set("time_bucket", params.time_bucket);
  qs.set("start_date", params.start_date);
  qs.set("end_date", params.end_date);
  if (params.timezone) qs.set("timezone", params.timezone);
  if (params.cluster_resource_id)
    qs.set("cluster_resource_id", params.cluster_resource_id);
  if (params.topic_name) qs.set("topic_name", params.topic_name);
  if (params.product_type) qs.set("product_type", params.product_type);

  const response = await fetch(
    `${API_URL}/tenants/${tenantName}/topic-attributions/aggregate?${qs.toString()}`,
    { signal },
  );
  if (!response.ok)
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  return response.json() as Promise<TopicAttributionAggregationResponse>;
}

/** 3. Export — POST with query params (not JSON body); returns Blob for download */
export async function exportTopicAttributions(
  tenantName: string,
  params: ExportTopicAttributionsParams,
): Promise<Blob> {
  const qs = new URLSearchParams();
  if (params.start_date) qs.set("start_date", params.start_date);
  if (params.end_date) qs.set("end_date", params.end_date);
  if (params.timezone) qs.set("timezone", params.timezone);

  const response = await fetch(
    `${API_URL}/tenants/${tenantName}/topic-attributions/export?${qs.toString()}`,
    { method: "POST" },
  );
  if (!response.ok) throw new Error(`Export failed: HTTP ${response.status}`);
  return response.blob();
}

/** 4. Available dates — used to populate date picker available range */
export async function fetchTopicAttributionDates(
  tenantName: string,
  signal?: AbortSignal,
): Promise<TopicAttributionDatesResponse> {
  const response = await fetch(
    `${API_URL}/tenants/${tenantName}/topic-attributions/dates`,
    { signal },
  );
  if (!response.ok)
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  return response.json() as Promise<TopicAttributionDatesResponse>;
}
