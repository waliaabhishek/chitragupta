import { http, HttpResponse } from "msw";
import type {
  AggregationResponse,
  BulkTagResponse,
  InventorySummaryResponse,
  PaginatedResponse,
  ReadinessResponse,
  TagWithDimensionResponse,
  TenantListResponse,
} from "../../types/api";

const BASE = "/api/v1";

export const tenantFixtures: TenantListResponse = {
  tenants: [
    {
      tenant_name: "acme",
      tenant_id: "t-001",
      ecosystem: "ccloud",
      dates_pending: 2,
      dates_calculated: 10,
      last_calculated_date: "2024-01-10",
    },
    {
      tenant_name: "globex",
      tenant_id: "t-002",
      ecosystem: "self_managed",
      dates_pending: 0,
      dates_calculated: 5,
      last_calculated_date: "2024-01-08",
    },
  ],
};

export const chargebackFixtures: PaginatedResponse<unknown> = {
  items: [
    {
      ecosystem: "ccloud",
      tenant_id: "t-001",
      timestamp: "2024-01-10T00:00:00Z",
      resource_id: "r-001",
      product_category: "KAFKA",
      product_type: "KAFKA_NUM_BYTES",
      identity_id: "user@example.com",
      cost_type: "USAGE",
      amount: "12.50",
      allocation_method: "ratio",
      allocation_detail: null,
      tags: [],
      metadata: {},
    },
  ],
  total: 1,
  page: 1,
  page_size: 100,
  pages: 1,
};

export const readinessFixture: ReadinessResponse = {
  status: "ready",
  version: "1.0.0",
  mode: "both",
  tenants: [
    {
      tenant_name: "acme",
      tables_ready: true,
      has_data: true,
      pipeline_running: false,
      pipeline_stage: null,
      pipeline_current_date: null,
      last_run_status: "completed",
      last_run_at: null,
      permanent_failure: null,
    },
    {
      tenant_name: "globex",
      tables_ready: true,
      has_data: true,
      pipeline_running: false,
      pipeline_stage: null,
      pipeline_current_date: null,
      last_run_status: "completed",
      last_run_at: null,
      permanent_failure: null,
    },
  ],
};

export const handlers = [
  http.get(`${BASE}/readiness`, () => {
    return HttpResponse.json(readinessFixture);
  }),

  http.get(`${BASE}/tenants`, () => {
    return HttpResponse.json(tenantFixtures);
  }),

  http.get(`${BASE}/tenants/:tenant/chargebacks`, ({ request }) => {
    const url = new URL(request.url);
    const page = url.searchParams.get("page") ?? "1";
    const pageSize = url.searchParams.get("page_size") ?? "100";
    return HttpResponse.json({
      ...chargebackFixtures,
      page: Number(page),
      page_size: Number(pageSize),
    });
  }),

  http.get(`${BASE}/tenants/:tenant/billing`, () => {
    return HttpResponse.json({
      items: [],
      total: 0,
      page: 1,
      page_size: 100,
      pages: 0,
    });
  }),

  http.get(`${BASE}/tenants/:tenant/resources`, () => {
    return HttpResponse.json({
      items: [],
      total: 0,
      page: 1,
      page_size: 100,
      pages: 0,
    });
  }),

  http.get(`${BASE}/tenants/:tenant/identities`, () => {
    return HttpResponse.json({
      items: [],
      total: 0,
      page: 1,
      page_size: 100,
      pages: 0,
    });
  }),

  http.get(`${BASE}/tenants/:tenant/inventory/summary`, () => {
    const response: InventorySummaryResponse = {
      resource_counts: {
        kafka_cluster: { total: 5, active: 4, deleted: 1 },
        connector: { total: 3, active: 3, deleted: 0 },
      },
      identity_counts: {
        service_account: { total: 12, active: 10, deleted: 2 },
        user: { total: 3, active: 3, deleted: 0 },
      },
    };
    return HttpResponse.json(response);
  }),

  // Must be before /:id to prevent static paths being captured as ID params
  http.get(`${BASE}/tenants/:tenant/chargebacks/dates`, () => {
    return HttpResponse.json({ dates: [] });
  }),

  http.get(`${BASE}/tenants/:tenant/chargebacks/aggregate`, () => {
    const response: AggregationResponse = {
      buckets: [
        {
          dimensions: { identity_id: "user-1" },
          time_bucket: "2026-02-15",
          total_amount: "10.00",
          usage_amount: "8.00",
          shared_amount: "2.00",
          row_count: 1,
        },
        {
          dimensions: { identity_id: "user-2" },
          time_bucket: "2026-02-15",
          total_amount: "5.00",
          usage_amount: "4.00",
          shared_amount: "1.00",
          row_count: 1,
        },
      ],
      total_amount: "15.00",
      usage_amount: "12.00",
      shared_amount: "3.00",
      total_rows: 2,
    };
    return HttpResponse.json(response);
  }),

  http.get(`${BASE}/tenants/:tenant/chargebacks/:id`, ({ params }) => {
    return HttpResponse.json({
      ecosystem: "ccloud",
      tenant_id: "t-001",
      id: params.id,
    });
  }),

  http.post(`${BASE}/tenants/:tenant/chargebacks`, async ({ request }) => {
    const body = await request.json();
    return HttpResponse.json(body, { status: 201 });
  }),

  http.patch(`${BASE}/tenants/:tenant/chargebacks/:id`, async ({ request }) => {
    const body = await request.json();
    return HttpResponse.json(body);
  }),

  http.delete(`${BASE}/tenants/:tenant/chargebacks/:id`, () => {
    return HttpResponse.json({ ok: true });
  }),

  // Tags endpoints
  http.get(`${BASE}/tenants/:tenant/tags`, ({ request }) => {
    const url = new URL(request.url);
    const page = Number(url.searchParams.get("page") ?? "1");
    const pageSize = Number(url.searchParams.get("page_size") ?? "100");
    const tags: TagWithDimensionResponse[] = [
      {
        tag_id: 1,
        dimension_id: 10,
        tag_key: "env",
        tag_value: "uuid-1",
        display_name: "Production",
        created_by: "ui",
        created_at: null,
        identity_id: "user@example.com",
        product_type: "KAFKA_NUM_BYTES",
        resource_id: null,
      },
    ];
    const response: PaginatedResponse<TagWithDimensionResponse> = {
      items: tags,
      total: 1,
      page,
      page_size: pageSize,
      pages: 1,
    };
    return HttpResponse.json(response);
  }),

  http.patch(`${BASE}/tenants/:tenant/tags/:id`, async ({ request }) => {
    const body = (await request.json()) as { display_name: string };
    const tag: TagWithDimensionResponse = {
      tag_id: 1,
      dimension_id: 10,
      tag_key: "env",
      tag_value: "uuid-1",
      display_name: body.display_name,
      created_by: "ui",
      created_at: null,
      identity_id: "user@example.com",
      product_type: "KAFKA_NUM_BYTES",
      resource_id: null,
    };
    return HttpResponse.json(tag);
  }),

  http.delete(`${BASE}/tenants/:tenant/tags/:id`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  http.post(`${BASE}/tenants/:tenant/tags/bulk`, async () => {
    const result: BulkTagResponse = {
      created_count: 3,
      updated_count: 0,
      skipped_count: 0,
      errors: [],
    };
    return HttpResponse.json(result);
  }),

  http.post(`${BASE}/tenants/:tenant/tags/bulk-by-filter`, async () => {
    const result: BulkTagResponse = {
      created_count: 5,
      updated_count: 0,
      skipped_count: 0,
      errors: [],
    };
    return HttpResponse.json(result);
  }),

  // Export endpoint — returns CSV blob
  http.post(`${BASE}/tenants/:tenant/export`, () => {
    return new HttpResponse("date,amount\n2024-01-01,12.50\n", {
      headers: { "Content-Type": "text/csv" },
    });
  }),
];
