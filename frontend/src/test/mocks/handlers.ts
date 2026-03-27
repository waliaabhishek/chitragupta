import { http, HttpResponse } from "msw";
import type {
  AggregationResponse,
  EntityTagCreateRequest,
  EntityTagResponse,
  InventorySummaryResponse,
  PaginatedResponse,
  PipelineRunResponse,
  PipelineStatusResponse,
  ReadinessResponse,
  TenantListResponse,
  TenantStatusDetailResponse,
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
      tags: {},
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
      dimension_id: Number(params.id),
      ecosystem: "ccloud",
      tenant_id: "t-001",
      resource_id: "r-001",
      product_category: "KAFKA",
      product_type: "KAFKA_NUM_BYTES",
      identity_id: "user@example.com",
      cost_type: "USAGE",
      allocation_method: "ratio",
      allocation_detail: null,
      tags: {},
    });
  }),

  http.post(`${BASE}/tenants/:tenant/chargebacks`, async ({ request }) => {
    const body = await request.json();
    return HttpResponse.json(body, { status: 201 });
  }),

  http.delete(`${BASE}/tenants/:tenant/chargebacks/:id`, () => {
    return HttpResponse.json({ ok: true });
  }),

  // Tags endpoints
  http.get(`${BASE}/tenants/:tenant/tags`, ({ request }) => {
    const url = new URL(request.url);
    const page = Number(url.searchParams.get("page") ?? "1");
    const pageSize = Number(url.searchParams.get("page_size") ?? "100");
    const tags: EntityTagResponse[] = [
      {
        tag_id: 1,
        tenant_id: "t-001",
        entity_type: "resource",
        entity_id: "r-001",
        tag_key: "env",
        tag_value: "production",
        created_by: "ui",
        created_at: null,
      },
    ];
    const response: PaginatedResponse<EntityTagResponse> = {
      items: tags,
      total: 1,
      page,
      page_size: pageSize,
      pages: 1,
    };
    return HttpResponse.json(response);
  }),

  // Entity tag CRUD handlers
  http.get(`${BASE}/tenants/:tenant/entities/:entityType/:entityId/tags`, ({ params }) => {
    const tags: EntityTagResponse[] = [
      {
        tag_id: 1,
        tenant_id: "t-001",
        entity_type: params.entityType as string,
        entity_id: params.entityId as string,
        tag_key: "env",
        tag_value: "production",
        created_by: "ui",
        created_at: null,
      },
    ];
    return HttpResponse.json(tags);
  }),

  http.post(`${BASE}/tenants/:tenant/entities/:entityType/:entityId/tags`, async ({ request, params }) => {
    const body = (await request.json()) as EntityTagCreateRequest;
    const tag: EntityTagResponse = {
      tag_id: Date.now(),
      tenant_id: "t-001",
      entity_type: params.entityType as string,
      entity_id: params.entityId as string,
      tag_key: body.tag_key,
      tag_value: body.tag_value,
      created_by: body.created_by,
      created_at: null,
    };
    return HttpResponse.json(tag, { status: 201 });
  }),

  http.put(`${BASE}/tenants/:tenant/entities/:entityType/:entityId/tags/:tagKey`, async ({ request, params }) => {
    const body = await request.json() as { tag_value: string };
    const tag: EntityTagResponse = {
      tag_id: 1,
      tenant_id: "t-001",
      entity_type: params.entityType as string,
      entity_id: params.entityId as string,
      tag_key: params.tagKey as string,
      tag_value: body.tag_value,
      created_by: "ui",
      created_at: null,
    };
    return HttpResponse.json(tag);
  }),

  http.delete(`${BASE}/tenants/:tenant/entities/:entityType/:entityId/tags/:tagKey`, () => {
    return new HttpResponse(null, { status: 204 });
  }),

  // Export endpoint — returns CSV blob
  http.post(`${BASE}/tenants/:tenant/export`, () => {
    return new HttpResponse("date,amount\n2024-01-01,12.50\n", {
      headers: { "Content-Type": "text/csv" },
    });
  }),

  // Pipeline run
  http.post(`${BASE}/tenants/:tenant/pipeline/run`, () => {
    return HttpResponse.json<PipelineRunResponse>({
      tenant_name: "acme",
      status: "started",
      message: "Pipeline started successfully",
    });
  }),

  // Pipeline status
  http.get(`${BASE}/tenants/:tenant/pipeline/status`, () => {
    return HttpResponse.json<PipelineStatusResponse>({
      tenant_name: "acme",
      is_running: false,
      last_run: "2026-03-26T10:00:00Z",
      last_result: {
        dates_gathered: 5,
        dates_calculated: 5,
        chargeback_rows_written: 120,
        errors: [],
        completed_at: "2026-03-26T10:05:00Z",
      },
    });
  }),

  // Tenant status (per-date)
  http.get(`${BASE}/tenants/:tenant/status`, ({ params }) => {
    return HttpResponse.json<TenantStatusDetailResponse>({
      tenant_name: params.tenant as string,
      tenant_id: "t-001",
      ecosystem: "ccloud",
      states: [
        {
          tracking_date: "2026-03-26",
          billing_gathered: true,
          resources_gathered: true,
          chargeback_calculated: true,
        },
        {
          tracking_date: "2026-03-25",
          billing_gathered: true,
          resources_gathered: true,
          chargeback_calculated: false,
        },
      ],
    });
  }),
];
