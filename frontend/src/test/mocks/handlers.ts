import { http, HttpResponse } from "msw";
import type {
  AggregationResponse,
  TenantListResponse,
  PaginatedResponse,
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

export const handlers = [
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

  // Must be before /:id to prevent "aggregate" being captured as an ID param
  http.get(`${BASE}/tenants/:tenant/chargebacks/aggregate`, () => {
    const response: AggregationResponse = {
      buckets: [
        {
          dimensions: { identity_id: "user-1" },
          time_bucket: "2026-02-15",
          total_amount: "10.00",
          row_count: 1,
        },
        {
          dimensions: { identity_id: "user-2" },
          time_bucket: "2026-02-15",
          total_amount: "5.00",
          row_count: 1,
        },
      ],
      total_amount: "15.00",
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
];
