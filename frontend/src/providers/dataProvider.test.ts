import { describe, expect, it, vi } from "vitest";
import { dataProvider } from "./dataProvider";

// dataProvider uses window.location.origin for URL construction.
// In jsdom the origin is "http://localhost".

describe("dataProvider.getApiUrl", () => {
  it("returns the API base URL", () => {
    expect(dataProvider.getApiUrl()).toBe("/api/v1");
  });
});

describe("dataProvider.getList", () => {
  it("fetches tenant-scoped resource with pagination", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const result = await dataProvider.getList({
      resource: "chargebacks",
      pagination: { currentPage: 2, pageSize: 50 },
      meta: { tenantName: "acme" },
    });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain("page=2");
    expect(calledUrl).toContain("page_size=50");
    expect(result.data).toHaveLength(1);
    expect(result.total).toBe(1);
    fetchSpy.mockRestore();
  });

  it("defaults to page 1, pageSize 100 when pagination omitted", async () => {
    const result = await dataProvider.getList({
      resource: "chargebacks",
      meta: { tenantName: "acme" },
    });
    expect(result.data).toHaveLength(1);
  });

  it("maps filters to query params — verifies filter appears in request URL", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    await dataProvider.getList({
      resource: "billing",
      filters: [{ field: "product_category", operator: "eq", value: "KAFKA" }],
      meta: { tenantName: "acme" },
    });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain("product_category=KAFKA");
    fetchSpy.mockRestore();
  });

  it("fetches non-tenant resource without tenant segment in URL", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    // /api/v1/tenants returns a non-paginated shape; the call succeeds at
    // the network level even though items/total are undefined — we care about
    // the URL, not the return value here.
    await dataProvider.getList({ resource: "tenants" });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain("/api/v1/tenants");
    expect(calledUrl).not.toContain("undefined");
    fetchSpy.mockRestore();
  });
});

describe("dataProvider.getOne", () => {
  it("GETs tenant-scoped single resource by id", async () => {
    const result = await dataProvider.getOne({
      resource: "chargebacks",
      id: "cb-042",
      meta: { tenantName: "acme" },
    });
    expect(result.data).toMatchObject({ ecosystem: "ccloud" });
  });
});

describe("dataProvider.create", () => {
  it("POSTs to tenant-scoped URL and returns created resource", async () => {
    const variables = { product_category: "KAFKA", amount: "10.00" };
    const result = await dataProvider.create({
      resource: "chargebacks",
      variables,
      meta: { tenantName: "acme" },
    });
    expect(result.data).toMatchObject(variables);
  });
});

describe("dataProvider.update", () => {
  it("PATCHes tenant-scoped URL and returns updated resource", async () => {
    const { server } = await import("../test/mocks/server");
    const { http, HttpResponse } = await import("msw");
    server.use(
      http.patch(
        "/api/v1/tenants/:tenant/chargebacks/:id",
        async ({ request }) => {
          const body = await request.json();
          return HttpResponse.json(body);
        },
      ),
    );
    const variables = { amount: "20.00" };
    const result = await dataProvider.update({
      resource: "chargebacks",
      id: "cb-001",
      variables,
      meta: { tenantName: "acme" },
    });
    expect(result.data).toMatchObject(variables);
  });
});

describe("dataProvider.deleteOne", () => {
  it("DELETEs from tenant-scoped URL", async () => {
    const result = await dataProvider.deleteOne({
      resource: "chargebacks",
      id: "cb-001",
      meta: { tenantName: "acme" },
    });
    expect(result.data).toMatchObject({ ok: true });
  });
});

describe("fetchJson error handling", () => {
  it("throws with HTTP status on non-OK response", async () => {
    const { server } = await import("../test/mocks/server");
    const { http, HttpResponse } = await import("msw");

    server.use(
      http.get(
        "/api/v1/tenants/acme/chargebacks",
        () => new HttpResponse(null, { status: 404, statusText: "Not Found" }),
      ),
    );

    await expect(
      dataProvider.getList({
        resource: "chargebacks",
        meta: { tenantName: "acme" },
      }),
    ).rejects.toThrow("HTTP 404: Not Found");
  });
});

describe("buildUrl logic (via getList)", () => {
  it("includes tenantName in path", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    await dataProvider.getList({
      resource: "chargebacks",
      meta: { tenantName: "acme" },
    });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain("/tenants/acme/chargebacks");
    fetchSpy.mockRestore();
  });

  it("omits tenant segment when tenantName absent — URL goes to resource directly", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    await dataProvider.getList({ resource: "tenants" });
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    // URL must go directly to the resource with no undefined-tenant segment
    expect(calledUrl).toContain("/api/v1/tenants");
    expect(calledUrl).not.toContain("undefined");
    fetchSpy.mockRestore();
  });
});
