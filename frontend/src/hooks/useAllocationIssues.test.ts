import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useAllocationIssues } from "./useAllocationIssues";
import type { ChargebackFilters } from "../types/filters";

function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
}

function createWrapper() {
  const queryClient = createTestQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client: queryClient }, children);
  };
}

const EMPTY_FILTERS: ChargebackFilters = {
  start_date: null,
  end_date: null,
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
  timezone: null,
  tag_key: null,
  tag_value: null,
};

const BASE_PARAMS = {
  tenantName: "acme",
  filters: EMPTY_FILTERS,
  page: 1,
  pageSize: 100,
};

const MOCK_RESPONSE = {
  items: [
    {
      ecosystem: "ccloud",
      resource_id: "lkc-abc123",
      product_type: "kafka",
      identity_id: "sa-001",
      allocation_detail: "no_identities_located",
      row_count: 3,
      usage_cost: "120.00",
      shared_cost: "0.00",
      total_cost: "120.00",
    },
  ],
  total: 1,
  page: 1,
  page_size: 100,
  pages: 1,
};

describe("useAllocationIssues", () => {
  it("starts in loading state", () => {
    const { result } = renderHook(() => useAllocationIssues(BASE_PARAMS), { wrapper: createWrapper() });
    expect(result.current.isLoading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("returns paginated data from successful fetch", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", () =>
        HttpResponse.json(MOCK_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useAllocationIssues(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).not.toBeNull();
    expect(result.current.data?.total).toBe(1);
    expect(result.current.data?.items).toHaveLength(1);
    expect(result.current.data?.items[0].identity_id).toBe("sa-001");
    expect(result.current.data?.items[0].allocation_detail).toBe("no_identities_located");
    expect(result.current.error).toBeNull();
  });

  it("returns empty items array when no allocation issues exist", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", () =>
        HttpResponse.json({ items: [], total: 0, page: 1, page_size: 100, pages: 0 }),
      ),
    );

    const { result } = renderHook(() => useAllocationIssues(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.items).toEqual([]);
    expect(result.current.data?.total).toBe(0);
    expect(result.current.error).toBeNull();
  });

  it("sets error when server returns 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", () =>
        HttpResponse.json({ detail: "Internal Server Error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useAllocationIssues(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.error).toContain("HTTP 500");
    expect(result.current.data).toBeNull();
  });

  it("refetch triggers a new fetch", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", () => {
        callCount++;
        return HttpResponse.json(MOCK_RESPONSE);
      }),
    );

    const { result } = renderHook(() => useAllocationIssues(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    const countAfterFirst = callCount;

    act(() => {
      result.current.refetch();
    });

    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });

  it("includes filter params in the request URL", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(MOCK_RESPONSE);
      }),
    );

    // Stable reference outside renderHook callback to avoid infinite effect re-runs
    const stableFilters: ChargebackFilters = {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      identity_id: "sa-001",
      product_type: "kafka",
      resource_id: "lkc-abc123",
      cost_type: null,
      timezone: null,
      tag_key: null,
      tag_value: null,
    };

    const { result } = renderHook(
      () =>
        useAllocationIssues({
          tenantName: "acme",
          filters: stableFilters,
          page: 1,
          pageSize: 100,
        }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("start_date=2026-01-01");
    expect(capturedUrl).toContain("end_date=2026-01-31");
    expect(capturedUrl).toContain("identity_id=sa-001");
    expect(capturedUrl).toContain("product_type=kafka");
    expect(capturedUrl).toContain("resource_id=lkc-abc123");
  });

  it("re-fetches when tenantName changes", async () => {
    let tenantName = "acme";

    server.use(
      http.get("/api/v1/tenants/:tenant/chargebacks/allocation-issues", () =>
        HttpResponse.json({ items: [], total: 0, page: 1, page_size: 100, pages: 0 }),
      ),
    );

    const { result, rerender } = renderHook(
      () => useAllocationIssues({ tenantName, filters: EMPTY_FILTERS, page: 1, pageSize: 100 }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    tenantName = "globex";
    rerender();
    await waitFor(() => expect(result.current.data?.total).toBe(0));
  });

  it("includes timezone in request URL when filters have timezone set", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/allocation-issues", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(MOCK_RESPONSE);
      }),
    );

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const filtersWithTimezone: ChargebackFilters = { ...EMPTY_FILTERS, timezone: "America/Chicago" } as any;

    const { result } = renderHook(
      () => useAllocationIssues({ tenantName: "acme", filters: filtersWithTimezone, page: 1, pageSize: 100 }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("timezone=America%2FChicago");
  });
});
