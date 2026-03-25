import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useInventorySummary } from "./useInventorySummary";

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

const BASE_PARAMS = {
  tenantName: "acme",
};

const MOCK_SUMMARY = {
  resource_counts: {
    kafka_cluster: { total: 5, active: 4, deleted: 1 },
    connector: { total: 3, active: 3, deleted: 0 },
  },
  identity_counts: {
    service_account: { total: 12, active: 10, deleted: 2 },
    user: { total: 3, active: 3, deleted: 0 },
  },
};

describe("useInventorySummary", () => {
  it("starts in loading state", () => {
    const { result } = renderHook(() => useInventorySummary(BASE_PARAMS), { wrapper: createWrapper() });
    expect(result.current.isLoading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("returns inventory counts from successful fetch", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () =>
        HttpResponse.json(MOCK_SUMMARY),
      ),
    );

    const { result } = renderHook(() => useInventorySummary(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.resource_counts).toEqual({
      kafka_cluster: { total: 5, active: 4, deleted: 1 },
      connector: { total: 3, active: 3, deleted: 0 },
    });
    expect(result.current.data?.identity_counts).toEqual({
      service_account: { total: 12, active: 10, deleted: 2 },
      user: { total: 3, active: 3, deleted: 0 },
    });
    expect(result.current.error).toBeNull();
  });

  it("returns empty counts when API returns empty objects", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () =>
        HttpResponse.json({ resource_counts: {}, identity_counts: {} }),
      ),
    );

    const { result } = renderHook(() => useInventorySummary(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.resource_counts).toEqual({});
    expect(result.current.data?.identity_counts).toEqual({});
    expect(result.current.error).toBeNull();
  });

  it("sets error when server returns 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () =>
        HttpResponse.json({ detail: "Internal Server Error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useInventorySummary(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.error).toContain("HTTP 500");
    expect(result.current.data).toBeNull();
  });

  it("refetch triggers a new fetch", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () => {
        callCount++;
        return HttpResponse.json(MOCK_SUMMARY);
      }),
    );

    const { result } = renderHook(() => useInventorySummary(BASE_PARAMS), { wrapper: createWrapper() });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    const countAfterFirst = callCount;

    act(() => {
      result.current.refetch();
    });

    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });

  it("re-fetches when tenantName changes", async () => {
    let tenantName = "acme";

    server.use(
      http.get("/api/v1/tenants/:tenant/inventory/summary", () =>
        HttpResponse.json({ resource_counts: {}, identity_counts: {} }),
      ),
    );

    const { result, rerender } = renderHook(
      () => useInventorySummary({ tenantName }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    tenantName = "globex";
    rerender();
    await waitFor(() => expect(result.current.data?.resource_counts).toEqual({}));
  });
});
