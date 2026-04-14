import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useGraphTimeline } from "./useGraphTimeline";

function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
}

function createWrapper() {
  const queryClient = createTestQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(
      QueryClientProvider,
      { client: queryClient },
      children,
    );
  };
}

const BASE_PARAMS = {
  tenantName: "acme",
  entityId: "lkc-abc",
  startDate: "2026-01-01",
  endDate: "2026-04-01",
};

const TIMELINE_RESPONSE = [
  { date: "2026-01-01", cost: 100.0 },
  { date: "2026-01-02", cost: 105.5 },
  { date: "2026-01-03", cost: 98.25 },
];

describe("useGraphTimeline", () => {
  it("fires query when entityId and dates are set", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph/timeline", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(TIMELINE_RESPONSE);
      }),
    );

    const { result } = renderHook(() => useGraphTimeline(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("/api/v1/tenants/acme/graph/timeline");
    expect(capturedUrl).toContain("entity_id=lkc-abc");
    expect(capturedUrl).toContain("start=2026-01-01");
    expect(capturedUrl).toContain("end=2026-04-01");
    expect(result.current.data).not.toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("is disabled when entityId is null", () => {
    const { result } = renderHook(
      () => useGraphTimeline({ ...BASE_PARAMS, entityId: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when tenantName is null", () => {
    const { result } = renderHook(
      () => useGraphTimeline({ ...BASE_PARAMS, tenantName: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when startDate is null", () => {
    const { result } = renderHook(
      () => useGraphTimeline({ ...BASE_PARAMS, startDate: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when endDate is null", () => {
    const { result } = renderHook(
      () => useGraphTimeline({ ...BASE_PARAMS, endDate: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("returns flat array of { date, cost } — no .points wrapper", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/timeline", () =>
        HttpResponse.json(TIMELINE_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphTimeline(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(Array.isArray(result.current.data)).toBe(true);
    expect(result.current.data).toHaveLength(3);
    expect(result.current.data![0]).toHaveProperty("date");
    expect(result.current.data![0]).toHaveProperty("cost");
    expect(result.current.data![0].date).toBe("2026-01-01");
    expect(result.current.data![0].cost).toBe(100.0);
  });

  it("returns empty array when entity has no cost data", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/timeline", () =>
        HttpResponse.json([]),
      ),
    );

    const { result } = renderHook(() => useGraphTimeline(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).toEqual([]);
    expect(result.current.error).toBeNull();
  });

  it("surfaces error string on API 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/timeline", () =>
        HttpResponse.json({ detail: "Internal error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useGraphTimeline(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toBeNull();
  });

  it("re-fetches when entityId changes (query key includes entityId)", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/graph/timeline", () => {
        callCount++;
        return HttpResponse.json(TIMELINE_RESPONSE);
      }),
    );

    const { result, rerender } = renderHook(
      ({ entityId }: { entityId: string }) =>
        useGraphTimeline({ ...BASE_PARAMS, entityId }),
      {
        wrapper: createWrapper(),
        initialProps: { entityId: "lkc-abc" },
      },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const firstCount = callCount;

    rerender({ entityId: "lkc-xyz" });
    await waitFor(() => expect(callCount).toBeGreaterThan(firstCount));
  });
});
