import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useAggregation } from "./useAggregation";

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
  groupBy: ["identity_id"],
  timeBucket: "day" as const,
  startDate: "2026-02-01",
  endDate: "2026-02-28",
};

describe("useAggregation", () => {
  it("starts in loading state", () => {
    const { result } = renderHook(() => useAggregation(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    expect(result.current.isLoading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("returns data after successful fetch", async () => {
    const { result } = renderHook(() => useAggregation(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.data).not.toBeNull();
    expect(result.current.data?.buckets).toHaveLength(2);
    expect(result.current.error).toBeNull();
  });

  it("returns error when server responds with 500", async () => {
    server.use(
      http.get("/api/v1/tenants/error-tenant/chargebacks/aggregate", () => {
        return HttpResponse.json(
          { detail: "Internal Server Error" },
          { status: 500 },
        );
      }),
    );

    const { result } = renderHook(
      () => useAggregation({ ...BASE_PARAMS, tenantName: "error-tenant" }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.error).toContain("HTTP 500");
    expect(result.current.data).toBeNull();
  });

  it("appends filter params to request URL", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          total_rows: 0,
        });
      }),
    );

    const { result } = renderHook(
      () =>
        useAggregation({
          ...BASE_PARAMS,
          identityId: "user-1",
          productType: "kafka",
          resourceId: "r-001",
          costType: "usage",
        }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("identity_id=user-1");
    expect(capturedUrl).toContain("product_type=kafka");
    expect(capturedUrl).toContain("resource_id=r-001");
    expect(capturedUrl).toContain("cost_type=usage");
  });

  it("re-fetches when params change", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () => {
        callCount++;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          total_rows: 0,
        });
      }),
    );

    let groupBy = ["identity_id"];
    const { result, rerender } = renderHook(
      () => useAggregation({ ...BASE_PARAMS, groupBy }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const countAfterFirst = callCount;

    groupBy = ["product_type"];
    rerender();
    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });

  it("returns empty data when API returns empty buckets", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () =>
        HttpResponse.json({ buckets: [], total_amount: "0", total_rows: 0 }),
      ),
    );

    const { result } = renderHook(() => useAggregation(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.buckets).toEqual([]);
    expect(result.current.data?.total_rows).toBe(0);
    expect(result.current.error).toBeNull();
  });

  it("refetch triggers a new fetch", async () => {
    const { result } = renderHook(() => useAggregation(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    act(() => {
      result.current.refetch();
    });

    await waitFor(() => expect(result.current.data).not.toBeNull());
  });

  it("appends timezone to request URL when provided", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          total_rows: 0,
        });
      }),
    );

    const { result } = renderHook(
      () => useAggregation({ ...BASE_PARAMS, timezone: "America/Chicago" }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("timezone=America%2FChicago");
  });

  it("re-fetches when timezone param changes", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () => {
        callCount++;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          total_rows: 0,
        });
      }),
    );

    let timezone: string | null = "America/Denver";
    const { result, rerender } = renderHook(
      () => useAggregation({ ...BASE_PARAMS, timezone }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const countAfterFirst = callCount;

    timezone = "America/Chicago";
    rerender();
    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });
});
