import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useGraphData } from "./useGraphData";

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

const GRAPH_RESPONSE = {
  nodes: [
    {
      id: "env-abc",
      resource_type: "environment",
      display_name: "my-env",
      cost: "100.00",
      created_at: "2026-01-01T00:00:00Z",
      deleted_at: null,
      tags: {},
      parent_id: null,
      cloud: null,
      region: null,
      status: "active",
      cross_references: [],
    },
  ],
  edges: [],
};

const BASE_PARAMS = {
  tenantName: "acme",
  focus: null,
};

describe("useGraphData", () => {
  it("fetches graph data with correct URL", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(GRAPH_RESPONSE);
      }),
    );

    const { result } = renderHook(() => useGraphData(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).toContain("/api/v1/tenants/acme/graph");
    expect(result.current.data).not.toBeNull();
    expect(result.current.data?.nodes).toHaveLength(1);
    expect(result.current.error).toBeNull();
  });

  it("includes focus param in URL when provided", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(GRAPH_RESPONSE);
      }),
    );

    const { result } = renderHook(
      () => useGraphData({ ...BASE_PARAMS, focus: "lkc-abc" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).toContain("focus=lkc-abc");
  });

  it("omits optional params when null", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(GRAPH_RESPONSE);
      }),
    );

    const { result } = renderHook(
      () => useGraphData({ ...BASE_PARAMS, at: null, focus: null }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).not.toContain("at=");
    expect(capturedUrl).not.toContain("focus=");
  });

  it("surfaces error string on API 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({ detail: "Internal error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useGraphData(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toBeNull();
  });

  it("surfaces error on 404 response", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({ detail: "Not Found" }, { status: 404 }),
      ),
    );

    const { result } = renderHook(
      () => useGraphData({ ...BASE_PARAMS, focus: "sa-ghost" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toBeNull();
  });

  it("is disabled when tenantName is null", () => {
    const { result } = renderHook(
      () => useGraphData({ ...BASE_PARAMS, tenantName: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("re-fetches when focus param changes", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/graph", () => {
        callCount++;
        return HttpResponse.json(GRAPH_RESPONSE);
      }),
    );

    const { result, rerender } = renderHook(
      ({ focus }: { focus: string | null }) =>
        useGraphData({ ...BASE_PARAMS, focus }),
      {
        wrapper: createWrapper(),
        initialProps: { focus: null } as { focus: string | null },
      },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const firstCount = callCount;

    rerender({ focus: "lkc-abc" });
    await waitFor(() => expect(callCount).toBeGreaterThan(firstCount));
  });

  it("includes depth, at, startDate, endDate, timezone in URL when provided", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(GRAPH_RESPONSE);
      }),
    );

    const { result } = renderHook(
      () =>
        useGraphData({
          ...BASE_PARAMS,
          depth: 2,
          at: "2026-03-15T00:00:00Z",
          startDate: "2026-03-01",
          endDate: "2026-04-01",
          timezone: "America/New_York",
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).toContain("depth=2");
    expect(capturedUrl).toContain("at=");
    expect(capturedUrl).toContain("start_date=2026-03-01");
    expect(capturedUrl).toContain("end_date=2026-04-01");
    expect(capturedUrl).toContain("timezone=");
  });
});
