import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useGraphSearch } from "./useGraphSearch";

function createTestQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
}

function createWrapper(): ({ children }: { children: ReactNode }) => ReactNode {
  const queryClient = createTestQueryClient();
  return function Wrapper({ children }: { children: ReactNode }): ReactNode {
    return createElement(QueryClientProvider, { client: queryClient }, children);
  };
}

const SEARCH_RESPONSE = {
  results: [
    {
      id: "lkc-abc",
      resource_type: "kafka_cluster",
      display_name: "Kafka Prod",
      parent_id: "env-abc",
      parent_display_name: "ACME Env",
      status: "active",
    },
  ],
};

describe("useGraphSearch", () => {
  it("fires API call and returns results when query length >= 1", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/search", () =>
        HttpResponse.json(SEARCH_RESPONSE),
      ),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: "acme", query: "kafka" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.results).toHaveLength(1);
    expect(result.current.results[0].id).toBe("lkc-abc");
  });

  it("returns parsed results with parent_display_name field", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/search", () =>
        HttpResponse.json(SEARCH_RESPONSE),
      ),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: "acme", query: "kafka" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.results[0].parent_display_name).toBe("ACME Env");
  });

  it("is disabled when tenantName is null — makes no API call", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/:tenant/graph/search", () => {
        callCount++;
        return HttpResponse.json({ results: [] });
      }),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: null, query: "kafka" }),
      { wrapper: createWrapper() },
    );

    await new Promise((r) => setTimeout(r, 50));
    expect(callCount).toBe(0);
    expect(result.current.results).toHaveLength(0);
    expect(result.current.isLoading).toBe(false);
  });

  it("is disabled when query is empty string — makes no API call", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/graph/search", () => {
        callCount++;
        return HttpResponse.json({ results: [] });
      }),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: "acme", query: "" }),
      { wrapper: createWrapper() },
    );

    await new Promise((r) => setTimeout(r, 50));
    expect(callCount).toBe(0);
    expect(result.current.results).toHaveLength(0);
  });

  it("passes query param to API", async () => {
    let capturedQuery = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph/search", ({ request }) => {
        const url = new URL(request.url);
        capturedQuery = url.searchParams.get("q") ?? "";
        return HttpResponse.json({ results: [] });
      }),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: "acme", query: "lkc-abc" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedQuery).toBe("lkc-abc");
  });

  it("returns error when API fails", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/search", () =>
        new HttpResponse(null, { status: 500 }),
      ),
    );

    const { result } = renderHook(
      () => useGraphSearch({ tenantName: "acme", query: "kafka" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.error).not.toBeNull();
  });
});
