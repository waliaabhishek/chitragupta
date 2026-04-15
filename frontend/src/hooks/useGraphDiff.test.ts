import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useGraphDiff } from "./useGraphDiff";

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
  fromStart: "2026-01-01",
  fromEnd: "2026-01-31",
  toStart: "2026-02-01",
  toEnd: "2026-02-28",
  focus: null,
};

const DIFF_RESPONSE = [
  {
    id: "env-abc",
    resource_type: "environment",
    display_name: "my-env",
    parent_id: null,
    cost_before: "100.00",
    cost_after: "150.00",
    cost_delta: "50.00",
    pct_change: "50.00",
    status: "changed",
  },
  {
    id: "lkc-new",
    resource_type: "kafka_cluster",
    display_name: "new-cluster",
    parent_id: "env-abc",
    cost_before: "0.00",
    cost_after: "75.00",
    cost_delta: "75.00",
    pct_change: null,
    status: "new",
  },
];

describe("useGraphDiff", () => {
  it("fires query when all 4 dates and tenantName are set", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(DIFF_RESPONSE);
      }),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("/api/v1/tenants/acme/graph/diff");
    expect(capturedUrl).toContain("from_start=2026-01-01");
    expect(capturedUrl).toContain("from_end=2026-01-31");
    expect(capturedUrl).toContain("to_start=2026-02-01");
    expect(capturedUrl).toContain("to_end=2026-02-28");
    expect(result.current.data).not.toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("is disabled (no fetch) when tenantName is null", () => {
    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, tenantName: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when fromStart is null", () => {
    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, fromStart: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when fromEnd is null", () => {
    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, fromEnd: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when toStart is null", () => {
    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, toStart: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("is disabled when toEnd is null", () => {
    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, toEnd: null }),
      { wrapper: createWrapper() },
    );

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });

  it("parses Decimal string cost_before to number", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json(DIFF_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(typeof result.current.data![0].cost_before).toBe("number");
    expect(result.current.data![0].cost_before).toBe(100);
  });

  it("parses Decimal string cost_after to number", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json(DIFF_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(typeof result.current.data![0].cost_after).toBe("number");
    expect(result.current.data![0].cost_after).toBe(150);
  });

  it("parses Decimal string cost_delta to number", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json(DIFF_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(typeof result.current.data![0].cost_delta).toBe("number");
    expect(result.current.data![0].cost_delta).toBe(50);
  });

  it("parses Decimal string pct_change to number", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json(DIFF_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(typeof result.current.data![0].pct_change).toBe("number");
    expect(result.current.data![0].pct_change).toBe(50);
  });

  it("preserves null pct_change for new nodes", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json(DIFF_RESPONSE),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data![1].pct_change).toBeNull();
  });

  it("includes focus param in URL when provided", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(DIFF_RESPONSE);
      }),
    );

    const { result } = renderHook(
      () => useGraphDiff({ ...BASE_PARAMS, focus: "lkc-abc" }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("focus=lkc-abc");
  });

  it("uses default depth=1 when depth not specified", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json(DIFF_RESPONSE);
      }),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(capturedUrl).toContain("depth=1");
  });

  it("surfaces error string on API 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () =>
        HttpResponse.json({ detail: "Internal error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useGraphDiff(BASE_PARAMS), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toBeNull();
  });

  it("uses query key including all params", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/graph/diff", () => {
        callCount++;
        return HttpResponse.json(DIFF_RESPONSE);
      }),
    );

    const { result, rerender } = renderHook(
      ({ toEnd }: { toEnd: string }) => useGraphDiff({ ...BASE_PARAMS, toEnd }),
      {
        wrapper: createWrapper(),
        initialProps: { toEnd: "2026-02-28" },
      },
    );

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const firstCount = callCount;

    rerender({ toEnd: "2026-03-31" });
    await waitFor(() => expect(callCount).toBeGreaterThan(firstCount));
  });
});
