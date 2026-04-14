import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "../test/mocks/server";
import { useDateRange } from "./useDateRange";

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

// Today is 2026-04-13 per project context
const TODAY = "2026-04-13";

function makeApiNode(overrides: Record<string, unknown> = {}) {
  return {
    id: "env-abc",
    resource_type: "environment",
    display_name: "my-env",
    cost: "100.00",
    created_at: null,
    deleted_at: null,
    tags: {},
    parent_id: null,
    cloud: null,
    region: null,
    status: "active",
    cross_references: [],
    ...overrides,
  };
}

describe("useDateRange", () => {
  beforeEach(() => {
    // Fix today's date for deterministic maxDate
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-13T00:00:00Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("is disabled when tenantName is null", () => {
    const { result } = renderHook(() => useDateRange({ tenantName: null }), {
      wrapper: createWrapper(),
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.minDate).toBeNull();
    expect(result.current.maxDate).toBeNull();
  });

  it("extracts min(created_at) across all nodes as minDate", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        const url = new URL(request.url);
        // useDateRange calls root topology (no focus param)
        if (!url.searchParams.has("focus")) {
          return HttpResponse.json({
            nodes: [
              makeApiNode({ id: "env-1", created_at: "2026-02-01T00:00:00Z" }),
              makeApiNode({ id: "env-2", created_at: "2026-01-15T00:00:00Z" }),
              makeApiNode({ id: "env-3", created_at: "2026-03-01T00:00:00Z" }),
            ],
            edges: [],
          });
        }
        return HttpResponse.json({ nodes: [], edges: [] });
      }),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBe("2026-01-15");
  });

  it("skips nodes with null created_at when computing minDate", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({ id: "env-1", created_at: null }),
            makeApiNode({ id: "env-2", created_at: "2026-02-10T00:00:00Z" }),
            makeApiNode({ id: "env-3", created_at: null }),
          ],
          edges: [],
        }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBe("2026-02-10");
  });

  it("returns minDate=null when no nodes have created_at", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({ id: "env-1", created_at: null }),
            makeApiNode({ id: "env-2", created_at: null }),
          ],
          edges: [],
        }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBeNull();
    expect(result.current.maxDate).toBeNull();
  });

  it("uses today as maxDate when no deleted_at exceeds today", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({
              id: "env-1",
              created_at: "2026-01-01T00:00:00Z",
              deleted_at: null,
            }),
            makeApiNode({
              id: "env-2",
              created_at: "2026-02-01T00:00:00Z",
              deleted_at: "2026-03-01T00:00:00Z",
            }),
          ],
          edges: [],
        }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.maxDate).toBe(TODAY);
  });

  it("uses max(deleted_at) as maxDate when it exceeds today", async () => {
    const futureDate = "2026-12-31";
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({
              id: "env-1",
              created_at: "2026-01-01T00:00:00Z",
              deleted_at: `${futureDate}T00:00:00Z`,
            }),
          ],
          edges: [],
        }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.maxDate).toBe(futureDate);
  });

  it("calls root topology endpoint (no focus param)", async () => {
    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          nodes: [makeApiNode({ created_at: "2026-01-01T00:00:00Z" })],
          edges: [],
        });
      }),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    const url = new URL(capturedUrl);
    expect(url.searchParams.has("focus")).toBe(false);
  });

  it("returns minDate and maxDate as ISO date strings (YYYY-MM-DD)", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({
              id: "env-1",
              created_at: "2026-01-15T12:30:00Z",
              deleted_at: null,
            }),
          ],
          edges: [],
        }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    // Should return date-only strings, not ISO datetimes
    expect(result.current.minDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(result.current.maxDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});
