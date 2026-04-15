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

/** Set up both MSW handlers needed by useDateRange. */
function setupHandlers(
  nodes: Record<string, unknown>[],
  chargebackDates: string[],
) {
  server.use(
    http.get("/api/v1/tenants/acme/graph", () =>
      HttpResponse.json({ nodes, edges: [] }),
    ),
    http.get("/api/v1/tenants/acme/chargebacks/dates", () =>
      HttpResponse.json({ dates: chargebackDates }),
    ),
  );
}

describe("useDateRange", () => {
  beforeEach(() => {
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
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: "2026-02-01T00:00:00Z" }),
        makeApiNode({ id: "env-2", created_at: "2026-01-15T00:00:00Z" }),
        makeApiNode({ id: "env-3", created_at: "2026-03-01T00:00:00Z" }),
      ],
      ["2026-01-15", "2026-02-01", "2026-04-10"],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBe("2026-01-15");
  });

  it("skips nodes with null created_at when computing minDate", async () => {
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: null }),
        makeApiNode({ id: "env-2", created_at: "2026-02-10T00:00:00Z" }),
        makeApiNode({ id: "env-3", created_at: null }),
      ],
      ["2026-02-10", "2026-03-15"],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBe("2026-02-10");
  });

  it("returns minDate=null when no nodes have created_at", async () => {
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: null }),
        makeApiNode({ id: "env-2", created_at: null }),
      ],
      ["2026-03-01"],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toBeNull();
    expect(result.current.maxDate).toBeNull();
  });

  it("uses last chargeback date as maxDate", async () => {
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: "2026-01-01T00:00:00Z" }),
      ],
      ["2026-01-01", "2026-02-15", "2026-04-10"],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.maxDate).toBe("2026-04-10");
  });

  it("falls back to minDate when chargeback dates are empty", async () => {
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: "2026-01-01T00:00:00Z" }),
      ],
      [],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.maxDate).toBe("2026-01-01");
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
      http.get("/api/v1/tenants/acme/chargebacks/dates", () =>
        HttpResponse.json({ dates: ["2026-01-01"] }),
      ),
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    const url = new URL(capturedUrl);
    expect(url.searchParams.has("focus")).toBe(false);
  });

  it("returns minDate and maxDate as ISO date strings (YYYY-MM-DD)", async () => {
    setupHandlers(
      [
        makeApiNode({ id: "env-1", created_at: "2026-01-15T12:30:00Z" }),
      ],
      ["2026-01-15", "2026-03-20"],
    );

    const { result } = renderHook(() => useDateRange({ tenantName: "acme" }), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.minDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(result.current.maxDate).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});
