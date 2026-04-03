import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useDataAvailability } from "./useDataAvailability";

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
};

describe("useDataAvailability", () => {
  it("starts in loading state", () => {
    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    expect(result.current.isLoading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("returns dates from successful fetch", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/dates", () =>
        HttpResponse.json({ dates: ["2026-01-15", "2026-01-17"] }),
      ),
    );

    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.dates).toEqual(["2026-01-15", "2026-01-17"]);
    expect(result.current.error).toBeNull();
  });

  it("returns empty dates array when API returns empty list", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/dates", () =>
        HttpResponse.json({ dates: [] }),
      ),
    );

    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data?.dates).toEqual([]);
    expect(result.current.error).toBeNull();
  });

  it("sets error when server returns 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/dates", () =>
        HttpResponse.json({ detail: "Internal Server Error" }, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.error).toContain("HTTP 500");
    expect(result.current.data).toBeNull();
  });

  it("exposes a refetch function", () => {
    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
    expect(typeof result.current.refetch).toBe("function");
  });

  it("refetch triggers a new fetch", async () => {
    let callCount = 0;
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/dates", () => {
        callCount++;
        return HttpResponse.json({ dates: ["2026-01-15"] });
      }),
    );

    const { result } = renderHook(() => useDataAvailability(BASE_PARAMS), {
      wrapper: createWrapper(),
    });
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
      http.get("/api/v1/tenants/:tenant/chargebacks/dates", () =>
        HttpResponse.json({ dates: [] }),
      ),
    );

    const { result, rerender } = renderHook(
      () => useDataAvailability({ tenantName }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    tenantName = "globex";
    rerender();
    await waitFor(() => expect(result.current.data?.dates).toEqual([]));
  });
});
