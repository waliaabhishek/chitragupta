import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useTagKeys } from "./useTagKeys";

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

describe("useTagKeys", () => {
  it("returns tag keys when API returns keys array", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["owner", "team"] }),
      ),
    );

    const { result } = renderHook(() => useTagKeys("acme"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).toEqual(["owner", "team"]);
    expect(result.current.error).toBeNull();
  });

  it("returns error string when API returns 500", async () => {
    server.use(
      http.get(
        "/api/v1/tenants/acme/tags/keys",
        () => new HttpResponse(null, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useTagKeys("acme"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toEqual([]);
  });

  it("returns empty array when API returns empty keys", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: [] }),
      ),
    );

    const { result } = renderHook(() => useTagKeys("acme"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).toEqual([]);
    expect(result.current.error).toBeNull();
  });
});
