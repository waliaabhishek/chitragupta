import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useTagValues } from "./useTagValues";

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

describe("useTagValues", () => {
  it("returns tag values when API returns values array", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys/owner/values", () =>
        HttpResponse.json({ values: ["alice", "bob"] }),
      ),
    );

    const { result } = renderHook(() => useTagValues("acme", "owner"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).toEqual(["alice", "bob"]);
    expect(result.current.error).toBeNull();
  });

  it("returns empty array when API returns empty values", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys/owner/values", () =>
        HttpResponse.json({ values: [] }),
      ),
    );

    const { result } = renderHook(() => useTagValues("acme", "owner"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.data).toEqual([]);
    expect(result.current.error).toBeNull();
  });

  it("returns error string when API returns 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys/owner/values", () =>
        new HttpResponse(null, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useTagValues("acme", "owner"), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toEqual([]);
  });

  it("is disabled when tagKey is empty string", () => {
    const { result } = renderHook(() => useTagValues("acme", ""), {
      wrapper: createWrapper(),
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toEqual([]);
  });
});
