import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TenantProvider, useTenant } from "./TenantContext";

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return <TenantProvider>{children}</TenantProvider>;
}

afterEach(() => {
  localStorage.clear();
});

describe("TenantContext", () => {
  it("loads tenants on mount", async () => {
    const { result } = renderHook(() => useTenant(), { wrapper });
    expect(result.current.isLoading).toBe(true);

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.tenants).toHaveLength(2);
    expect(result.current.tenants[0].tenant_name).toBe("acme");
    expect(result.current.tenants[1].tenant_name).toBe("globex");
  });

  it("selects first tenant by default", async () => {
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.currentTenant?.tenant_name).toBe("acme");
  });

  it("restores tenant from localStorage", async () => {
    localStorage.setItem("chargeback_selected_tenant", "globex");
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.currentTenant?.tenant_name).toBe("globex");
  });

  it("falls back to first tenant if saved tenant not found", async () => {
    localStorage.setItem("chargeback_selected_tenant", "nonexistent");
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.currentTenant?.tenant_name).toBe("acme");
  });

  it("setCurrentTenant updates state and localStorage", async () => {
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    act(() => {
      result.current.setCurrentTenant(result.current.tenants[1]);
    });

    expect(result.current.currentTenant?.tenant_name).toBe("globex");
    expect(localStorage.getItem("chargeback_selected_tenant")).toBe("globex");
  });

  it("setCurrentTenant(null) clears localStorage", async () => {
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    act(() => {
      result.current.setCurrentTenant(null);
    });

    expect(result.current.currentTenant).toBeNull();
    expect(localStorage.getItem("chargeback_selected_tenant")).toBeNull();
  });

  it("useTenant throws outside provider", () => {
    // Suppress React's verbose error boundary output for this expected throw.
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      expect(() => renderHook(() => useTenant())).toThrow(
        "useTenant must be used within TenantProvider",
      );
    } finally {
      consoleError.mockRestore();
    }
  });

  it("exposes error state when fetch fails", async () => {
    // Override handler to return 500
    const { server } = await import("../test/mocks/server");
    const { http, HttpResponse } = await import("msw");

    server.use(
      http.get("/api/v1/tenants", () => {
        return new HttpResponse(null, { status: 500, statusText: "Internal Server Error" });
      }),
    );

    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).toMatch(/500|Internal Server Error/);
    expect(result.current.tenants).toHaveLength(0);
  });
});
