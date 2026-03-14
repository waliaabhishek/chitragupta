import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { TenantProvider, useTenant } from "./TenantContext";
import type { ReadinessResponse } from "../types/api";

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

// ---------------------------------------------------------------------------
// Tests 12 & 13: Adaptive polling interval based on pipeline_running state
// ---------------------------------------------------------------------------

describe("TenantContext — adaptive polling interval", () => {
  function makeReadiness(pipelineRunning: boolean): ReadinessResponse {
    return {
      status: "ready",
      version: "1.0.0",
      mode: "both",
      tenants: [
        {
          tenant_name: "acme",
          tables_ready: true,
          has_data: true,
          pipeline_running: pipelineRunning,
          pipeline_stage: pipelineRunning ? "gathering" : null,
          pipeline_current_date: null,
          last_run_status: pipelineRunning ? "running" : "completed",
          last_run_at: null,
          permanent_failure: null,
        },
      ],
    };
  }

  it("polls at 5000ms when any tenant pipeline_running=true", async () => {
    const { server } = await import("../test/mocks/server");

    server.use(
      http.get("/api/v1/readiness", () => {
        return HttpResponse.json(makeReadiness(true));
      }),
    );

    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");

    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    // Extract all delay arguments passed to setTimeout
    const delays = setTimeoutSpy.mock.calls
      .map(([, delay]) => delay as number | undefined)
      .filter((d): d is number => typeof d === "number");

    // After receiving a ready+running response, must schedule next poll at 5000
    expect(delays).toContain(5000);
    // Must NOT have scheduled at 15000 for a running pipeline
    expect(delays).not.toContain(15000);

    setTimeoutSpy.mockRestore();
  });

  it("readiness object reference is stable when poll returns identical data (AC4)", async () => {
    const { server } = await import("../test/mocks/server");

    server.use(
      http.get("/api/v1/readiness", () => {
        return HttpResponse.json(makeReadiness(false));
      }),
    );

    vi.useFakeTimers();

    try {
      const { result } = renderHook(() => useTenant(), { wrapper });

      // Let the initial poll (useEffect → fetch → setState) complete.
      // advanceTimersByTimeAsync advances time AND awaits resulting async work.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(100);
      });

      const firstRef = result.current.readiness;
      expect(firstRef).not.toBeNull();

      // Advance past the 15s idle interval to fire the second poll.
      // Does NOT use runAllTimersAsync — that causes infinite rescheduling.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(15001);
      });

      // With fingerprinting: setReadiness NOT called for identical data → same ref (PASS)
      // Without fingerprinting: setReadiness called with new object → different ref (FAIL — red state)
      expect(result.current.readiness).toBe(firstRef);
    } finally {
      vi.useRealTimers();
    }
  });

  it("polls at 15000ms when all tenants are idle (pipeline_running=false)", async () => {
    const { server } = await import("../test/mocks/server");

    server.use(
      http.get("/api/v1/readiness", () => {
        return HttpResponse.json(makeReadiness(false));
      }),
    );

    const setTimeoutSpy = vi.spyOn(globalThis, "setTimeout");

    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    const delays = setTimeoutSpy.mock.calls
      .map(([, delay]) => delay as number | undefined)
      .filter((d): d is number => typeof d === "number");

    // After receiving a ready+idle response, must schedule next poll at 15000
    expect(delays).toContain(15000);
    // Must NOT have scheduled at 5000 for an idle pipeline (only for running)
    expect(delays.filter((d) => d === 5000)).toHaveLength(0);

    setTimeoutSpy.mockRestore();
  });
});
