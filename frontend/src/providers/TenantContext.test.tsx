import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { TenantProvider, useTenant, useReadiness } from "./TenantContext";
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

  it("refetch() resets error state and restarts loading (GIT-001)", async () => {
    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    act(() => {
      result.current.refetch();
    });

    expect(result.current.isLoading).toBe(true);
    expect(result.current.error).toBeNull();

    // Let provider finish the second load cycle
    await waitFor(() => expect(result.current.isLoading).toBe(false));
  });

  it("setCurrentTenant before first poll: isReadOnly stays false (GIT-002)", async () => {
    // applyIsReadOnly is a no-op when readinessRef.current is null (before first poll).
    // Calling setCurrentTenant synchronously after renderHook hits that early exit.
    const { result } = renderHook(() => useTenant(), { wrapper });

    act(() => {
      result.current.setCurrentTenant({
        tenant_name: "early-call",
        tenant_id: "t-early",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 0,
        last_calculated_date: null,
      });
    });

    expect(result.current.isReadOnly).toBe(false);

    // Drain async work to avoid act() warnings
    await waitFor(() => expect(result.current.isLoading).toBe(false));
  });

  it("fetchReadiness returns null on non-AbortError network failure and retries (GIT-003)", async () => {
    const { server } = await import("../test/mocks/server");

    let callCount = 0;
    server.use(
      http.get("/api/v1/readiness", () => {
        callCount++;
        if (callCount === 1) return HttpResponse.error(); // network error → catch → null
        return HttpResponse.json({
          status: "ready",
          version: "1.0.0",
          mode: "both",
          tenants: [],
        });
      }),
    );

    vi.useFakeTimers();
    try {
      const { result } = renderHook(() => useTenant(), { wrapper });

      // Let first poll complete (network error → null → schedules 5000ms retry)
      await act(async () => {
        await vi.advanceTimersByTimeAsync(100);
      });

      expect(result.current.isLoading).toBe(true);
      expect(callCount).toBe(1);

      // Advance past the retry delay to trigger second poll
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5001);
      });

      // Second poll succeeds — isLoading becomes false (no waitFor: fake timers mock setTimeout)
      expect(result.current.isLoading).toBe(false);
      expect(callCount).toBeGreaterThanOrEqual(2);
    } finally {
      vi.useRealTimers();
    }
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
      const { result } = renderHook(() => useReadiness(), { wrapper });

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

// ---------------------------------------------------------------------------
// GAP-100 Tests: Context split — useReadiness + isReadOnly transition-driven
// ---------------------------------------------------------------------------

describe("TenantContext — context split (GAP-100)", () => {
  function makeReadinessForTenants(
    tenants: Array<{ name: string; running: boolean }>,
  ): ReadinessResponse {
    return {
      status: "ready",
      version: "1.0.0",
      mode: "both",
      tenants: tenants.map((t) => ({
        tenant_name: t.name,
        tables_ready: true,
        has_data: true,
        pipeline_running: t.running,
        pipeline_stage: t.running ? "gathering" : null,
        pipeline_current_date: t.running ? "2026-03-14" : null,
        last_run_status: t.running ? "running" : "completed",
        last_run_at: null,
        permanent_failure: null,
      })),
    };
  }

  it("useReadiness() called outside TenantProvider throws expected error (verification item 1)", () => {
    // FAILS in red state: useReadiness is not exported from TenantContext yet.
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      expect(() => renderHook(() => useReadiness())).toThrow(
        "useReadiness must be used within TenantProvider",
      );
    } finally {
      consoleError.mockRestore();
    }
  });

  it("readiness poll does NOT trigger useTenant() consumers to re-render (verification item 2)", async () => {
    // FAILS in red state: readiness is in TenantContext useMemo deps → every poll
    // causes contextValue to be a new object → all useTenant consumers re-render.
    //
    // To bypass fingerprinting (which would prevent setReadiness on identical data),
    // we increment a counter in the version field so each poll produces different JSON.
    const { server } = await import("../test/mocks/server");

    let pollCounter = 0;
    server.use(
      http.get("/api/v1/readiness", () => {
        pollCounter++;
        return HttpResponse.json({
          ...makeReadinessForTenants([{ name: "acme", running: true }]),
          version: `1.0.${pollCounter}`, // Different each poll → fingerprint mismatch → setReadiness called
        });
      }),
    );

    vi.useFakeTimers();
    try {
      let tenantRenderCount = 0;

      const { result } = renderHook(
        () => {
          tenantRenderCount++;
          return useTenant();
        },
        { wrapper },
      );

      // Complete initial fetch cycle.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(200);
      });

      expect(result.current.currentTenant?.tenant_name).toBe("acme");
      const renderCountAfterInit = tenantRenderCount;

      // Fire 3 more poll cycles (pipeline running → 5 s interval).
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5001);
      });
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5001);
      });
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5001);
      });

      // With context split: readiness goes to ReadinessContext, useTenant consumers
      // do NOT re-render on readiness polls.
      // Without split (current): each poll calls setReadiness → contextValue memo
      // invalidated → re-render. So renderCount > renderCountAfterInit.
      expect(tenantRenderCount).toBe(renderCountAfterInit);
    } finally {
      vi.useRealTimers();
    }
  });

  it("setCurrentTenant immediately sets isReadOnly=true for a running-pipeline tenant (verification item 3)", async () => {
    // Verify that switching to a tenant with pipeline_running=true reflects in isReadOnly
    // immediately — without waiting for the next poll cycle.
    const { server } = await import("../test/mocks/server");

    server.use(
      http.get("/api/v1/readiness", () =>
        HttpResponse.json(
          makeReadinessForTenants([
            { name: "acme", running: false },
            { name: "globex", running: true },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useTenant(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    // Initial state: acme selected, not running → isReadOnly false.
    expect(result.current.currentTenant?.tenant_name).toBe("acme");
    expect(result.current.isReadOnly).toBe(false);

    // Switch to globex (pipeline_running=true) synchronously.
    act(() => {
      result.current.setCurrentTenant(result.current.tenants[1]);
    });

    // isReadOnly must be true immediately — no extra poll wait.
    expect(result.current.currentTenant?.tenant_name).toBe("globex");
    expect(result.current.isReadOnly).toBe(true);
  });

  it("useReadiness() returns appStatus and readiness from within TenantProvider (verification item 10 pre-condition)", async () => {
    // FAILS in red state: useReadiness is not exported from TenantContext yet.
    const { result } = renderHook(() => useReadiness(), { wrapper });
    await waitFor(() => {
      // After initial readiness fetch, readiness should be non-null.
      expect(result.current.readiness).not.toBeNull();
    });
    expect(result.current.appStatus).toBeDefined();
  });
});
