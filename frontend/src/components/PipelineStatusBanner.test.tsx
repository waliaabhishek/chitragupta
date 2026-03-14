import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PipelineStatusBanner } from "./PipelineStatusBanner";
import type { AppStatus } from "../providers/TenantContext";
import type { ReadinessResponse, TenantReadiness } from "../types/api";

// Mock useTenant so tests can control context values without a provider tree.
vi.mock("../providers/TenantContext", () => ({
  useTenant: vi.fn(),
}));

import { useTenant } from "../providers/TenantContext";
const mockUseTenant = vi.mocked(useTenant);

function makeTenant(overrides: Partial<TenantReadiness> = {}): TenantReadiness {
  return {
    tenant_name: "acme",
    tables_ready: true,
    has_data: true,
    pipeline_running: false,
    pipeline_stage: null,
    pipeline_current_date: null,
    last_run_status: null,
    last_run_at: null,
    permanent_failure: null,
    ...overrides,
  };
}

function makeReadiness(
  overrides: Partial<ReadinessResponse> = {},
): ReadinessResponse {
  return {
    status: "ready",
    version: "1.0.0",
    mode: "both",
    tenants: [makeTenant()],
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Test 10: no_data + api mode
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — no_data status", () => {
  it("api mode shows instance-does-not-run-pipeline message", () => {
    mockUseTenant.mockReturnValue({
      appStatus: "no_data" as AppStatus,
      readiness: makeReadiness({ status: "no_data", mode: "api" }),
      currentTenant: null,
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(<PipelineStatusBanner />);

    expect(
      screen.getByText(
        /This instance does not run the pipeline/i,
      ),
    ).toBeInTheDocument();
    // Must NOT show the "waiting" message
    expect(
      screen.queryByText(/Waiting for pipeline to run/i),
    ).not.toBeInTheDocument();
  });

  // ---------------------------------------------------------------------------
  // Test 11: no_data + both mode
  // ---------------------------------------------------------------------------

  it("both mode shows waiting-for-pipeline message", () => {
    mockUseTenant.mockReturnValue({
      appStatus: "no_data" as AppStatus,
      readiness: makeReadiness({ status: "no_data", mode: "both" }),
      currentTenant: null,
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(<PipelineStatusBanner />);

    expect(
      screen.getByText(/Waiting for pipeline to run/i),
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Test 14: Per-tenant failure banner when appStatus="ready"
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — per-tenant permanent_failure", () => {
  it("shows error banner for current tenant permanent_failure when appStatus=ready", () => {
    const tenant = makeTenant({ permanent_failure: "API credentials invalid" });
    mockUseTenant.mockReturnValue({
      appStatus: "ready" as AppStatus,
      readiness: makeReadiness({
        status: "ready",
        mode: "both",
        tenants: [tenant],
      }),
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
      },
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    const { container } = render(<PipelineStatusBanner />);

    // Banner must render — not return null
    expect(container.firstChild).not.toBeNull();

    // Must show the tenant name and failure reason
    expect(screen.getByText(/acme.*API credentials invalid/i)).toBeInTheDocument();

    // Must be an error-type alert
    const alert = container.querySelector(".ant-alert-error");
    expect(alert).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Test 15: All-tenants-failed — existing error behaviour preserved
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — all tenants failed", () => {
  it("shows combined error message when all tenants have permanent_failure", () => {
    const tenants: TenantReadiness[] = [
      makeTenant({
        tenant_name: "acme",
        permanent_failure: "Credentials expired",
      }),
      makeTenant({
        tenant_name: "globex",
        permanent_failure: "Quota exceeded",
      }),
    ];

    mockUseTenant.mockReturnValue({
      appStatus: "error" as AppStatus,
      readiness: makeReadiness({
        status: "error",
        mode: "both",
        tenants,
      }),
      currentTenant: null,
      tenants: [],
      isLoading: false,
      error: "acme: Credentials expired; globex: Quota exceeded",
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(<PipelineStatusBanner />);

    // Banner must show at least one failure message
    expect(
      screen.getByText(/Credentials expired/i),
    ).toBeInTheDocument();
  });
});
