// GAP-100: Updated to split useTenant / useReadiness mocks (Category A).
// New test 10: PipelineStatusBanner shows stage text during pipeline run after context split.
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PipelineStatusBanner } from "./PipelineStatusBanner";
import type { AppStatus } from "../providers/TenantContext";
import type { ReadinessResponse, TenantReadiness } from "../types/api";

// Mock both hooks — after context split, PipelineStatusBanner calls useTenant() for
// currentTenant and useReadiness() for appStatus + readiness.
vi.mock("../providers/TenantContext", () => ({
  useTenant: vi.fn(),
  useReadiness: vi.fn(),
}));

import { useTenant, useReadiness } from "../providers/TenantContext";
const mockUseTenant = vi.mocked(useTenant);
const mockUseReadiness = vi.mocked(useReadiness);

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
    topic_attribution_status: "disabled",
    topic_attribution_error: null,
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

const baseTenantContext = {
  currentTenant: null,
  tenants: [],
  isLoading: false,
  error: null,
  refetch: vi.fn(),
  setCurrentTenant: vi.fn(),
  isReadOnly: false,
};

beforeEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Test 10: no_data + api mode
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — no_data status", () => {
  it("api mode shows instance-does-not-run-pipeline message", () => {
    // GAP-100: appStatus/readiness now come from useReadiness(), not useTenant().
    // FAILS in red state: PipelineStatusBanner still calls useTenant() for these.
    mockUseTenant.mockReturnValue(baseTenantContext);
    mockUseReadiness.mockReturnValue({
      appStatus: "no_data" as AppStatus,
      readiness: makeReadiness({ status: "no_data", mode: "api" }),
    });

    render(<PipelineStatusBanner />);

    expect(
      screen.getByText(/This instance does not run the pipeline/i),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/Waiting for pipeline to run/i),
    ).not.toBeInTheDocument();
  });

  it("both mode shows waiting-for-pipeline message", () => {
    mockUseTenant.mockReturnValue(baseTenantContext);
    mockUseReadiness.mockReturnValue({
      appStatus: "no_data" as AppStatus,
      readiness: makeReadiness({ status: "no_data", mode: "both" }),
    });

    render(<PipelineStatusBanner />);

    expect(
      screen.getByText(/Waiting for pipeline to run/i),
    ).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Per-tenant failure banner when appStatus="ready"
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — per-tenant permanent_failure", () => {
  it("shows error banner for current tenant permanent_failure when appStatus=ready", () => {
    const tenant = makeTenant({ permanent_failure: "API credentials invalid" });
    mockUseTenant.mockReturnValue({
      ...baseTenantContext,
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_status: "disabled",
        topic_attribution_error: null,
      },
    });
    mockUseReadiness.mockReturnValue({
      appStatus: "ready" as AppStatus,
      readiness: makeReadiness({
        status: "ready",
        mode: "both",
        tenants: [tenant],
      }),
    });

    const { container } = render(<PipelineStatusBanner />);

    expect(container.firstChild).not.toBeNull();
    expect(
      screen.getByText(/acme.*API credentials invalid/i),
    ).toBeInTheDocument();

    const alert = container.querySelector(".ant-alert-error");
    expect(alert).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// All-tenants-failed — existing error behaviour preserved
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

    mockUseTenant.mockReturnValue(baseTenantContext);
    mockUseReadiness.mockReturnValue({
      appStatus: "error" as AppStatus,
      readiness: makeReadiness({ status: "error", mode: "both", tenants }),
    });

    render(<PipelineStatusBanner />);

    expect(screen.getByText(/Credentials expired/i)).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// GAP-100 verification item 10: stage text still renders after context split
// ---------------------------------------------------------------------------

describe("PipelineStatusBanner — pipeline running stage text (GAP-100 verification item 10)", () => {
  it("shows stage text and current date during pipeline run after context split", () => {
    // FAILS in red state: PipelineStatusBanner does not call useReadiness() yet.
    // After the fix: Banner calls useReadiness() for appStatus/readiness and useTenant()
    // for currentTenant. This test verifies the stage text still renders correctly.
    const acmeTenant = makeTenant({
      tenant_name: "acme",
      pipeline_running: true,
      pipeline_stage: "calculating",
      pipeline_current_date: "2026-03-14",
    });

    mockUseTenant.mockReturnValue({
      ...baseTenantContext,
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 5,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_status: "disabled",
        topic_attribution_error: null,
      },
    });
    mockUseReadiness.mockReturnValue({
      appStatus: "ready" as AppStatus,
      readiness: makeReadiness({
        status: "ready",
        mode: "both",
        tenants: [acmeTenant],
      }),
    });

    render(<PipelineStatusBanner />);

    // Banner must show the "calculating" stage description with the current date.
    expect(
      screen.getByText(/Calculating chargebacks for 2026-03-14/i),
    ).toBeInTheDocument();
  });
});
