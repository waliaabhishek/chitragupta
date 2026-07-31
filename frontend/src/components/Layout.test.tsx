import type React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { AppLayout } from "./Layout";
import { useTenant, useReadiness } from "../providers/TenantContext";
import { useResourceLinks } from "../providers/ResourceLinkContext";

// Mock TenantContext to avoid provider requirement.
// GAP-100: useReadiness added — PipelineStatusBanner (rendered by AppLayout) now calls both hooks.
vi.mock("../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: null,
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
  useReadiness: vi.fn(() => ({
    appStatus: "ready" as const,
    readiness: null,
  })),
}));

// Mock TenantSelector to avoid heavy network/context setup.
vi.mock("./TenantSelector", () => ({
  TenantSelector: () => <div data-testid="tenant-selector" />,
}));

// Mock ResourceLinkContext to avoid provider requirement.
vi.mock("../providers/ResourceLinkContext", () => ({
  useResourceLinks: vi.fn(() => ({
    enabled: false,
    setEnabled: vi.fn(),
    resolveUrl: vi.fn(() => null),
    isLoading: false,
  })),
  ResourceLinkProvider: ({ children }: { children: React.ReactNode }) =>
    children,
}));

function wrapper({
  children,
}: {
  children: React.ReactNode;
}): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

describe("AppLayout toggle button", () => {
  it("renders 'Switch to light mode' button when isDark is true", () => {
    render(
      <AppLayout isDark={true} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByTitle("Switch to light mode")).toBeTruthy();
  });

  it("renders 'Switch to dark mode' button when isDark is false", () => {
    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByTitle("Switch to dark mode")).toBeTruthy();
  });

  it("calls onToggleTheme when toggle button is clicked", () => {
    const onToggleTheme = vi.fn();

    render(
      <AppLayout isDark={true} onToggleTheme={onToggleTheme}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    fireEvent.click(screen.getByTitle("Switch to light mode"));

    expect(onToggleTheme).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// TASK-187: Topic Attribution nav badge when feature is disabled
// ---------------------------------------------------------------------------

describe("TASK-187: Topic Attribution nav item", () => {
  it("topic_attribution_status=disabled → Topic Attribution nav item shows 'Not configured' badge", () => {
    vi.mocked(useTenant).mockReturnValue({
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
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByText("Not configured")).toBeTruthy();
  });

  it("topic_attribution_status=enabled → Topic Attribution nav item shows normal label without 'Not configured'", () => {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_status: "enabled",
        topic_attribution_error: null,
      },
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.queryByText("Not configured")).toBeNull();
  });

  it("topic_attribution_status=config_error → Topic Attribution nav item shows 'Config error' badge", () => {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_status: "config_error",
        topic_attribution_error: "requires metrics",
      },
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByText("Config error")).toBeTruthy();
    expect(screen.queryByText("Not configured")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// TASK-197: Links toggle tooltip
// ---------------------------------------------------------------------------

describe("TASK-197: Links toggle tooltip", () => {
  it("renders tooltip text about Confluent Cloud console", async () => {
    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    await userEvent.hover(screen.getByRole("switch"));
    expect(await screen.findByText(/Confluent Cloud console/i)).toBeTruthy();
  });

  it("tooltip text mentions connectors and identity pools not supported", async () => {
    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    await userEvent.hover(screen.getByRole("switch"));
    expect(
      await screen.findByText(/Connectors and identity pools/i),
    ).toBeTruthy();
  });

  it("tooltip text mentions deleted resources", async () => {
    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    await userEvent.hover(screen.getByRole("switch"));
    expect(await screen.findByText(/Deleted resources/i)).toBeTruthy();
  });

  it("Switch does not have title attribute", () => {
    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    // The Links switch button should not carry a native title attribute
    const linksSwitch = screen.getByRole("switch");
    expect(linksSwitch.getAttribute("title")).toBeNull();
    // Switch checked state reflects mocked enabled: false
    expect(linksSwitch).not.toBeChecked();
  });

  it("Switch onChange fires when clicked", () => {
    const mockSetEnabled = vi.fn();
    vi.mocked(useResourceLinks).mockReturnValueOnce({
      enabled: true,
      setEnabled: mockSetEnabled,
      resolveUrl: vi.fn(() => null),
      isLoading: false,
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    const linksSwitch = screen.getByRole("switch");
    fireEvent.click(linksSwitch);

    expect(mockSetEnabled).toHaveBeenCalledTimes(1);
  });
});

describe("FOCUS Mapping Preview feature readiness navigation", () => {
  const currentTenant = {
    tenant_name: "acme",
    tenant_id: "t-001",
    ecosystem: "ccloud",
    dates_pending: 0,
    dates_calculated: 10,
    last_calculated_date: null,
    topic_attribution_status: "enabled" as const,
    topic_attribution_error: null,
  };

  function setFocusState(
    state: "disabled" | "ready" | "upgrading" | "degraded" | "unavailable",
  ): void {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant,
      tenants: [currentTenant],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });
    vi.mocked(useReadiness).mockReturnValue({
      appStatus: "ready",
      readiness: {
        status: "ready",
        version: "1.0.0",
        mode: "both",
        tenants: [
          {
            tenant_name: "acme",
            tables_ready: true,
            has_data: true,
            pipeline_running: false,
            pipeline_stage: null,
            pipeline_current_date: null,
            last_run_status: "completed",
            last_run_at: null,
            permanent_failure: null,
            topic_attribution_status: "enabled",
            topic_attribution_error: null,
            focus_preview_state: state,
            focus_preview_completed_repair_dates:
              state === "upgrading" || state === "degraded" ? 2 : null,
            focus_preview_total_repair_dates:
              state === "upgrading" || state === "degraded" ? 3 : null,
            focus_preview_message: null,
            focus_preview_ordinary_retention: null,
            focus_preview_evidence_retention: null,
          },
        ],
      },
    });
  }

  it.each([
    ["disabled", "Not configured"],
    ["upgrading", "Upgrading"],
    ["degraded", "Needs retry"],
    ["unavailable", "Unavailable"],
  ] as const)("shows the exact %s badge", (state, badge) => {
    setFocusState(state);

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByText(badge)).toBeInTheDocument();
  });

  it("shows no FOCUS badge when ready", () => {
    setFocusState("ready");

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    const item = screen.getByText("FOCUS Mapping Preview").closest("li");
    expect(item).not.toHaveClass("ant-menu-item-disabled");
    expect(item?.querySelector(".ant-badge")).toBeNull();
  });

  it("keeps FOCUS and unrelated navigation enabled while upgrading", () => {
    setFocusState("upgrading");

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(
      screen.getByText("FOCUS Mapping Preview").closest("li"),
    ).not.toHaveClass("ant-menu-item-disabled");
    expect(screen.getByText("Dashboard").closest("li")).not.toHaveClass(
      "ant-menu-item-disabled",
    );
    expect(screen.getByText("Chargebacks").closest("li")).not.toHaveClass(
      "ant-menu-item-disabled",
    );
  });

  it("keeps the existing degraded badge and navigation availability for retention-only failures", () => {
    setFocusState("degraded");
    vi.mocked(useReadiness).mockReturnValue({
      appStatus: "ready",
      readiness: {
        ...vi.mocked(useReadiness)().readiness!,
        tenants: [
          {
            ...vi.mocked(useReadiness)().readiness!.tenants[0],
            focus_preview_completed_repair_dates: null,
            focus_preview_total_repair_dates: null,
            focus_preview_message:
              "Retention cleanup needs attention. Review the latest retention outcome and worker logs; existing valid Preview data remains available.",
            focus_preview_ordinary_retention: {
              attempted_at: "2026-07-30T23:25:01Z",
              status: "failure",
              diagnostic: {
                code: "focus_preview_ordinary_retention_failed",
                message:
                  "Ordinary tenant retention cleanup failed. Review worker logs and restore tenant storage; existing valid Preview data remains available.",
                error_type: "OperationalError",
              },
            },
            focus_preview_evidence_retention: null,
          },
        ],
      },
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.getByText("Needs retry")).toBeInTheDocument();
    expect(
      screen.getByText("FOCUS Mapping Preview").closest("li"),
    ).not.toHaveClass("ant-menu-item-disabled");
    expect(screen.getByText("Dashboard").closest("li")).not.toHaveClass(
      "ant-menu-item-disabled",
    );
  });

  it("uses only the selected tenant state", () => {
    setFocusState("ready");
    vi.mocked(useReadiness).mockReturnValue({
      appStatus: "ready",
      readiness: {
        ...vi.mocked(useReadiness)().readiness!,
        tenants: [
          {
            ...vi.mocked(useReadiness)().readiness!.tenants[0],
            tenant_name: "other",
            focus_preview_state: "upgrading",
            focus_preview_completed_repair_dates: 1,
            focus_preview_total_repair_dates: 3,
          },
          vi.mocked(useReadiness)().readiness!.tenants[0],
        ],
      },
    });

    render(
      <AppLayout isDark={false} onToggleTheme={vi.fn()}>
        <div>content</div>
      </AppLayout>,
      { wrapper },
    );

    expect(screen.queryByText("Upgrading")).not.toBeInTheDocument();
    expect(screen.getByText("FOCUS Mapping Preview")).toBeInTheDocument();
  });
});
