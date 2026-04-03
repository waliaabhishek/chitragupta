import type React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { AppLayout } from "./Layout";
import { useTenant } from "../providers/TenantContext";

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
  it("topic_attribution_enabled=false → Topic Attribution nav item shows 'Not configured' badge", () => {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_enabled: false,
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

  it("topic_attribution_enabled=true → Topic Attribution nav item shows normal label without 'Not configured'", () => {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: {
        tenant_name: "acme",
        tenant_id: "t-001",
        ecosystem: "ccloud",
        dates_pending: 0,
        dates_calculated: 10,
        last_calculated_date: null,
        topic_attribution_enabled: true,
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
});
