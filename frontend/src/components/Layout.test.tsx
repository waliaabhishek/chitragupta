import type React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { AppLayout } from "./Layout";

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

function wrapper({ children }: { children: React.ReactNode }): React.JSX.Element {
  return (
    <MemoryRouter>
      {children}
    </MemoryRouter>
  );
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
