import type React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ChargebackListPage } from "./list";
import type { ChargebackResponse } from "../../types/api";

// Mock heavy sub-components
vi.mock("../../components/chargebacks/ChargebackGrid", () => ({
  ChargebackGrid: ({
    onRowClick,
  }: {
    onRowClick?: (row: ChargebackResponse) => void;
  }) => (
    <div data-testid="chargeback-grid">
      <button
        data-testid="trigger-row-click"
        onClick={() =>
          onRowClick?.({
            dimension_id: 1,
            ecosystem: "ccloud",
            tenant_id: "t-001",
            timestamp: "2024-01-10T00:00:00Z",
            resource_id: "r-001",
            product_category: "KAFKA",
            product_type: "KAFKA_NUM_BYTES",
            identity_id: "user@example.com",
            cost_type: "USAGE",
            amount: "10.00",
            allocation_method: null,
            allocation_detail: null,
            tags: { env: "prod" },
            metadata: {},
          })
        }
      >
        Trigger Row Click
      </button>
    </div>
  ),
}));

vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(
    ({
      onReset,
      onRefresh,
    }: {
      onReset: () => void;
      onRefresh?: () => void;
    }) => (
      <div data-testid="filter-panel">
        <button onClick={onReset}>Reset</button>
        {onRefresh !== undefined && (
          <button data-testid="filter-refresh" onClick={onRefresh}>
            Refresh Data
          </button>
        )}
      </div>
    ),
  ),
}));

vi.mock("./ChargebackDetailDrawer", () => ({
  ChargebackDetailDrawer: vi.fn(
    ({
      onClose,
      dimensionId,
      inheritedTags,
    }: {
      onClose: () => void;
      dimensionId: number | null;
      inheritedTags: Record<string, string>;
    }) =>
      dimensionId !== null ? (
        <div data-testid="drawer" data-dimension-id={dimensionId}>
          <button data-testid="trigger-close" onClick={onClose}>
            Close
          </button>
          {Object.keys(inheritedTags ?? {}).length > 0 && (
            <span data-testid="drawer-inherited-tags">
              {Object.entries(inheritedTags)
                .map(([k, v]) => `${k}: ${v}`)
                .join(", ")}
            </span>
          )}
        </div>
      ) : null,
  ),
}));

vi.mock("../../components/chargebacks/ExportButton", () => ({
  ExportButton: () => <button data-testid="export-button">Export CSV</button>,
}));

// Mock antd
vi.mock("antd", () => ({
  Typography: {
    Title: ({
      children,
    }: {
      children: ReactNode;
      level?: number;
      style?: object;
    }) => <h3>{children}</h3>,
    Text: ({ children }: { children: ReactNode; type?: string }) => (
      <span>{children}</span>
    ),
  },
}));

// Stable mock tenant for tests that need one
const mockTenant = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
};

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: null,
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
}));

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

describe("ChargebackListPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<ChargebackListPage />, { wrapper });

    expect(screen.getByText("Chargebacks")).toBeTruthy();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    expect(screen.queryByTestId("chargeback-grid")).toBeNull();
  });

  it("renders grid and filter panel when tenant is selected", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });

    expect(screen.getByTestId("chargeback-grid")).toBeTruthy();
    expect(screen.getByTestId("filter-panel")).toBeTruthy();
  });

  it("shows export button when tenant is selected", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });
    expect(screen.getByTestId("export-button")).toBeTruthy();
  });

  it("ChargebackListPage_no_bulk_state_SelectionToolbar_and_BulkTagModal_absent", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });

    expect(screen.queryByTestId("selection-toolbar")).toBeNull();
    expect(screen.queryByTestId("bulk-modal")).toBeNull();
  });

  it("passes onRefresh to FilterPanel and calling it does not throw when grid not ready", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });

    expect(screen.getByTestId("filter-refresh")).toBeInTheDocument();
    expect(() =>
      fireEvent.click(screen.getByTestId("filter-refresh")),
    ).not.toThrow();
  });

  it("row click passes full row tags as inheritedTags to drawer", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-row-click"));

    // Drawer appears with correct dimension_id
    expect(screen.getByTestId("drawer").getAttribute("data-dimension-id")).toBe(
      "1",
    );
    // Inherited tags from the row are passed through
    expect(screen.getByTestId("drawer-inherited-tags").textContent).toContain(
      "env: prod",
    );
  });

  it("closing the drawer resets selection", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(<ChargebackListPage />, { wrapper });

    fireEvent.click(screen.getByTestId("trigger-row-click"));
    expect(screen.getByTestId("drawer")).toBeTruthy();

    fireEvent.click(screen.getByTestId("trigger-close"));
    expect(screen.queryByTestId("drawer")).toBeNull();
  });

  it("?selected= with non-numeric value does not open drawer", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(
      <MemoryRouter initialEntries={["/?selected=not-a-number"]}>
        <ChargebackListPage />
      </MemoryRouter>,
    );

    expect(screen.queryByTestId("drawer")).toBeNull();
  });

  it("?selected= URL param opens drawer with that dimensionId", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });

    render(
      <MemoryRouter initialEntries={["/?selected=42"]}>
        <ChargebackListPage />
      </MemoryRouter>,
    );

    expect(screen.getByTestId("drawer").getAttribute("data-dimension-id")).toBe(
      "42",
    );
  });
});
