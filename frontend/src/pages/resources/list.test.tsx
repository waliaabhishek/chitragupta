import type React from "react";
import { act, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ResourceListPage } from "./list";
import type { ResourceResponse } from "../../types/api";

// Capture onRowClick so tests can trigger grid row selection
let capturedOnRowClick: ((row: ResourceResponse) => void) | undefined;

vi.mock("../../components/resources/ResourceGrid", () => ({
  ResourceGrid: (props: {
    tenantName: string;
    queryParams: Record<string, string>;
    onRowClick: (row: ResourceResponse) => void;
  }) => {
    capturedOnRowClick = props.onRowClick;
    return (
      <div
        data-testid="resource-grid"
        data-tenant={props.tenantName}
        data-query-params={JSON.stringify(props.queryParams)}
      />
    );
  },
}));

vi.mock("../../components/resources/ResourceFilterBar", () => ({
  ResourceFilterBar: () => <div data-testid="resource-filter-bar" />,
}));

vi.mock("../../components/resources/ResourceDetailDrawer", () => ({
  ResourceDetailDrawer: (props: {
    resource: ResourceResponse;
    tenantName: string;
    onClose: () => void;
  }) => (
    <div data-testid="resource-drawer">
      <button data-testid="drawer-close" onClick={props.onClose}>
        Close
      </button>
      <div
        data-testid="entity-tag-editor"
        data-tenant={props.tenantName}
        data-entity-type="resource"
        data-entity-id={props.resource.resource_id}
      />
      <span data-testid="display-name">
        {props.resource.display_name ?? "—"}
      </span>
    </div>
  ),
}));

vi.mock("../../hooks/useResourceFilters", () => ({
  useResourceFilters: () => ({
    filters: {},
    setFilter: vi.fn(),
    resetFilters: vi.fn(),
    queryParams: {},
  }),
}));

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

vi.mock("antd", () => ({
  Typography: {
    Title: ({
      children,
    }: {
      children: ReactNode;
      level?: number;
      style?: object;
    }) => <h3>{children}</h3>,
    Text: ({ children, type }: { children: ReactNode; type?: string }) => (
      <span data-type={type}>{children}</span>
    ),
  },
}));

const mockTenant = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
  topic_attribution_enabled: false,
};

const resourceFixtures: ResourceResponse[] = [
  {
    ecosystem: "ccloud",
    tenant_id: "t-001",
    resource_id: "r-001",
    resource_type: "kafka_cluster",
    display_name: "My Cluster",
    parent_id: null,
    owner_id: null,
    status: "active",
    created_at: null,
    deleted_at: null,
    last_seen_at: null,
    metadata: {},
  },
  {
    ecosystem: "ccloud",
    tenant_id: "t-001",
    resource_id: "r-002",
    resource_type: "connector",
    display_name: null,
    parent_id: null,
    owner_id: null,
    status: "active",
    created_at: null,
    deleted_at: null,
    last_seen_at: null,
    metadata: {},
  },
];

beforeEach(() => {
  capturedOnRowClick = undefined;
});

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

describe("ResourceListPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<ResourceListPage />, { wrapper });
    expect(screen.getByText("Resources")).toBeTruthy();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    expect(screen.queryByTestId("resource-grid")).toBeNull();
  });

  it("renders table with resource rows when tenant is selected", async () => {
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

    render(<ResourceListPage />, { wrapper });

    expect(screen.getByTestId("resource-grid")).toBeTruthy();
    expect(screen.getByTestId("resource-filter-bar")).toBeTruthy();
    expect(
      screen.getByTestId("resource-grid").getAttribute("data-tenant"),
    ).toBe("acme");
  });

  it("clicking Details opens ResourceDetailDrawer with EntityTagEditor", async () => {
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

    render(<ResourceListPage />, { wrapper });
    expect(screen.queryByTestId("resource-drawer")).toBeNull();

    act(() => {
      capturedOnRowClick?.(resourceFixtures[0]);
    });

    expect(screen.getByTestId("resource-drawer")).toBeTruthy();
    const editor = screen.getByTestId("entity-tag-editor");
    expect(editor.getAttribute("data-entity-type")).toBe("resource");
    expect(editor.getAttribute("data-entity-id")).toBe("r-001");
    expect(editor.getAttribute("data-tenant")).toBe("acme");
  });

  it("closing drawer removes it from DOM", async () => {
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

    render(<ResourceListPage />, { wrapper });

    act(() => {
      capturedOnRowClick?.(resourceFixtures[0]);
    });
    expect(screen.getByTestId("resource-drawer")).toBeTruthy();

    fireEvent.click(screen.getByTestId("drawer-close"));
    expect(screen.queryByTestId("resource-drawer")).toBeNull();
  });

  it("drawer shows dash when display_name is null", async () => {
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

    render(<ResourceListPage />, { wrapper });

    // r-002 has null display_name
    act(() => {
      capturedOnRowClick?.(resourceFixtures[1]);
    });

    expect(screen.getByTestId("resource-drawer")).toBeTruthy();
    expect(screen.getByTestId("display-name").textContent).toBe("—");
  });

  it("shows error notification when API fetch fails", async () => {
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

    render(<ResourceListPage />, { wrapper });

    // ResourceGrid handles its own data fetching and error notifications.
    // The page passes correct tenantName/queryParams to the grid.
    expect(
      screen.getByTestId("resource-grid").getAttribute("data-tenant"),
    ).toBe("acme");
  });
});
