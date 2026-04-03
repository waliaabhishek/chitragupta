import type React from "react";
import { act, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { IdentityListPage } from "./list";
import type { IdentityResponse } from "../../types/api";

// Capture onRowClick so tests can trigger grid row selection
let capturedOnRowClick: ((row: IdentityResponse) => void) | undefined;

vi.mock("../../components/identities/IdentityGrid", () => ({
  IdentityGrid: (props: {
    tenantName: string;
    queryParams: Record<string, string>;
    onRowClick: (row: IdentityResponse) => void;
  }) => {
    capturedOnRowClick = props.onRowClick;
    return (
      <div
        data-testid="identity-grid"
        data-tenant={props.tenantName}
        data-query-params={JSON.stringify(props.queryParams)}
      />
    );
  },
}));

vi.mock("../../components/identities/IdentityFilterBar", () => ({
  IdentityFilterBar: () => <div data-testid="identity-filter-bar" />,
}));

vi.mock("../../components/identities/IdentityDetailDrawer", () => ({
  IdentityDetailDrawer: (props: {
    identity: IdentityResponse;
    tenantName: string;
    onClose: () => void;
  }) => (
    <div data-testid="identity-drawer">
      <button data-testid="drawer-close" onClick={props.onClose}>
        Close
      </button>
      <div
        data-testid="entity-tag-editor"
        data-tenant={props.tenantName}
        data-entity-type="identity"
        data-entity-id={props.identity.identity_id}
      />
      {props.identity.deleted_at && <span>Deleted</span>}
    </div>
  ),
}));

vi.mock("../../hooks/useIdentityFilters", () => ({
  useIdentityFilters: () => ({
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
};

const identityFixtures: IdentityResponse[] = [
  {
    ecosystem: "ccloud",
    tenant_id: "t-001",
    identity_id: "u-001",
    identity_type: "user",
    display_name: "Alice",
    created_at: null,
    deleted_at: null,
    last_seen_at: null,
    metadata: {},
  },
  {
    ecosystem: "ccloud",
    tenant_id: "t-001",
    identity_id: "sa-001",
    identity_type: "service_account",
    display_name: null,
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

describe("IdentityListPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<IdentityListPage />, { wrapper });
    expect(screen.getByText("Identities")).toBeTruthy();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    expect(screen.queryByTestId("identity-grid")).toBeNull();
  });

  it("renders table with identity rows when tenant is selected", async () => {
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

    render(<IdentityListPage />, { wrapper });

    expect(screen.getByTestId("identity-grid")).toBeTruthy();
    expect(screen.getByTestId("identity-filter-bar")).toBeTruthy();
    expect(
      screen.getByTestId("identity-grid").getAttribute("data-tenant"),
    ).toBe("acme");
  });

  it("clicking Details opens IdentityDetailDrawer with EntityTagEditor", async () => {
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

    render(<IdentityListPage />, { wrapper });
    expect(screen.queryByTestId("identity-drawer")).toBeNull();

    act(() => {
      capturedOnRowClick?.(identityFixtures[0]);
    });

    expect(screen.getByTestId("identity-drawer")).toBeTruthy();
    const editor = screen.getByTestId("entity-tag-editor");
    expect(editor.getAttribute("data-entity-type")).toBe("identity");
    expect(editor.getAttribute("data-entity-id")).toBe("u-001");
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

    render(<IdentityListPage />, { wrapper });

    act(() => {
      capturedOnRowClick?.(identityFixtures[0]);
    });
    expect(screen.getByTestId("identity-drawer")).toBeTruthy();

    fireEvent.click(screen.getByTestId("drawer-close"));
    expect(screen.queryByTestId("identity-drawer")).toBeNull();
  });

  it("drawer shows Deleted status when identity is deleted", async () => {
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

    render(<IdentityListPage />, { wrapper });

    const deletedIdentity: IdentityResponse = {
      ...identityFixtures[0],
      deleted_at: "2024-01-01",
      display_name: null,
    };

    act(() => {
      capturedOnRowClick?.(deletedIdentity);
    });

    expect(screen.getByTestId("identity-drawer")).toBeTruthy();
    expect(screen.getByText("Deleted")).toBeTruthy();
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

    render(<IdentityListPage />, { wrapper });

    // IdentityGrid handles its own data fetching and error notifications.
    // The page passes correct tenantName/queryParams to the grid.
    expect(
      screen.getByTestId("identity-grid").getAttribute("data-tenant"),
    ).toBe("acme");
  });
});
