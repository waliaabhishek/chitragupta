import type React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { IdentityListPage } from "./list";
import type { IdentityResponse, PaginatedResponse } from "../../types/api";

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

vi.mock("../../components/entities/EntityTagEditor", () => ({
  EntityTagEditor: ({
    tenantName,
    entityType,
    entityId,
  }: {
    tenantName: string;
    entityType: string;
    entityId: string;
  }) => (
    <div
      data-testid="entity-tag-editor"
      data-tenant={tenantName}
      data-entity-type={entityType}
      data-entity-id={entityId}
    />
  ),
}));

vi.mock("antd", () => ({
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number; style?: object }) => (
      <h3>{children}</h3>
    ),
    Text: ({ children, type }: { children: ReactNode; type?: string }) => (
      <span data-type={type}>{children}</span>
    ),
  },
  Button: ({
    children,
    onClick,
    type: btnType,
    size,
  }: {
    children: ReactNode;
    onClick?: () => void;
    type?: string;
    size?: string;
  }) => (
    <button onClick={onClick} data-btn-type={btnType} data-size={size}>
      {children}
    </button>
  ),
  Table: ({
    dataSource,
    columns,
    loading: tableLoading,
    rowKey,
    pagination,
  }: {
    dataSource: IdentityResponse[];
    columns: {
      key: string;
      title: string;
      render?: (v: unknown, r: IdentityResponse) => ReactNode;
      dataIndex?: string;
    }[];
    loading?: boolean;
    rowKey?: string;
    pagination?: { onChange?: (p: number) => void; showTotal?: (t: number) => string; total?: number };
  }) => (
    <div data-testid="identity-table" data-loading={tableLoading ? "true" : undefined}>
      {pagination?.showTotal && (
        <span data-testid="pagination-total">{pagination.showTotal(pagination.total ?? 0)}</span>
      )}
      <table>
        <thead>
          <tr>
            {columns.map((c) => <th key={c.key}>{c.title}</th>)}
          </tr>
        </thead>
        <tbody>
          {(dataSource ?? []).map((row, i) => (
            <tr
              key={rowKey ? String(row[rowKey as keyof IdentityResponse]) : i}
              data-testid="identity-row"
            >
              {columns.map((col) => (
                <td key={col.key} data-col={col.key}>
                  {col.render
                    ? col.render(
                        col.dataIndex ? row[col.dataIndex as keyof IdentityResponse] : undefined,
                        row,
                      )
                    : col.dataIndex
                      ? String(row[col.dataIndex as keyof IdentityResponse] ?? "")
                      : null}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ),
  Drawer: ({
    children,
    open,
    onClose,
    title,
  }: {
    children: ReactNode;
    open?: boolean;
    onClose?: () => void;
    title?: string;
    width?: number;
  }) =>
    open ? (
      <div data-testid="identity-drawer" data-title={title}>
        <button data-testid="drawer-close" onClick={onClose}>
          Close
        </button>
        {children}
      </div>
    ) : null,
  Descriptions: Object.assign(
    ({
      children,
    }: {
      children: ReactNode;
      column?: number;
      size?: string;
      bordered?: boolean;
    }) => <dl>{children}</dl>,
    {
      Item: ({ children, label }: { children: ReactNode; label?: string }) => (
        <div><dt>{label}</dt><dd>{children}</dd></div>
      ),
    },
  ),
  Divider: () => <hr />,
  notification: {
    error: vi.fn(),
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
  server.use(
    http.get("/api/v1/tenants/acme/identities", () => {
      const response: PaginatedResponse<IdentityResponse> = {
        items: identityFixtures,
        total: 2,
        page: 1,
        page_size: 100,
        pages: 1,
      };
      return HttpResponse.json(response);
    }),
  );
});

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

describe("IdentityListPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<IdentityListPage />, { wrapper });
    expect(screen.getByText("Identities")).toBeTruthy();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    expect(screen.queryByTestId("identity-table")).toBeNull();
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

    await waitFor(() => {
      expect(screen.getByTestId("identity-table")).toBeTruthy();
      expect(screen.getByText("u-001")).toBeTruthy();
      expect(screen.getByText("user")).toBeTruthy();
      expect(screen.getByText("Alice")).toBeTruthy();
    });
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

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(0);
    });

    fireEvent.click(screen.getAllByText("Details")[0]);

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

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(0);
    });

    fireEvent.click(screen.getAllByText("Details")[0]);
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

    server.use(
      http.get("/api/v1/tenants/acme/identities", () => {
        const response: PaginatedResponse<IdentityResponse> = {
          items: [{ ...identityFixtures[0], deleted_at: "2024-01-01", display_name: null }],
          total: 1,
          page: 1,
          page_size: 100,
          pages: 1,
        };
        return HttpResponse.json(response);
      }),
    );

    render(<IdentityListPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(0);
    });

    fireEvent.click(screen.getAllByText("Details")[0]);
    expect(screen.getByTestId("identity-drawer")).toBeTruthy();
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

    const { notification } = await import("antd");

    server.use(
      http.get("/api/v1/tenants/acme/identities", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<IdentityListPage />, { wrapper });

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to load identities" }),
      );
    });
  });
});
