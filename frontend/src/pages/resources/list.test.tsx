import type React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { ResourceListPage } from "./list";
import type { PaginatedResponse, ResourceResponse } from "../../types/api";

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
    dataSource: ResourceResponse[];
    columns: {
      key: string;
      title: string;
      render?: (v: unknown, r: ResourceResponse) => ReactNode;
      dataIndex?: string;
    }[];
    loading?: boolean;
    rowKey?: string;
    pagination?: { onChange?: (p: number) => void; showTotal?: (t: number) => string; total?: number };
  }) => (
    <div data-testid="resource-table" data-loading={tableLoading ? "true" : undefined}>
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
              key={rowKey ? String(row[rowKey as keyof ResourceResponse]) : i}
              data-testid="resource-row"
            >
              {columns.map((col) => (
                <td key={col.key} data-col={col.key}>
                  {col.render
                    ? col.render(
                        col.dataIndex ? row[col.dataIndex as keyof ResourceResponse] : undefined,
                        row,
                      )
                    : col.dataIndex
                      ? String(row[col.dataIndex as keyof ResourceResponse] ?? "")
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
      <div data-testid="resource-drawer" data-title={title}>
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
  server.use(
    http.get("/api/v1/tenants/acme/resources", () => {
      const response: PaginatedResponse<ResourceResponse> = {
        items: resourceFixtures,
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

describe("ResourceListPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<ResourceListPage />, { wrapper });
    expect(screen.getByText("Resources")).toBeTruthy();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    expect(screen.queryByTestId("resource-table")).toBeNull();
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

    await waitFor(() => {
      expect(screen.getByTestId("resource-table")).toBeTruthy();
      expect(screen.getByText("r-001")).toBeTruthy();
      expect(screen.getByText("kafka_cluster")).toBeTruthy();
      expect(screen.getByText("My Cluster")).toBeTruthy();
    });
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

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(0);
    });

    fireEvent.click(screen.getAllByText("Details")[0]);

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

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(0);
    });

    fireEvent.click(screen.getAllByText("Details")[0]);
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

    await waitFor(() => {
      expect(screen.getAllByText("Details").length).toBeGreaterThan(1);
    });

    // Click Details for r-002 which has null display_name and null owner_id
    fireEvent.click(screen.getAllByText("Details")[1]);

    expect(screen.getByTestId("resource-drawer")).toBeTruthy();
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
      http.get("/api/v1/tenants/acme/resources", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<ResourceListPage />, { wrapper });

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to load resources" }),
      );
    });
  });
});
