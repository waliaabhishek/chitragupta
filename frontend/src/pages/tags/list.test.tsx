// TASK-160.02 TDD red phase — TagManagementPage rewrite for entity-level tag system.
// Tests 10-12 FAIL because:
//   - EntityTagResponse type does not exist yet
//   - Current page shows TagWithDimensionResponse columns (display_name, identity_id, etc.)
//   - Current edit/delete use /tags/:id endpoints, not /entities/{type}/{id}/tags/{key}
import type React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TagManagementPage } from "./list";
import type { EntityTagResponse, PaginatedResponse } from "../../types/api";

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

// Minimal antd mock — enough for TagManagementPage interactions in jsdom
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
    danger,
    size: _size,
    loading,
    htmlType,
    style,
  }: {
    children: ReactNode;
    onClick?: () => void;
    type?: string;
    danger?: boolean;
    size?: string;
    loading?: boolean;
    htmlType?: string;
    style?: object;
  }) => (
    <button
      onClick={onClick}
      data-btn-type={btnType}
      data-danger={danger ? "true" : undefined}
      disabled={loading}
      type={(htmlType as "button" | "submit" | "reset") ?? "button"}
      style={style}
    >
      {children}
    </button>
  ),
  Input: ({
    value,
    onChange,
    onPressEnter,
    onBlur,
    autoFocus,
    maxLength,
    style,
  }: {
    value?: string;
    onChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
    onPressEnter?: () => void;
    onBlur?: () => void;
    autoFocus?: boolean;
    maxLength?: number;
    style?: object;
  }) => (
    <input
      data-testid="value-input"
      value={value}
      onChange={onChange}
      onKeyDown={(e) => e.key === "Enter" && onPressEnter?.()}
      onBlur={onBlur}
      autoFocus={autoFocus}
      maxLength={maxLength}
      style={style}
    />
  ),
  Space: ({ children }: { children: ReactNode; wrap?: boolean }) => <span>{children}</span>,
  Table: ({
    dataSource,
    columns,
    loading: tableLoading,
    rowKey,
    pagination,
  }: {
    dataSource: EntityTagResponse[];
    columns: {
      key: string;
      title: string;
      render?: (v: unknown, r: EntityTagResponse) => ReactNode;
      dataIndex?: string;
    }[];
    loading?: boolean;
    rowKey?: string;
    pagination?: { onChange?: (p: number) => void; showTotal?: (t: number) => string; total?: number; pageSize?: number; current?: number };
  }) => (
    <div data-testid="tag-table" data-loading={tableLoading ? "true" : undefined}>
      {pagination?.showTotal && (
        <span data-testid="pagination-total">{pagination.showTotal(pagination.total ?? 0)}</span>
      )}
      {pagination?.onChange && (
        <button data-testid="pagination-next" onClick={() => pagination.onChange?.(2)}>Next Page</button>
      )}
      <table>
        <thead>
          <tr>
            {columns.map((c) => <th key={c.key}>{c.title}</th>)}
          </tr>
        </thead>
        <tbody>
          {(dataSource ?? []).map((row, i) => (
            <tr key={rowKey ? String(row[rowKey as keyof EntityTagResponse]) : i} data-testid="tag-row">
              {columns.map((col) => (
                <td key={col.key} data-col={col.key}>
                  {col.render
                    ? col.render(col.dataIndex ? row[col.dataIndex as keyof EntityTagResponse] : undefined, row)
                    : col.dataIndex ? String(row[col.dataIndex as keyof EntityTagResponse] ?? "") : null}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ),
  Popconfirm: ({
    children,
    onConfirm,
    title,
  }: {
    children: ReactNode;
    onConfirm?: () => void;
    title?: string;
    okText?: string;
    cancelText?: string;
  }) => (
    <span data-testid="popconfirm" data-title={title} onClick={onConfirm}>
      {children}
    </span>
  ),
  notification: {
    error: vi.fn(),
  },
}));

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <MemoryRouter>
      {children}
    </MemoryRouter>
  );
}

// EntityTagResponse fixtures — new shape replacing TagWithDimensionResponse
const entityTagFixtures: EntityTagResponse[] = [
  {
    tag_id: 1,
    tenant_id: "t-001",
    entity_type: "resource",
    entity_id: "r-001",
    tag_key: "env",
    tag_value: "prod",
    created_by: "ui",
    created_at: null,
  },
  {
    tag_id: 2,
    tenant_id: "t-001",
    entity_type: "identity",
    entity_id: "u-001",
    tag_key: "team",
    tag_value: "platform",
    created_by: "ui",
    created_at: null,
  },
];

beforeEach(() => {
  server.use(
    http.get("/api/v1/tenants/acme/tags", () => {
      const response: PaginatedResponse<EntityTagResponse> = {
        items: [entityTagFixtures[0]],
        total: 1,
        page: 1,
        page_size: 100,
        pages: 1,
      };
      return HttpResponse.json(response);
    }),
  );
});

describe("TagManagementPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByText("Select a tenant to view tags.")).toBeTruthy();
  });

  it("TagManagementPage_renders_EntityTagResponse_columns", async () => {
    // TASK-160.02 test 10: page shows entity_type, entity_id, tag_key, tag_value columns.
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
      http.get("/api/v1/tenants/acme/tags", () => {
        const response: PaginatedResponse<EntityTagResponse> = {
          items: entityTagFixtures,
          total: 2,
          page: 1,
          page_size: 100,
          pages: 1,
        };
        return HttpResponse.json(response);
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      // entity_type column visible
      expect(screen.getByText("resource")).toBeTruthy();
      // entity_id column visible
      expect(screen.getByText("r-001")).toBeTruthy();
      // tag_key column visible
      expect(screen.getByText("env")).toBeTruthy();
      // tag_value column visible (click-to-edit button)
      expect(screen.getByText("prod")).toBeTruthy();
    });
  });

  it("TagManagementPage_edit_calls_PUT_on_entity_endpoint", async () => {
    // TASK-160.02 test 11: editing a tag calls PUT /entities/{type}/{id}/tags/{key}.
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

    let putCalled = false;
    server.use(
      http.put("/api/v1/tenants/acme/entities/resource/r-001/tags/env", async ({ request }) => {
        putCalled = true;
        const body = await request.json() as Record<string, string>;
        return HttpResponse.json({
          ...entityTagFixtures[0],
          tag_value: body.tag_value ?? entityTagFixtures[0].tag_value,
        });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    // Wait for data to load — value cell renders as a button
    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    // Click the value cell to enter inline edit mode
    fireEvent.click(screen.getByText("prod"));

    // Input should appear
    await waitFor(() => {
      expect(screen.getByTestId("value-input")).toBeTruthy();
    });

    const input = screen.getByTestId("value-input");
    fireEvent.change(input, { target: { value: "staging" } });
    fireEvent.blur(input);

    await waitFor(() => {
      expect(putCalled).toBe(true);
    });
  });

  it("TagManagementPage_delete_calls_DELETE_on_entity_endpoint", async () => {
    // TASK-160.02 test 12: deleting a tag calls DELETE /entities/{type}/{id}/tags/{key}.
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

    let deleteCalled = false;
    server.use(
      http.delete("/api/v1/tenants/acme/entities/resource/r-001/tags/env", () => {
        deleteCalled = true;
        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("env")).toBeTruthy();
    });

    // Click delete via Popconfirm (mock triggers onConfirm on click)
    fireEvent.click(screen.getByText("Delete"));

    await waitFor(() => {
      expect(deleteCalled).toBe(true);
    });
  });

  it("shows error notification when DELETE fails", async () => {
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
      http.delete("/api/v1/tenants/acme/entities/resource/r-001/tags/env", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("env")).toBeTruthy();
    });

    fireEvent.click(screen.getByText("Delete"));

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to delete tag" }),
      );
    });
  });

  it("pressing Enter in edit input saves the tag via PUT", async () => {
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

    let putCalled = false;
    server.use(
      http.put("/api/v1/tenants/acme/entities/resource/r-001/tags/env", async ({ request }) => {
        putCalled = true;
        const body = await request.json() as Record<string, string>;
        return HttpResponse.json({ ...entityTagFixtures[0], tag_value: body.tag_value });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    fireEvent.click(screen.getByText("prod"));

    await waitFor(() => {
      expect(screen.getByTestId("value-input")).toBeTruthy();
    });

    const input = screen.getByTestId("value-input");
    fireEvent.change(input, { target: { value: "staging" } });
    fireEvent.keyDown(input, { key: "Enter" });

    await waitFor(() => {
      expect(putCalled).toBe(true);
    });
  });

  it("shows error notification when PUT fails during edit save", async () => {
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
      http.put("/api/v1/tenants/acme/entities/resource/r-001/tags/env", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    fireEvent.click(screen.getByText("prod"));

    await waitFor(() => {
      expect(screen.getByTestId("value-input")).toBeTruthy();
    });

    const input = screen.getByTestId("value-input");
    fireEvent.change(input, { target: { value: "staging" } });
    fireEvent.blur(input);

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to update tag" }),
      );
    });
  });

  it("shows fetch failure notification when API fails", async () => {
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
      http.get("/api/v1/tenants/acme/tags", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to load tags" }),
      );
    });
  });

  it("cancel edit hides the input", async () => {
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

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    fireEvent.click(screen.getByText("prod"));
    expect(screen.getByTestId("value-input")).toBeTruthy();

    fireEvent.click(screen.getByText("Cancel"));
    expect(screen.queryByTestId("value-input")).toBeNull();
  });

  it("shows (click to set) placeholder when tag_value is empty", async () => {
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
      http.get("/api/v1/tenants/acme/tags", () => {
        const response: PaginatedResponse<EntityTagResponse> = {
          items: [{ ...entityTagFixtures[0], tag_value: "" }],
          total: 1,
          page: 1,
          page_size: 100,
          pages: 1,
        };
        return HttpResponse.json(response);
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("(click to set)")).toBeTruthy();
    });
  });

  it("pagination showTotal and onChange callbacks are invocable", async () => {
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

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("pagination-total")).toBeTruthy();
    });

    // showTotal is rendered
    expect(screen.getByTestId("pagination-total").textContent).toContain("tags");

    // onChange doesn't throw
    expect(() => fireEvent.click(screen.getByTestId("pagination-next"))).not.toThrow();
  });

  it("isReadOnly hides edit button and shows plain text value", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: true,
    });

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    // Delete button should not be rendered in read-only mode
    expect(screen.queryByText("Delete")).toBeNull();
    // No edit input
    expect(screen.queryByTestId("value-input")).toBeNull();
  });
});
