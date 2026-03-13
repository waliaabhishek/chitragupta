import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { notification } from "antd";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TagManagementPage } from "./list";
import type { PaginatedResponse, TagWithDimensionResponse } from "../../types/api";

// Stable mock tenant
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
    appStatus: "ready" as const,
    readiness: null,
    isReadOnly: false,
  })),
}));

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return (
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      {children}
    </MemoryRouter>
  );
}

describe("TagManagementPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByText("Select a tenant to view tags.")).toBeTruthy();
  });

  it("renders table and search when tenant selected", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    render(<TagManagementPage />, { wrapper });

    expect(screen.getByPlaceholderText("Search tags...")).toBeTruthy();

    await waitFor(() => {
      expect(screen.getByText("Production")).toBeTruthy();
    });
  });

  it("shows 'no tags' empty state for empty result without search", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    server.use(
      http.get("/api/v1/tenants/acme/tags", () => {
        const empty: PaginatedResponse<TagWithDimensionResponse> = {
          items: [],
          total: 0,
          page: 1,
          page_size: 100,
          pages: 0,
        };
        return HttpResponse.json(empty);
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("No tags yet. Add tags from the Chargebacks page.")).toBeTruthy();
    });
  });

  it("shows 'no match' message for empty search result", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    server.use(
      http.get("/api/v1/tenants/acme/tags", () => {
        const empty: PaginatedResponse<TagWithDimensionResponse> = {
          items: [],
          total: 0,
          page: 1,
          page_size: 100,
          pages: 0,
        };
        return HttpResponse.json(empty);
      }),
    );

    render(<TagManagementPage />, { wrapper });

    // Simulate typing in the search box
    const searchInput = screen.getByPlaceholderText("Search tags...");
    fireEvent.change(searchInput, { target: { value: "zzz" } });

    await waitFor(() => {
      expect(screen.getByText("No tags match your search.")).toBeTruthy();
    });
  });

  it("calls notification.error on fetch failure", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    server.use(
      http.get("/api/v1/tenants/acme/tags", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    const errorSpy = vi.spyOn(notification, "error").mockImplementation(vi.fn());

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(errorSpy).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to load tags" }),
      );
    });

    errorSpy.mockRestore();
  });

  it("allows inline editing of display_name", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    let patchCalled = false;
    server.use(
      http.patch("/api/v1/tenants/acme/tags/:id", () => {
        patchCalled = true;
        return HttpResponse.json({
          tag_id: 1,
          tag_key: "env",
          tag_value: "uuid-1",
          display_name: "Updated Name",
          dimension_id: 1,
          created_by: "user",
          created_at: "2026-01-01T00:00:00Z",
        });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    // Wait for data to load
    await waitFor(() => {
      expect(screen.getByText("Production")).toBeTruthy();
    });

    // Click on the display name to start editing
    const editButton = screen.getByText("Production");
    fireEvent.click(editButton);

    // Input should appear
    await waitFor(() => {
      const input = screen.getByDisplayValue("Production");
      expect(input).toBeTruthy();
    });

    // Change the value and blur to save
    const input = screen.getByDisplayValue("Production");
    fireEvent.change(input, { target: { value: "Updated Name" } });
    fireEvent.blur(input);

    await waitFor(() => {
      expect(patchCalled).toBe(true);
    });
  });

  it("allows canceling edit", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("Production")).toBeTruthy();
    });

    // Start editing
    fireEvent.click(screen.getByText("Production"));

    await waitFor(() => {
      expect(screen.getByDisplayValue("Production")).toBeTruthy();
    });

    // Cancel edit
    const cancelButton = screen.getByText("Cancel");
    fireEvent.click(cancelButton);

    // Should return to non-editing state
    await waitFor(() => {
      expect(screen.queryByDisplayValue("Production")).toBeNull();
      expect(screen.getByText("Production")).toBeTruthy();
    });
  });

  it("handles delete with popconfirm", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    let deleteCalled = false;
    server.use(
      http.delete("/api/v1/tenants/acme/tags/:id", () => {
        deleteCalled = true;
        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(<TagManagementPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByText("Production")).toBeTruthy();
    });

    // Click the delete button
    const deleteButtons = screen.getAllByText("Delete");
    fireEvent.click(deleteButtons[0]);

    // Popconfirm should show
    await waitFor(() => {
      expect(screen.getByText("Delete this tag?")).toBeTruthy();
    });

    // Confirm deletion - the popconfirm's confirm button has okText="Delete"
    // Find the button within the popconfirm that appears
    const popconfirmButtons = screen.getAllByRole("button", { name: /delete/i });
    // The last one is the confirmation button in the popconfirm
    fireEvent.click(popconfirmButtons[popconfirmButtons.length - 1]);

    await waitFor(() => {
      expect(deleteCalled).toBe(true);
    });
  });
});
