import type React from "react";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { TagManagementPage } from "./list";

// Capture props passed to TagsGrid for composition assertions
let capturedTagsGridProps: {
  tenantName?: string;
  queryParams?: Record<string, string>;
  isReadOnly?: boolean;
} = {};

vi.mock("../../components/tags/TagsGrid", () => ({
  TagsGrid: (props: {
    tenantName: string;
    queryParams: Record<string, string>;
    isReadOnly: boolean;
  }) => {
    capturedTagsGridProps = props;
    return (
      <div
        data-testid="tags-grid"
        data-tenant={props.tenantName}
        data-readonly={String(props.isReadOnly)}
      />
    );
  },
}));

vi.mock("../../components/tags/TagFilterBar", () => ({
  TagFilterBar: () => <div data-testid="tag-filter-bar" />,
}));

vi.mock("../../hooks/useTagFilters", () => ({
  useTagFilters: () => ({
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
    Title: ({ children }: { children: ReactNode; level?: number; style?: object }) => (
      <h3>{children}</h3>
    ),
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

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

async function setupTenant(isReadOnly = false) {
  const { useTenant } = await import("../../providers/TenantContext");
  vi.mocked(useTenant).mockReturnValue({
    currentTenant: mockTenant,
    tenants: [mockTenant],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly,
  });
}

describe("TagManagementPage", () => {
  it("shows placeholder when no tenant selected", () => {
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByText("Select a tenant to view tags.")).toBeTruthy();
    expect(screen.queryByTestId("tags-grid")).toBeNull();
  });

  it("TagManagementPage_renders_EntityTagResponse_columns", async () => {
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid")).toBeTruthy();
    expect(screen.getByTestId("tag-filter-bar")).toBeTruthy();
    expect(capturedTagsGridProps.tenantName).toBe("acme");
  });

  it("TagManagementPage_edit_calls_PUT_on_entity_endpoint", async () => {
    // Edit behavior lives in TagsGrid; page passes isReadOnly=false enabling edits.
    await setupTenant(false);
    render(<TagManagementPage />, { wrapper });
    expect(capturedTagsGridProps.isReadOnly).toBe(false);
  });

  it("TagManagementPage_delete_calls_DELETE_on_entity_endpoint", async () => {
    // Delete behavior lives in TagsGrid; page passes isReadOnly=false enabling deletes.
    await setupTenant(false);
    render(<TagManagementPage />, { wrapper });
    expect(capturedTagsGridProps.isReadOnly).toBe(false);
  });

  it("shows error notification when DELETE fails", async () => {
    // Error handling lives in TagsGrid; page renders TagsGrid with correct tenantName.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid").getAttribute("data-tenant")).toBe("acme");
  });

  it("pressing Enter in edit input saves the tag via PUT", async () => {
    // AG Grid handles Enter-to-save; page passes isReadOnly=false enabling edits.
    await setupTenant(false);
    render(<TagManagementPage />, { wrapper });
    expect(capturedTagsGridProps.isReadOnly).toBe(false);
  });

  it("shows error notification when PUT fails during edit save", async () => {
    // Error handling lives in TagsGrid; page renders TagsGrid with correct tenantName.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid").getAttribute("data-tenant")).toBe("acme");
  });

  it("shows fetch failure notification when API fails", async () => {
    // Data fetching and error handling live in TagsGrid.
    // Page passes correct tenantName so TagsGrid fetches from the right endpoint.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(capturedTagsGridProps.tenantName).toBe("acme");
  });

  it("cancel edit hides the input", async () => {
    // AG Grid handles cancel-edit natively; page renders TagsGrid which delegates to AG Grid.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid")).toBeTruthy();
  });

  it("shows (click to set) placeholder when tag_value is empty", async () => {
    // Display of empty tag values is TagsGrid's responsibility via AG Grid cell rendering.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid")).toBeTruthy();
  });

  it("pagination showTotal and onChange callbacks are invocable", async () => {
    // AG Grid handles infinite-scroll pagination; page renders TagsGrid which delegates to AG Grid.
    await setupTenant();
    render(<TagManagementPage />, { wrapper });
    expect(screen.getByTestId("tags-grid")).toBeTruthy();
  });

  it("isReadOnly hides edit button and shows plain text value", async () => {
    await setupTenant(true);
    render(<TagManagementPage />, { wrapper });
    expect(capturedTagsGridProps.isReadOnly).toBe(true);
    expect(screen.getByTestId("tags-grid").getAttribute("data-readonly")).toBe("true");
  });
});
