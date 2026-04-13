import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "../../test/mocks/server";
import { useGraphData } from "../../hooks/useGraphData";
import { useTenant } from "../../providers/TenantContext";
import { ExplorerPage } from "./ExplorerPage";

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: {
      tenant_name: "acme",
      tenant_id: "acme",
      ecosystem: "ccloud",
      dates_pending: 0,
      dates_calculated: 10,
      last_calculated_date: "2026-04-10",
      topic_attribution_status: "enabled",
      topic_attribution_error: null,
    },
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
  useReadiness: vi.fn(() => ({ appStatus: "ready", readiness: null })),
}));

vi.mock("../../contexts/AppShellContext", () => ({
  useAppShell: vi.fn(() => ({
    isDark: false,
    setSidebarCollapsed: vi.fn(),
  })),
}));

// Mock useGraphData — overridden per-test or restored to real impl in integration test.
vi.mock("../../hooks/useGraphData", () => ({
  useGraphData: vi.fn(() => ({
    data: { nodes: [], edges: [] },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useGraphNavigation", () => ({
  useGraphNavigation: vi.fn(() => ({
    state: { focusId: null, focusType: null, breadcrumbs: [] },
    navigate: vi.fn(),
    goBack: vi.fn(),
    goToRoot: vi.fn(),
    goToBreadcrumb: vi.fn(),
  })),
}));

// GraphContainer mock — renders data-testid + per-node data attributes so tests
// can observe enriched nodes (including phantom nodes from enrichWithPhantomNodes).
vi.mock("./GraphContainer", () => ({
  GraphContainer: ({
    nodes,
  }: {
    nodes: Array<{ id: string; status: string }>;
  }) => (
    <div data-testid="graph-container">
      {nodes.map((n) => (
        <div key={n.id} data-node-id={n.id} data-node-status={n.status} />
      ))}
    </div>
  ),
}));

// ---------------------------------------------------------------------------
// Default mock reset helpers
// ---------------------------------------------------------------------------

const MOCK_TENANT = {
  tenant_name: "acme",
  tenant_id: "acme",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: "2026-04-10",
  topic_attribution_status: "enabled" as const,
  topic_attribution_error: null,
};

function resetTenantMock() {
  vi.mocked(useTenant).mockReturnValue({
    currentTenant: MOCK_TENANT,
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  });
}

function resetGraphDataMock() {
  vi.mocked(useGraphData).mockReturnValue({
    data: { nodes: [], edges: [] },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  });
}

// ---------------------------------------------------------------------------
// Render helpers
// ---------------------------------------------------------------------------

function renderExplorerPage() {
  return render(
    <MemoryRouter>
      <ExplorerPage />
    </MemoryRouter>,
  );
}

function renderExplorerPageWithQueryClient() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return render(
    <MemoryRouter>
      <QueryClientProvider client={queryClient}>
        <ExplorerPage />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

// A graph node as the API returns it (cost as string, cross_references as array).
function makeApiNode(overrides: Record<string, unknown> = {}) {
  return {
    id: "env-abc",
    resource_type: "environment",
    display_name: "my-env",
    cost: "100.00",
    created_at: null,
    deleted_at: null,
    tags: {},
    parent_id: null,
    cloud: null,
    region: null,
    status: "active",
    cross_references: [] as string[],
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Unit tests — useGraphData mocked
// ---------------------------------------------------------------------------

describe("ExplorerPage", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
  });

  it("renders graph container on mount", () => {
    renderExplorerPage();
    expect(
      document.querySelector("[data-testid='graph-container']"),
    ).not.toBeNull();
  });

  it("shows 'Select a tenant' placeholder when no tenant selected", () => {
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: null,
      tenants: [],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: false,
    });
    vi.mocked(useGraphData).mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();
    expect(screen.getByText(/select a tenant/i)).toBeInTheDocument();
  });

  it("shows loading state during fetch", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();
    expect(screen.getByTestId("graph-loading")).toBeInTheDocument();
  });

  it("shows error state on API failure", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: null,
      isLoading: false,
      error: "Entity not found",
      refetch: vi.fn(),
    });

    renderExplorerPage();
    expect(screen.getByText(/entity not found/i)).toBeInTheDocument();
  });

  it("fetches root view (no focus) on initial load with tenant", () => {
    vi.mocked(useGraphData).mockClear();
    renderExplorerPage();
    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ focus: null, tenantName: "acme" }),
    );
  });

  it("renders breadcrumb trail", () => {
    renderExplorerPage();
    expect(document.querySelector("[data-testid='breadcrumb-trail']")).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// enrichWithPhantomNodes tests (GIT-003)
// Tests the private enrichWithPhantomNodes function via ExplorerPage rendering.
// GraphContainer mock exposes nodes as data-node-* attributes.
// ---------------------------------------------------------------------------

describe("ExplorerPage — enrichWithPhantomNodes", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
  });

  it("two identity nodes sharing a cross_reference produce only one phantom node", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-001", cross_references: ["lkc-phantom"] }),
          makeApiNode({ id: "sa-002", cross_references: ["lkc-phantom"] }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    const phantomNodes = document.querySelectorAll("[data-node-status='phantom']");
    expect(phantomNodes).toHaveLength(1);
    expect((phantomNodes[0] as HTMLElement).dataset.nodeId).toBe("lkc-phantom");
  });

  it("cross_reference ID already in real nodes → no phantom created", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-001", cross_references: ["lkc-real"] }),
          makeApiNode({ id: "lkc-real" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    expect(document.querySelectorAll("[data-node-status='phantom']")).toHaveLength(0);
  });

  it("no cross_references → no phantom nodes added", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "env-abc", cross_references: [] })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    expect(document.querySelectorAll("[data-node-status='phantom']")).toHaveLength(0);
    expect(document.querySelector("[data-node-id='env-abc']")).not.toBeNull();
  });

  it("phantom node has status='phantom'", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "sa-001", cross_references: ["lkc-ghost"] })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    const phantomEl = document.querySelector(
      "[data-node-id='lkc-ghost']",
    ) as HTMLElement | null;
    expect(phantomEl).not.toBeNull();
    expect(phantomEl!.dataset.nodeStatus).toBe("phantom");
  });
});

// ---------------------------------------------------------------------------
// Integration test (GIT-001)
// Real useGraphData fires → MSW intercepts → enrichWithPhantomNodes runs →
// phantom node reaches GraphContainer.
// ---------------------------------------------------------------------------

describe("ExplorerPage — integration", () => {
  beforeEach(() => {
    resetTenantMock();
  });

  afterEach(() => {
    // Restore mock to the factory default for subsequent test suites.
    resetGraphDataMock();
  });

  it("data flows: real useGraphData → MSW → enrichWithPhantomNodes → renderer", async () => {
    const { useGraphData: realUseGraphData } =
      await vi.importActual<typeof import("../../hooks/useGraphData")>(
        "../../hooks/useGraphData",
      );
    vi.mocked(useGraphData).mockImplementation(
      realUseGraphData as typeof useGraphData,
    );

    server.use(
      http.get("/api/v1/tenants/acme/graph", () =>
        HttpResponse.json({
          nodes: [
            makeApiNode({
              id: "sa-001",
              resource_type: "service_account",
              cross_references: ["lkc-phantom"],
            }),
          ],
          edges: [],
        }),
      ),
    );

    renderExplorerPageWithQueryClient();

    await waitFor(
      () => {
        expect(
          document.querySelector("[data-node-status='phantom']"),
        ).not.toBeNull();
      },
      { timeout: 5000 },
    );

    const phantomEl = document.querySelector(
      "[data-node-id='lkc-phantom']",
    ) as HTMLElement | null;
    expect(phantomEl).not.toBeNull();
    expect(phantomEl!.dataset.nodeStatus).toBe("phantom");
  });
});
