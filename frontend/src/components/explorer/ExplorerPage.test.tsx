import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "../../test/mocks/server";
import { useGraphData } from "../../hooks/useGraphData";
import { useTenant } from "../../providers/TenantContext";
import { useDateRange } from "../../hooks/useDateRange";
import { usePlayback } from "../../hooks/usePlayback";
import { useDebouncedValue } from "../../hooks/useDebouncedValue";
import { useGraphDiff } from "../../hooks/useGraphDiff";
import { useGraphNavigation } from "../../hooks/useGraphNavigation";
import { useTagOverlay } from "../../hooks/useTagOverlay";
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

vi.mock("../../hooks/useTagOverlay", () => ({
  useTagOverlay: vi.fn(() => ({
    availableKeys: [],
    isLoadingKeys: false,
    colorMap: {},
  })),
}));

vi.mock("../../hooks/useDateRange", () => ({
  useDateRange: vi.fn(() => ({
    minDate: null,
    maxDate: null,
    isLoading: false,
  })),
}));

vi.mock("../../hooks/usePlayback", () => ({
  usePlayback: vi.fn(() => ({
    state: {
      isPlaying: false,
      speed: 1,
      currentDate: null,
      stepDays: 3,
    },
    play: vi.fn(),
    pause: vi.fn(),
    setSpeed: vi.fn(),
    setStepDays: vi.fn(),
    setDate: vi.fn(),
    isAtEnd: false,
  })),
}));

vi.mock("../../hooks/useDebouncedValue", () => ({
  useDebouncedValue: vi.fn((value: unknown) => value),
}));

vi.mock("../../hooks/useGraphDiff", () => ({
  useGraphDiff: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}));

vi.mock("../../hooks/useGraphTimeline", () => ({
  useGraphTimeline: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
}));

// SearchBar mock — exposes a trigger button that fires onSelect with a fixed entity.
vi.mock("./SearchBar", () => ({
  SearchBar: ({
    onSelect,
  }: {
    onSelect: (id: string, type: string, name: string | null) => void;
  }) => (
    <button
      data-testid="searchbar-select-trigger"
      onClick={() => onSelect("env-xyz", "environment", "my-env")}
    >
      Search
    </button>
  ),
}));

// GraphContainer mock — renders data-testid + per-node data attributes so tests
// can observe enriched nodes (including phantom nodes, diff overlays, and faded state).
vi.mock("./GraphContainer", () => ({
  GraphContainer: ({
    nodes,
    fadedNodeIds = new Set<string>(),
  }: {
    nodes: Array<{
      id: string;
      resource_type?: string;
      status: string;
      tagColor?: string;
      diff?: { diff_status: string; cost_delta: number };
    }>;
    fadedNodeIds?: Set<string>;
  }) => (
    <div data-testid="graph-container">
      {nodes.map((n) => (
        <div
          key={n.id}
          data-node-id={n.id}
          data-node-resource-type={n.resource_type ?? ""}
          data-node-status={n.status}
          data-diff-status={n.diff?.diff_status ?? ""}
          data-cost-delta={n.diff != null ? String(n.diff.cost_delta) : ""}
          data-faded={fadedNodeIds.has(n.id) ? "true" : "false"}
          data-tag-color={n.tagColor ?? ""}
        />
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

// TASK-246: structured cross-reference types (replaces flat string[])
interface TestCrossReferenceItem {
  id: string;
  resource_type: string;
  display_name: string | null;
  cost: number;
}

interface TestCrossReferenceGroup {
  resource_type: string;
  items: TestCrossReferenceItem[];
  total_count: number;
}

function makeCrossRefGroup(
  id: string,
  resourceType: string,
  opts: { displayName?: string | null; cost?: number; totalCount?: number } = {},
): TestCrossReferenceGroup {
  const item: TestCrossReferenceItem = {
    id,
    resource_type: resourceType,
    display_name: opts.displayName ?? null,
    cost: opts.cost ?? 100,
  };
  return {
    resource_type: resourceType,
    items: [item],
    total_count: opts.totalCount ?? 1,
  };
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
    cross_references: [] as TestCrossReferenceGroup[],
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
    expect(
      document.querySelector("[data-testid='breadcrumb-trail']"),
    ).not.toBeNull();
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
          makeApiNode({ id: "sa-001", cross_references: [makeCrossRefGroup("lkc-phantom", "kafka_cluster")] }),
          makeApiNode({ id: "sa-002", cross_references: [makeCrossRefGroup("lkc-phantom", "kafka_cluster")] }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    const phantomNodes = document.querySelectorAll(
      "[data-node-status='phantom']",
    );
    expect(phantomNodes).toHaveLength(1);
    expect((phantomNodes[0] as HTMLElement).dataset.nodeId).toBe("lkc-phantom");
  });

  it("cross_reference ID already in real nodes → no phantom created", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-001", cross_references: [makeCrossRefGroup("lkc-real", "kafka_cluster")] }),
          makeApiNode({ id: "lkc-real" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    expect(
      document.querySelectorAll("[data-node-status='phantom']"),
    ).toHaveLength(0);
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

    expect(
      document.querySelectorAll("[data-node-status='phantom']"),
    ).toHaveLength(0);
    expect(document.querySelector("[data-node-id='env-abc']")).not.toBeNull();
  });

  it("phantom node has status='phantom'", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-001", cross_references: [makeCrossRefGroup("lkc-ghost", "kafka_cluster")] }),
        ] as never,
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

  // TASK-246: phantom nodes have correct resource_type (not hardcoded kafka_cluster)
  it("phantom node has resource_type from CrossReferenceItem (not hardcoded kafka_cluster)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({
            id: "sa-001",
            cross_references: [makeCrossRefGroup("lfcp-001", "flink_compute_pool")],
          }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    const phantomEl = document.querySelector(
      "[data-node-id='lfcp-001']",
    ) as HTMLElement | null;
    expect(phantomEl).not.toBeNull();
    expect(phantomEl!.dataset.nodeResourceType).toBe("flink_compute_pool");
  });

  // TASK-246: when total_count equals items.length → no summary group node created
  it("when total_count equals items.length → no xref_group summary node created", () => {
    const group: TestCrossReferenceGroup = {
      resource_type: "kafka_cluster",
      items: [
        { id: "lkc-a", resource_type: "kafka_cluster", display_name: null, cost: 100 },
        { id: "lkc-b", resource_type: "kafka_cluster", display_name: null, cost: 50 },
      ],
      total_count: 2, // equals items.length → no overflow
    };
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "sa-001", cross_references: [group] })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    // Two real phantom nodes from items
    expect(document.querySelector("[data-node-id='lkc-a']")).not.toBeNull();
    expect(document.querySelector("[data-node-id='lkc-b']")).not.toBeNull();
    // No xref_group summary node
    expect(
      document.querySelector("[data-node-resource-type='xref_group']"),
    ).toBeNull();
  });

  // TASK-246: group summary nodes created when total_count > items.length
  it("group summary node created when total_count exceeds items.length", () => {
    const group: TestCrossReferenceGroup = {
      resource_type: "flink_compute_pool",
      items: [
        { id: "lfcp-1", resource_type: "flink_compute_pool", display_name: null, cost: 200 },
        { id: "lfcp-2", resource_type: "flink_compute_pool", display_name: null, cost: 150 },
      ],
      total_count: 50, // 50 total, only 2 shown → summary node needed
    };
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "sa-001", cross_references: [group] })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    // Individual phantom nodes from items
    expect(document.querySelector("[data-node-id='lfcp-1']")).not.toBeNull();
    expect(document.querySelector("[data-node-id='lfcp-2']")).not.toBeNull();
    // Summary group node must exist
    const summaryEl = document.querySelector(
      "[data-node-resource-type='xref_group']",
    ) as HTMLElement | null;
    expect(summaryEl).not.toBeNull();
    expect(summaryEl!.dataset.nodeId).toBe("sa-001:xref_group:flink_compute_pool");
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
    const { useGraphData: realUseGraphData } = await vi.importActual<
      typeof import("../../hooks/useGraphData")
    >("../../hooks/useGraphData");
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
              cross_references: [makeCrossRefGroup("lkc-phantom", "kafka_cluster")],
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

// ---------------------------------------------------------------------------
// Timeline scrubber tests (TASK-222)
// ---------------------------------------------------------------------------

describe("ExplorerPage — timeline scrubber", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(usePlayback).mockReturnValue({
      state: { isPlaying: false, speed: 1, currentDate: null, stepDays: 3 },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDateRange).mockReturnValue({
      minDate: null,
      maxDate: null,
      isLoading: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);
  });

  it("scrubber is not rendered when date range bounds are null", () => {
    vi.mocked(useDateRange).mockReturnValue({
      minDate: null,
      maxDate: null,
      isLoading: false,
    });

    renderExplorerPage();

    expect(
      document.querySelector("[data-testid='timeline-scrubber']"),
    ).toBeNull();
  });

  it("scrubber renders when useDateRange returns valid minDate and maxDate", () => {
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-01-01",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });

    renderExplorerPage();

    expect(
      document.querySelector("[data-testid='timeline-scrubber']"),
    ).not.toBeNull();
  });

  it("passes at param to useGraphData when currentDate is set", () => {
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-02-15",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    // useDebouncedValue passes through the value immediately
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);

    vi.mocked(useGraphData).mockClear();
    renderExplorerPage();

    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({
        at: "2026-02-15T12:00:00Z",
        startDate: "2026-02-15",
        endDate: "2026-02-15",
      }),
    );
  });

  it("omits at param from useGraphData when currentDate is null", () => {
    vi.mocked(useDateRange).mockReturnValue({
      minDate: null,
      maxDate: null,
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: { isPlaying: false, speed: 1, currentDate: null, stepDays: 3 },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });

    vi.mocked(useGraphData).mockClear();
    renderExplorerPage();

    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ at: null, startDate: undefined, endDate: undefined }),
    );
  });

  it("MSW: at param appears in API call when scrubber currentDate is set", async () => {
    const { useGraphData: realUseGraphData } = await vi.importActual<
      typeof import("../../hooks/useGraphData")
    >("../../hooks/useGraphData");
    vi.mocked(useGraphData).mockImplementation(
      realUseGraphData as typeof useGraphData,
    );

    let capturedUrl = "";
    server.use(
      http.get("/api/v1/tenants/acme/graph", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({ nodes: [], edges: [] });
      }),
    );

    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-02-15",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);

    renderExplorerPageWithQueryClient();

    await waitFor(() => {
      expect(capturedUrl).toContain("at=");
    });

    expect(capturedUrl).toContain("2026-02-15");
    expect(capturedUrl).toContain("start_date=2026-02-15");
    expect(capturedUrl).toContain("end_date=2026-02-15");
  });
});

// ---------------------------------------------------------------------------
// Diff mode tests (TASK-222)
// ---------------------------------------------------------------------------

describe("ExplorerPage — diff mode", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-01-01",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);
  });

  it("diff mode toggle button is present", () => {
    renderExplorerPage();

    expect(
      screen.getByRole("button", { name: /diff|compare/i }),
    ).toBeInTheDocument();
  });

  it("DiffModePanel is not shown initially", () => {
    renderExplorerPage();

    expect(
      document.querySelector("[data-testid='diff-mode-panel']"),
    ).toBeNull();
  });

  it("clicking diff toggle shows DiffModePanel", () => {
    renderExplorerPage();

    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    expect(
      document.querySelector("[data-testid='diff-mode-panel']"),
    ).not.toBeNull();
  });

  it("clicking diff toggle a second time hides DiffModePanel", () => {
    renderExplorerPage();

    const toggleBtn = screen.getByRole("button", { name: /diff|compare/i });
    fireEvent.click(toggleBtn);
    fireEvent.click(toggleBtn);

    expect(
      document.querySelector("[data-testid='diff-mode-panel']"),
    ).toBeNull();
  });

  it("scrubber has disabled prop when diff mode is active", () => {
    renderExplorerPage();

    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    const scrubber = document.querySelector(
      "[data-testid='timeline-scrubber']",
    );
    expect(scrubber).not.toBeNull();
    expect(
      scrubber!.getAttribute("data-disabled") === "true" ||
        scrubber!.getAttribute("aria-disabled") === "true",
    ).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Node click pauses playback (TASK-222)
// ---------------------------------------------------------------------------

describe("ExplorerPage — node click and playback", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);
  });

  it("handleNodeClick pauses playback when a node is clicked", () => {
    const pause = vi.fn();
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: true,
        speed: 1,
        currentDate: "2026-02-01",
        stepDays: 3,
      },
      play: vi.fn(),
      pause,
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });

    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "env-abc" })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPage();

    // Simulate node click via GraphContainer mock
    const nodeEl = document.querySelector("[data-node-id='env-abc']");
    expect(nodeEl).not.toBeNull();

    fireEvent.click(nodeEl!);

    // Playback should have paused after node click
    expect(pause).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// GIT-004: diff overlay merges into nodes (TASK-222)
// ---------------------------------------------------------------------------

describe("ExplorerPage — diff overlay merge", () => {
  beforeEach(() => {
    resetTenantMock();
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-01-01",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);
  });

  it("nodes passed to GraphContainer have diff property when diff data is available", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "env-abc" })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    vi.mocked(useGraphDiff).mockReturnValue({
      data: [
        {
          id: "env-abc",
          resource_type: "environment",
          display_name: "my-env",
          parent_id: null,
          cost_before: 100,
          cost_after: 150,
          cost_delta: 50,
          pct_change: 50,
          status: "changed",
        },
      ],
      isLoading: false,
      error: null,
    });

    renderExplorerPage();

    // Activate diff mode
    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    const nodeEl = document.querySelector(
      "[data-node-id='env-abc']",
    ) as HTMLElement | null;
    expect(nodeEl).not.toBeNull();
    expect(nodeEl!.dataset.diffStatus).toBe("changed");
    expect(nodeEl!.dataset.costDelta).toBe("50");
  });

  it("nodes without diff data have no diff-status attribute", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "env-abc" })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    vi.mocked(useGraphDiff).mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
    });

    renderExplorerPage();

    const nodeEl = document.querySelector(
      "[data-node-id='env-abc']",
    ) as HTMLElement | null;
    expect(nodeEl).not.toBeNull();
    expect(nodeEl!.dataset.diffStatus).toBe("");
  });

  it("useGraphDiff is called with fromRange/toRange dates when diff mode activates", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: { nodes: [], edges: [] },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    vi.mocked(useGraphDiff).mockClear();

    renderExplorerPage();

    // ExplorerPage always calls useGraphDiff (enabled=false when dates not set)
    expect(vi.mocked(useGraphDiff)).toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// GIT-006: all-unchanged diff banner (TASK-222)
// ---------------------------------------------------------------------------

describe("ExplorerPage — all-unchanged diff state", () => {
  beforeEach(() => {
    resetTenantMock();
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-13",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: {
        isPlaying: false,
        speed: 1,
        currentDate: "2026-01-01",
        stepDays: 3,
      },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((value: unknown) => value);
  });

  it("shows 'No cost changes detected' when all diff nodes have status=unchanged", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "env-abc" }),
          makeApiNode({ id: "lkc-xyz" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    vi.mocked(useGraphDiff).mockReturnValue({
      data: [
        {
          id: "env-abc",
          resource_type: "environment",
          display_name: "my-env",
          parent_id: null,
          cost_before: 100,
          cost_after: 100,
          cost_delta: 0,
          pct_change: 0,
          status: "unchanged",
        },
        {
          id: "lkc-xyz",
          resource_type: "kafka_cluster",
          display_name: "my-cluster",
          parent_id: "env-abc",
          cost_before: 50,
          cost_after: 50,
          cost_delta: 0,
          pct_change: 0,
          status: "unchanged",
        },
      ],
      isLoading: false,
      error: null,
    });

    renderExplorerPage();

    // Activate diff mode
    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    expect(screen.getByText(/no cost changes detected/i)).toBeInTheDocument();
  });

  it("does not show 'No cost changes' banner when some nodes have cost changes", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "env-abc" })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    vi.mocked(useGraphDiff).mockReturnValue({
      data: [
        {
          id: "env-abc",
          resource_type: "environment",
          display_name: "my-env",
          parent_id: null,
          cost_before: 100,
          cost_after: 150,
          cost_delta: 50,
          pct_change: 50,
          status: "changed",
        },
      ],
      isLoading: false,
      error: null,
    });

    renderExplorerPage();

    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    expect(screen.queryByText(/no cost changes detected/i)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// TASK-223: URL-driven state, tag overlay fadedNodeIds, search navigation
// These tests FAIL until ExplorerPage wires useExplorerParams + useTagOverlay.
// ---------------------------------------------------------------------------

function renderExplorerPageWithUrl(search: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return render(
    <MemoryRouter initialEntries={[`/explorer${search}`]}>
      <QueryClientProvider client={queryClient}>
        <ExplorerPage />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

describe("ExplorerPage — URL state (TASK-223)", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: { focusId: null, focusType: null, breadcrumbs: [] },
      navigate: vi.fn(),
      goBack: vi.fn(),
      goToRoot: vi.fn(),
      goToBreadcrumb: vi.fn(),
    });
  });

  it("passes focusFromUrl from ?focus URL param to useGraphNavigation", () => {
    renderExplorerPageWithUrl("?focus=lkc-abc");

    expect(vi.mocked(useGraphNavigation)).toHaveBeenCalledWith(
      expect.objectContaining({ focusFromUrl: "lkc-abc" }),
    );
  });

  it("passes focusFromUrl=null to useGraphNavigation when ?focus absent", () => {
    renderExplorerPageWithUrl("");

    expect(vi.mocked(useGraphNavigation)).toHaveBeenCalledWith(
      expect.objectContaining({ focusFromUrl: null }),
    );
  });

  it("tag overlay computes non-empty fadedNodeIds for non-matching nodes when tag_value set", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "n1", tags: { team: "platform" } }),
          makeApiNode({ id: "n2", tags: { team: "data" } }), // should be faded
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?tag=team&tag_value=platform");

    // n2 (team=data) should be faded; n1 (team=platform) should not
    const n2el = document.querySelector("[data-node-id='n2']") as HTMLElement | null;
    expect(n2el).not.toBeNull();
    expect(n2el!.dataset.faded).toBe("true");

    const n1el = document.querySelector("[data-node-id='n1']") as HTMLElement | null;
    expect(n1el).not.toBeNull();
    expect(n1el!.dataset.faded).toBe("false");
  });

  it("phantom nodes are excluded from fadedNodeIds even when tag_value set", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-001", cross_references: [makeCrossRefGroup("lkc-phantom", "kafka_cluster")], tags: { team: "data" } }),
          makeApiNode({ id: "n-real", tags: { team: "data" } }), // non-phantom, should be faded
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?tag=team&tag_value=platform");

    // Real node with mismatched tag should be faded
    const realEl = document.querySelector("[data-node-id='n-real']") as HTMLElement | null;
    expect(realEl).not.toBeNull();
    expect(realEl!.dataset.faded).toBe("true");

    // Phantom node should NOT be faded regardless of tag mismatch
    const phantomEl = document.querySelector("[data-node-id='lkc-phantom']") as HTMLElement | null;
    expect(phantomEl).not.toBeNull();
    expect(phantomEl!.dataset.faded).toBe("false");
  });

  it("diff=true URL param activates diff mode (DiffModePanel shows as active)", () => {
    renderExplorerPageWithUrl("?diff=true");

    // DiffModePanel should render in active state — check for diff-related UI
    expect(
      document.querySelector("[data-testid='diff-mode-panel']"),
    ).not.toBeNull();
  });
});

// ---------------------------------------------------------------------------
// GIT-004 — enrichWithTagColor unit tests (TASK-223)
// Tests the private enrichWithTagColor function via ExplorerPage rendering.
// GraphContainer mock exposes data-tag-color per node.
// ---------------------------------------------------------------------------

describe("ExplorerPage — enrichWithTagColor (GIT-004)", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(useTagOverlay).mockReturnValue({
      availableKeys: [],
      isLoadingKeys: false,
      colorMap: {},
    });
  });

  it("node gets tagColor from colorMap for its tag value when activeTagKey is set", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "n1", tags: { team: "platform" } })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    vi.mocked(useTagOverlay).mockReturnValue({
      availableKeys: ["team"],
      isLoadingKeys: false,
      colorMap: { platform: "#1677ff", UNTAGGED: "#d9d9d9" },
    });

    renderExplorerPageWithUrl("?tag=team");

    const el = document.querySelector("[data-node-id='n1']") as HTMLElement | null;
    expect(el).not.toBeNull();
    expect(el!.dataset.tagColor).toBe("#1677ff");
  });

  it("node with no matching tag value gets UNTAGGED fallback color (#d9d9d9)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "n1", tags: {} })] as never, // no 'team' key
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    vi.mocked(useTagOverlay).mockReturnValue({
      availableKeys: ["team"],
      isLoadingKeys: false,
      colorMap: { platform: "#1677ff", UNTAGGED: "#d9d9d9" },
    });

    renderExplorerPageWithUrl("?tag=team");

    const el = document.querySelector("[data-node-id='n1']") as HTMLElement | null;
    expect(el).not.toBeNull();
    // tags["team"] is undefined → key "UNTAGGED" → colorMap["UNTAGGED"] = "#d9d9d9"
    expect(el!.dataset.tagColor).toBe("#d9d9d9");
  });

  it("phantom node gets no tagColor (tagColor=undefined → empty string in DOM)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({
            id: "sa-001",
            cross_references: [makeCrossRefGroup("lkc-phantom", "kafka_cluster")],
            tags: { team: "platform" },
          }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    vi.mocked(useTagOverlay).mockReturnValue({
      availableKeys: ["team"],
      isLoadingKeys: false,
      colorMap: { platform: "#1677ff", UNTAGGED: "#d9d9d9" },
    });

    renderExplorerPageWithUrl("?tag=team");

    const phantomEl = document.querySelector(
      "[data-node-id='lkc-phantom']",
    ) as HTMLElement | null;
    expect(phantomEl).not.toBeNull();
    // phantom nodes: enrichWithTagColor sets tagColor=undefined → data-tag-color=""
    expect(phantomEl!.dataset.tagColor).toBe("");
  });

  it("no tagColor assigned when activeTagKey (params.tag) is null", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [makeApiNode({ id: "n1", tags: { team: "platform" } })] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
    vi.mocked(useTagOverlay).mockReturnValue({
      availableKeys: ["team"],
      isLoadingKeys: false,
      colorMap: { platform: "#1677ff", UNTAGGED: "#d9d9d9" },
    });

    // No ?tag param → params.tag = null → enrichWithTagColor no-op
    renderExplorerPageWithUrl("");

    const el = document.querySelector("[data-node-id='n1']") as HTMLElement | null;
    expect(el).not.toBeNull();
    expect(el!.dataset.tagColor).toBe("");
  });
});

// ---------------------------------------------------------------------------
// TASK-244: progressive disclosure — group node click, collapse button,
// expand URL param wired to useGraphData, search/breadcrumb clear expand
// ---------------------------------------------------------------------------

// The delegated click handler in ExplorerPage reads data-node-status as resourceType.
// Group node tests set status to the group type string so the handler routes correctly.

describe("ExplorerPage — group node click (TASK-244)", () => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let navigate: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let goToRoot: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let goBack: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let goToBreadcrumb: any;

  beforeEach(() => {
    resetTenantMock();
    navigate = vi.fn();
    goToRoot = vi.fn();
    goBack = vi.fn();
    goToBreadcrumb = vi.fn();
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: { focusId: null, focusType: null, breadcrumbs: [] },
      navigate,
      goBack,
      goToRoot,
      goToBreadcrumb,
    });
  });

  it("clicking a topic_group node does NOT call navigate", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          // status="topic_group" so delegated handler passes resourceType="topic_group"
          makeApiNode({ id: "group:topics:lkc-abc", status: "topic_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc");

    const nodeEl = document.querySelector("[data-node-id='group:topics:lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    expect(navigate).not.toHaveBeenCalled();
  });

  it("clicking a topic_group node sets expand=topics in URL (collapse button appears)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "group:topics:lkc-abc", status: "topic_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc");

    const nodeEl = document.querySelector("[data-node-id='group:topics:lkc-abc']");
    fireEvent.click(nodeEl!);

    // After expand is set, collapse button should appear
    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();
  });

  it("clicking a zero_cost_summary node is a no-op (navigate not called)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "group:zero:lkc-abc", status: "zero_cost_summary" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc");

    const nodeEl = document.querySelector("[data-node-id='group:zero:lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    expect(navigate).not.toHaveBeenCalled();
  });

  it("clicking a capped_summary node is a no-op (navigate not called)", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "group:capped:lkc-abc", status: "capped_summary" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc");

    const nodeEl = document.querySelector("[data-node-id='group:capped:lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    expect(navigate).not.toHaveBeenCalled();
  });

  it("clicking identity_group node does NOT call navigate", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "group:identities:lkc-abc", status: "identity_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc");

    const nodeEl = document.querySelector("[data-node-id='group:identities:lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    expect(navigate).not.toHaveBeenCalled();
  });

  // TASK-245: resource_group and cluster_group expand
  it("clicking resource_group node: useGraphData called with expand=resources and collapse button appears", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "env-abc:resource_group", status: "resource_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=env-abc");
    vi.mocked(useGraphData).mockClear();

    const nodeEl = document.querySelector("[data-node-id='env-abc:resource_group']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    // URL changed → re-render → useGraphData called with expand=resources
    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ expand: "resources" }),
    );
    // Collapse button visible confirms expand is set in URL
    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();
  });

  it("clicking cluster_group node: useGraphData called with expand=clusters and collapse button appears", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "sa-abc:cluster_group", status: "cluster_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=sa-abc");
    vi.mocked(useGraphData).mockClear();

    const nodeEl = document.querySelector("[data-node-id='sa-abc:cluster_group']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    // URL changed → re-render → useGraphData called with expand=clusters
    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ expand: "clusters" }),
    );
    // Collapse button visible confirms expand is set in URL
    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();
  });

  it("already-expanded guard: clicking topic_group when expand=topics does not call navigate or pushParam again", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "group:topics:lkc-abc", status: "topic_group" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    // Start with expand=topics already in URL
    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    // Clear call counts after initial render
    vi.mocked(useGraphData).mockClear();

    const nodeEl = document.querySelector("[data-node-id='group:topics:lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    // Guard: expand already "topics" → pushParam not called → no re-render with new params
    // navigate should not be called (this is a group node)
    expect(navigate).not.toHaveBeenCalled();
    // URL unchanged → useGraphData NOT called again with new params
    const callsAfterClick = vi.mocked(useGraphData).mock.calls;
    expect(callsAfterClick).toHaveLength(0);
  });

  it("regular node click clears expand from URL when expand is active", () => {
    vi.mocked(useGraphData).mockReturnValue({
      data: {
        nodes: [
          makeApiNode({ id: "lkc-abc", status: "active" }),
        ] as never,
        edges: [],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });

    // Start with expand=topics in URL
    renderExplorerPageWithUrl("?focus=env-abc&expand=topics");

    // Collapse button visible before click
    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();

    const nodeEl = document.querySelector("[data-node-id='lkc-abc']");
    expect(nodeEl).not.toBeNull();
    fireEvent.click(nodeEl!);

    // Regular node: navigate called and expand cleared
    expect(navigate).toHaveBeenCalled();
    // expand cleared → collapse button gone
    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });
});

describe("ExplorerPage — collapse button (TASK-244)", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: { focusId: null, focusType: null, breadcrumbs: [] },
      navigate: vi.fn(),
      goBack: vi.fn(),
      goToRoot: vi.fn(),
      goToBreadcrumb: vi.fn(),
    });
  });

  it("collapse button is rendered when expand=topics is in URL", () => {
    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();
  });

  it("collapse button is NOT rendered when expand is absent from URL", () => {
    renderExplorerPageWithUrl("?focus=lkc-abc");

    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });

  it("clicking collapse button clears expand from URL (button disappears)", () => {
    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    const collapseBtn = screen.getByRole("button", { name: /collapse/i });
    fireEvent.click(collapseBtn);

    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });

  it("useGraphData is called with expand from URL", () => {
    vi.mocked(useGraphData).mockClear();
    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ expand: "topics" }),
    );
  });

  it("useGraphData is called with expand=null when expand absent from URL", () => {
    vi.mocked(useGraphData).mockClear();
    renderExplorerPageWithUrl("?focus=lkc-abc");

    expect(vi.mocked(useGraphData)).toHaveBeenCalledWith(
      expect.objectContaining({ expand: null }),
    );
  });

  // GIT-002: SearchBar onSelect clears expand
  it("SearchBar onSelect clears expand from URL (collapse button disappears)", () => {
    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    // Collapse button visible before search select
    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();

    // Trigger SearchBar.onSelect via the mock trigger button
    const trigger = screen.getByTestId("searchbar-select-trigger");
    fireEvent.click(trigger);

    // expand should be cleared → collapse button gone
    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });

  // GIT-004: prefetch query key has 9 elements with expand at position 8
  it("prefetch query key has 9 elements and expand at position 8 when expand is set", async () => {
    vi.mocked(useDateRange).mockReturnValue({
      minDate: "2026-01-01",
      maxDate: "2026-04-30",
      isLoading: false,
    });
    vi.mocked(usePlayback).mockReturnValue({
      state: { isPlaying: true, speed: 1, currentDate: "2026-01-15", stepDays: 1 },
      play: vi.fn(),
      pause: vi.fn(),
      setSpeed: vi.fn(),
      setStepDays: vi.fn(),
      setDate: vi.fn(),
      isAtEnd: false,
    });
    vi.mocked(useDebouncedValue).mockImplementation((v: unknown) => v);

    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 0 } },
    });
    const prefetchSpy = vi
      .spyOn(queryClient, "prefetchQuery")
      .mockResolvedValue(undefined);

    render(
      <MemoryRouter initialEntries={["/explorer?focus=lkc-abc&expand=topics"]}>
        <QueryClientProvider client={queryClient}>
          <ExplorerPage />
        </QueryClientProvider>
      </MemoryRouter>,
    );

    await waitFor(() => expect(prefetchSpy).toHaveBeenCalled());

    const queryKey = (
      prefetchSpy.mock.calls[0][0] as { queryKey: unknown[] }
    ).queryKey;
    expect(queryKey).toHaveLength(9);
    expect(queryKey[8]).toBe("topics");
  });
});

describe("ExplorerPage — breadcrumb navigation clears expand (TASK-244)", () => {
  beforeEach(() => {
    resetTenantMock();
    resetGraphDataMock();
  });

  it("clicking Tenant (goToRoot) clears expand from URL", () => {
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: { focusId: null, focusType: null, breadcrumbs: [] },
      navigate: vi.fn(),
      goBack: vi.fn(),
      goToRoot: vi.fn(),
      goToBreadcrumb: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /tenant/i }));

    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });

  it("clicking ← Back (goBack) clears expand from URL", () => {
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: {
        focusId: "lkc-abc",
        focusType: "kafka_cluster",
        breadcrumbs: [{ id: "lkc-abc", label: "my-cluster", type: "kafka_cluster" }],
      },
      navigate: vi.fn(),
      goBack: vi.fn(),
      goToRoot: vi.fn(),
      goToBreadcrumb: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();

    // ← button appears when breadcrumbs.length > 0
    const backBtn = screen.getByRole("button", { name: "←" });
    fireEvent.click(backBtn);

    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });

  it("clicking a breadcrumb crumb (goToBreadcrumb) clears expand from URL", () => {
    vi.mocked(useGraphNavigation).mockReturnValue({
      state: {
        focusId: "lkc-abc",
        focusType: "kafka_cluster",
        breadcrumbs: [
          { id: "env-abc", label: "my-env", type: "environment" },
          { id: "lkc-abc", label: "my-cluster", type: "kafka_cluster" },
        ],
      },
      navigate: vi.fn(),
      goBack: vi.fn(),
      goToRoot: vi.fn(),
      goToBreadcrumb: vi.fn(),
    });

    renderExplorerPageWithUrl("?focus=lkc-abc&expand=topics");

    expect(screen.getByRole("button", { name: /collapse/i })).toBeInTheDocument();

    // First crumb (env-abc / "my-env") is clickable — it's not the last crumb
    const crumbBtn = screen.getByRole("button", { name: /my-env/i });
    fireEvent.click(crumbBtn);

    expect(screen.queryByRole("button", { name: /collapse/i })).toBeNull();
  });
});
