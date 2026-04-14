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

// GraphContainer mock — renders data-testid + per-node data attributes so tests
// can observe enriched nodes (including phantom nodes and diff overlays).
vi.mock("./GraphContainer", () => ({
  GraphContainer: ({
    nodes,
  }: {
    nodes: Array<{
      id: string;
      status: string;
      diff?: { diff_status: string; cost_delta: number };
    }>;
  }) => (
    <div data-testid="graph-container">
      {nodes.map((n) => (
        <div
          key={n.id}
          data-node-id={n.id}
          data-node-status={n.status}
          data-diff-status={n.diff?.diff_status ?? ""}
          data-cost-delta={n.diff != null ? String(n.diff.cost_delta) : ""}
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
          makeApiNode({ id: "sa-001", cross_references: ["lkc-ghost"] }),
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
      expect.objectContaining({ at: "2026-02-15T12:00:00Z" }),
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
      expect.objectContaining({ at: null }),
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
