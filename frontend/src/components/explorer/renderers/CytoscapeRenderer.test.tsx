import { render, act, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import type { GraphNode, GraphEdge } from "./types";

// vi.mock() calls are hoisted before const declarations — use vi.hoisted() so the
// mock factory can reference cytoscapeMock/mockCyInstance without a TDZ ReferenceError.
const { cytoscapeMock, mockCyInstance, state } = vi.hoisted(() => {
  // Shared mutable state — reset in beforeEach between tests.
  const state = {
    onHandlers: {} as Record<
      string,
      (evt: {
        target: { id: () => string; data: (k: string) => string };
      }) => void
    >,
    addCalls: [] as unknown[],
    destroyCalled: false,
    fadedClassApplied: false,
  };

  const mockCyInstance = {
    on: vi.fn(
      (
        event: string,
        selector: string,
        handler: (evt: {
          target: { id: () => string; data: (k: string) => string };
        }) => void,
      ) => {
        state.onHandlers[`${event}:${selector}`] = handler;
      },
    ),
    nodes: vi.fn(() => ({
      map: (fn: (n: { id: () => string }) => string) => [].map(fn),
      filter: vi.fn(() => ({
        animate: vi.fn(),
        addClass: vi.fn((cls: string) => {
          if (cls === "faded") state.fadedClassApplied = true;
        }),
        removeClass: vi.fn(),
      })),
      addClass: vi.fn(),
      removeClass: vi.fn(),
    })),
    getElementById: vi.fn(() => ({ data: vi.fn() })),
    add: vi.fn((el: unknown) => {
      state.addCalls.push(el);
    }),
    layout: vi.fn(() => ({ run: vi.fn() })),
    destroy: vi.fn(() => {
      state.destroyCalled = true;
    }),
    style: vi.fn(() => ({ fromJson: vi.fn(() => ({ update: vi.fn() })) })),
  };

  const cytoscapeMock = vi.fn(() => mockCyInstance);
  (cytoscapeMock as unknown as { use: ReturnType<typeof vi.fn> }).use = vi.fn();

  return { cytoscapeMock, mockCyInstance, state };
});

vi.mock("cytoscape", () => ({ default: cytoscapeMock }));
vi.mock("cytoscape-cose-bilkent", () => ({ default: vi.fn() }));

import { CytoscapeRenderer } from "./CytoscapeRenderer";

function makeNode(overrides: Partial<GraphNode> = {}): GraphNode {
  return {
    id: "node-1",
    resource_type: "environment",
    display_name: "my-env",
    cost: 100,
    created_at: "2026-01-01T00:00:00Z",
    deleted_at: null,
    tags: {},
    parent_id: null,
    cloud: null,
    region: null,
    status: "active",
    cross_references: [],
    ...overrides,
  };
}

function makeEdge(overrides: Partial<GraphEdge> = {}): GraphEdge {
  return {
    source: "node-1",
    target: "node-2",
    relationship_type: "parent",
    cost: null,
    ...overrides,
  };
}

const DEFAULT_PROPS = {
  nodes: [] as GraphNode[],
  edges: [] as GraphEdge[],
  focusId: null as string | null,
  fadedNodeIds: new Set<string>(),
  onNodeClick: vi.fn(),
  onNodeHover: vi.fn(),
  isLoading: false,
  isDark: false,
  width: 800,
  height: 600,
  activeTagKey: null as string | null,
  tagSelectedValue: null as string | null,
};

describe("CytoscapeRenderer", () => {
  beforeEach(() => {
    state.onHandlers = {};
    state.addCalls = [];
    state.destroyCalled = false;
    state.fadedClassApplied = false;
    cytoscapeMock.mockClear();
    mockCyInstance.on.mockClear();
    mockCyInstance.add.mockClear();
    mockCyInstance.destroy.mockClear();
    mockCyInstance.layout.mockClear();
  });

  it("initializes Cytoscape instance on mount", async () => {
    render(<CytoscapeRenderer {...DEFAULT_PROPS} />);
    await waitFor(() => {
      expect(cytoscapeMock).toHaveBeenCalledTimes(1);
    });
  });

  it("renders correct number of nodes from props", async () => {
    const nodes = [
      makeNode({ id: "n1" }),
      makeNode({ id: "n2" }),
      makeNode({ id: "n3" }),
    ];
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={nodes} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(3);
    });
  });

  it("renders correct number of edges from props", async () => {
    const nodes = [makeNode({ id: "n1" }), makeNode({ id: "n2" })];
    const edges = [makeEdge({ source: "n1", target: "n2" })];
    render(
      <CytoscapeRenderer {...DEFAULT_PROPS} nodes={nodes} edges={edges} />,
    );
    await waitFor(() => {
      const edgeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "edges",
      );
      expect(edgeAdds).toHaveLength(1);
    });
  });

  it("fires onNodeClick callback on node tap", async () => {
    const onNodeClick = vi.fn();
    render(<CytoscapeRenderer {...DEFAULT_PROPS} onNodeClick={onNodeClick} />);

    await waitFor(() => {
      expect(state.onHandlers["tap:node"]).toBeDefined();
    });

    state.onHandlers["tap:node"]({
      target: {
        id: () => "env-abc",
        data: (k: string) => (k === "resource_type" ? "environment" : ""),
      },
    });
    expect(onNodeClick).toHaveBeenCalledWith("env-abc", "environment");
  });

  it("destroys Cytoscape instance on unmount", async () => {
    const { unmount } = render(<CytoscapeRenderer {...DEFAULT_PROPS} />);
    await waitFor(() => {
      expect(cytoscapeMock).toHaveBeenCalledTimes(1);
    });
    act(() => {
      unmount();
    });
    expect(state.destroyCalled).toBe(true);
  });

  it("applies correct shape per resource_type", async () => {
    const clusterNode = makeNode({
      id: "lkc-abc",
      resource_type: "kafka_cluster",
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[clusterNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds.length).toBeGreaterThan(0);
      const addedData = (nodeAdds[0] as { data: { shape: string } }).data;
      expect(addedData.shape).toBe("ellipse");
    });
  });

  it("applies faded class to nodes in fadedNodeIds", async () => {
    const node = makeNode({ id: "n1" });
    render(
      <CytoscapeRenderer
        {...DEFAULT_PROPS}
        nodes={[node]}
        fadedNodeIds={new Set(["n1"])}
      />,
    );
    await waitFor(() => {
      expect(state.fadedClassApplied).toBe(true);
    });
  });

  it("handles empty nodes and edges without error", () => {
    expect(() =>
      render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[]} edges={[]} />),
    ).not.toThrow();
  });

  // TASK-244: group node size and label
  it("topic_group node uses fixed size 100 instead of cost-scaled size", async () => {
    const groupNode = makeNode({
      id: "group:topics:lkc-abc",
      resource_type: "topic_group",
      cost: 0,
      child_count: 42,
      child_total_cost: 1234.56,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { size: number } }).data;
      expect(addedData.size).toBe(100);
    });
  });

  it("topic_group node has label with child_count and child_total_cost", async () => {
    const groupNode = makeNode({
      id: "group:topics:lkc-abc",
      resource_type: "topic_group",
      cost: 0,
      child_count: 42,
      child_total_cost: 1234.56,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("42 topics\n$1234.56 total");
    });
  });

  it("zero_cost_summary node has label N others at $0", async () => {
    const summaryNode = makeNode({
      id: "group:zero:lkc-abc",
      resource_type: "zero_cost_summary",
      cost: 0,
      child_count: 5,
      child_total_cost: 0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[summaryNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("5 others at $0");
    });
  });

  it("capped_summary node has label N more (capped)", async () => {
    const cappedNode = makeNode({
      id: "group:capped:lkc-abc",
      resource_type: "capped_summary",
      cost: 0,
      child_count: 10,
      child_total_cost: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[cappedNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("10 more (capped)");
    });
  });

  // TASK-245: resource_group and cluster_group labels
  it("resource_group node has label with child_count and child_total_cost", async () => {
    const groupNode = makeNode({
      id: "env-abc:resource_group",
      resource_type: "resource_group",
      cost: 0,
      child_count: 25,
      child_total_cost: 500.0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("25 resources\n$500.00 total");
    });
  });

  it("cluster_group node has label with child_count and child_total_cost", async () => {
    const groupNode = makeNode({
      id: "sa-abc:cluster_group",
      resource_type: "cluster_group",
      cost: 0,
      child_count: 12,
      child_total_cost: 99.5,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("12 clusters\n$99.50 total");
    });
  });

  it("identity_group node has label with child_count and child_total_cost", async () => {
    const groupNode = makeNode({
      id: "group:identities:lkc-abc",
      resource_type: "identity_group",
      cost: 0,
      child_count: 7,
      child_total_cost: 89.5,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("7 users\n$89.50 total");
    });
  });

  // ---------------------------------------------------------------------------
  // GIT-003 — Constrained re-layout on tag value selection
  // ---------------------------------------------------------------------------

  describe("constrained layout — tag selection (GIT-003)", () => {
    it("layout effect skips on initial mount (isFirstTagEffect guard)", async () => {
      const nodes = [makeNode({ id: "n1" })];
      render(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));

      // Tag effect skips on first render — no "cose" call should exist
      const coseCalls = (mockCyInstance.layout.mock.calls as unknown[][]).filter(
        (args) => (args[0] as { name: string }).name === "cose",
      );
      expect(coseCalls).toHaveLength(0);
    });

    it("fires 'cose' layout when tagSelectedValue changes to a string", async () => {
      const nodes = [makeNode({ id: "n1" })];
      const { rerender } = render(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue={null}
        />,
      );

      // Wait for initial mount effects to settle (clears isFirstTagEffect guard)
      await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));
      mockCyInstance.layout.mockClear();

      // Change tagSelectedValue → triggers tag effect with cose
      rerender(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => {
        const coseCalls = (mockCyInstance.layout.mock.calls as unknown[][]).filter(
          (args) => (args[0] as { name: string }).name === "cose",
        );
        expect(coseCalls).toHaveLength(1);
      });
    });

    it("fires 'cose-bilkent' layout when tagSelectedValue changes to null (deselect/restore)", async () => {
      const nodes = [makeNode({ id: "n1" })];
      const { rerender } = render(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));
      mockCyInstance.layout.mockClear();

      // Deselect tag value → should restore standard layout
      rerender(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue={null}
        />,
      );

      await waitFor(() => {
        const bilkentCalls = (mockCyInstance.layout.mock.calls as unknown[][]).filter(
          (args) => (args[0] as { name: string }).name === "cose-bilkent",
        );
        expect(bilkentCalls.length).toBeGreaterThanOrEqual(1);
      });
    });

    it("idealEdgeLength returns 50 for matching, 120 for mixed, 200 for non-matching", async () => {
      const nodes = [makeNode({ id: "n1" })];
      const { rerender } = render(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue={null}
        />,
      );

      await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));
      mockCyInstance.layout.mockClear();

      // Trigger cose layout to capture the idealEdgeLength function
      rerender(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => {
        const coseCalls = (mockCyInstance.layout.mock.calls as unknown[][]).filter(
          (args) => (args[0] as { name: string }).name === "cose",
        );
        expect(coseCalls).toHaveLength(1);
      });

      const coseArgs = (mockCyInstance.layout.mock.calls as unknown[][]).find(
        (args) => (args[0] as { name: string }).name === "cose",
      )!;
      const idealEdgeLength = (
        coseArgs[0] as { idealEdgeLength: (edge: unknown) => number }
      ).idealEdgeLength;

      function mockEdge(srcTags: Record<string, string>, tgtTags: Record<string, string>) {
        return {
          source: () => ({ data: (k: string) => (k === "tags" ? srcTags : undefined) }),
          target: () => ({ data: (k: string) => (k === "tags" ? tgtTags : undefined) }),
        };
      }

      // Both match "platform" → pull together (50)
      expect(idealEdgeLength(mockEdge({ team: "platform" }, { team: "platform" }))).toBe(50);
      // Neither matches → push apart (200)
      expect(idealEdgeLength(mockEdge({ team: "data" }, { team: "infra" }))).toBe(200);
      // Mixed (one matches, one doesn't) → standard (120)
      expect(idealEdgeLength(mockEdge({ team: "platform" }, { team: "data" }))).toBe(120);
    });
  });
});
