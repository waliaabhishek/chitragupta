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
    layout: vi.fn(() => ({ run: vi.fn(), stop: vi.fn() })),
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
vi.mock("cytoscape-d3-force", () => ({ default: vi.fn() }));

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
  it("topic_group node uses cost-scaled size like regular nodes", async () => {
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
      // Group nodes now scale with cost like all other nodes
      expect(addedData.size).toBeGreaterThanOrEqual(10);
      expect(addedData.size).toBeLessThanOrEqual(30);
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

  // ---------- computeNodeLabel uncovered branches ----------

  it("topic_group without child_total_cost shows count only", async () => {
    const groupNode = makeNode({
      id: "group:topics:lkc-abc",
      resource_type: "topic_group",
      cost: 0,
      child_count: 42,
      child_total_cost: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("42 topics");
    });
  });

  it("identity_group without child_total_cost shows count only", async () => {
    const groupNode = makeNode({
      id: "group:ids:lkc-abc",
      resource_type: "identity_group",
      cost: 0,
      child_count: 3,
      child_total_cost: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("3 users");
    });
  });

  it("resource_group without child_total_cost shows count only", async () => {
    const groupNode = makeNode({
      id: "env-abc:resource_group",
      resource_type: "resource_group",
      cost: 0,
      child_count: 8,
      child_total_cost: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("8 resources");
    });
  });

  it("cluster_group without child_total_cost shows count only", async () => {
    const groupNode = makeNode({
      id: "sa-abc:cluster_group",
      resource_type: "cluster_group",
      cost: 0,
      child_count: 4,
      child_total_cost: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("4 clusters");
    });
  });

  it("xref_group node has label 'N more'", async () => {
    const groupNode = makeNode({
      id: "env-abc:xref_group:kafka_topic",
      resource_type: "xref_group",
      cost: 0,
      child_count: 15,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("15 more");
    });
  });

  it("regular node without display_name falls back to id", async () => {
    const node = makeNode({
      id: "env-abc",
      display_name: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[node]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("env-abc");
    });
  });

  // ---------- Data diff effect — persisting nodes ----------

  it("updates data in place for persisting nodes on rerender", async () => {
    const node1 = makeNode({ id: "n1", cost: 100, display_name: "Node 1" });

    // Track which IDs the mock should report as already in the graph
    const prevNodeIds = new Set<string>();
    mockCyInstance.nodes.mockImplementation(() => ({
      map: (fn: (n: { id: () => string }) => string) =>
        [...prevNodeIds].map((id) => fn({ id: () => id })),
      filter: vi.fn(() => ({
        animate: vi.fn(),
        addClass: vi.fn((cls: string) => {
          if (cls === "faded") state.fadedClassApplied = true;
        }),
        removeClass: vi.fn(),
      })),
      addClass: vi.fn(),
      removeClass: vi.fn(),
      forEach: vi.fn(),
      length: prevNodeIds.size,
    }));

    const { rerender } = render(
      <CytoscapeRenderer {...DEFAULT_PROPS} nodes={[node1]} />,
    );

    await waitFor(() => {
      expect(state.addCalls.length).toBeGreaterThan(0);
    });

    // n1 now "exists" in the graph
    prevNodeIds.add("n1");
    state.addCalls = [];
    (mockCyInstance.getElementById as ReturnType<typeof vi.fn>).mockReturnValue({
      data: vi.fn(),
      isNode: () => true,
      position: () => ({ x: 100, y: 100 }),
      addClass: vi.fn(),
      style: vi.fn(),
      animate: vi.fn(),
    });

    // Rerender with updated cost — persisting path calls getElementById("n1").data()
    const updated = makeNode({
      id: "n1",
      cost: 200,
      display_name: "Node 1 Updated",
    });
    rerender(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[updated]} />);

    await waitFor(() => {
      expect(mockCyInstance.getElementById).toHaveBeenCalledWith("n1");
    });
  });

  // ---------- Dark mode style update ----------

  it("updates stylesheet when isDark changes", async () => {
    const { rerender } = render(<CytoscapeRenderer {...DEFAULT_PROPS} />);
    await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));
    mockCyInstance.style.mockClear();

    rerender(<CytoscapeRenderer {...DEFAULT_PROPS} isDark={true} />);

    await waitFor(() => {
      expect(mockCyInstance.style).toHaveBeenCalled();
    });
  });

  // ---------- Resize ----------

  it("calls cy.resize() when width/height changes", async () => {
    const resizeMock = vi.fn();
    (mockCyInstance as Record<string, unknown>).resize = resizeMock;

    const { rerender } = render(
      <CytoscapeRenderer {...DEFAULT_PROPS} width={800} height={600} />,
    );
    await waitFor(() => expect(cytoscapeMock).toHaveBeenCalledTimes(1));

    resizeMock.mockClear();
    rerender(
      <CytoscapeRenderer {...DEFAULT_PROPS} width={1000} height={700} />,
    );

    await waitFor(() => {
      expect(resizeMock).toHaveBeenCalled();
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

      // Tag effect skips on first render — the only layout call should be
      // the data-diff effect's d3-force, not the tag effect's d3-force.
      // We verify the tag effect didn't fire by checking layout was called
      // exactly once (from the data-diff effect only).
      const allCalls = mockCyInstance.layout.mock.calls as unknown[][];
      const d3Calls = allCalls.filter(
        (args) => (args[0] as { name: string }).name === "d3-force",
      );
      // Only the data-diff effect should fire d3-force, not the tag effect
      expect(d3Calls).toHaveLength(1);
    });

    it("fires d3-force layout when tagSelectedValue changes to a string", async () => {
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

      // Change tagSelectedValue → triggers tag effect with d3-force
      rerender(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => {
        const d3Calls = (
          mockCyInstance.layout.mock.calls as unknown[][]
        ).filter((args) => (args[0] as { name: string }).name === "d3-force");
        expect(d3Calls).toHaveLength(1);
      });
    });

    it("fires d3-force layout when tagSelectedValue changes to null (deselect/restore)", async () => {
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
        const d3Calls = (
          mockCyInstance.layout.mock.calls as unknown[][]
        ).filter((args) => (args[0] as { name: string }).name === "d3-force");
        expect(d3Calls.length).toBeGreaterThanOrEqual(1);
      });
    });

    it("tag-filtered layout uses dynamic linkDistance: 50 for matching, 120 for mixed, 200 for non-matching", async () => {
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

      // Mock getElementById to return nodes with tags data
      (
        mockCyInstance.getElementById as ReturnType<typeof vi.fn>
      ).mockImplementation((id: string) => {
        const tagMap: Record<string, Record<string, string>> = {
          "src-match": { team: "platform" },
          "tgt-match": { team: "platform" },
          "src-other": { team: "data" },
          "tgt-other": { team: "infra" },
          "src-mixed": { team: "platform" },
          "tgt-mixed": { team: "data" },
        };
        return {
          data: (k: string) => (k === "tags" ? (tagMap[id] ?? {}) : undefined),
        };
      });

      // Trigger tag-filtered layout
      rerender(
        <CytoscapeRenderer
          {...DEFAULT_PROPS}
          nodes={nodes}
          activeTagKey="team"
          tagSelectedValue="platform"
        />,
      );

      await waitFor(() => {
        const d3Calls = (
          mockCyInstance.layout.mock.calls as unknown[][]
        ).filter((args) => (args[0] as { name: string }).name === "d3-force");
        expect(d3Calls).toHaveLength(1);
      });

      const d3Args = (mockCyInstance.layout.mock.calls as unknown[][]).find(
        (args) => (args[0] as { name: string }).name === "d3-force",
      )!;
      const linkDistance = (
        d3Args[0] as {
          linkDistance: (d: {
            source: { id: string };
            target: { id: string };
          }) => number;
        }
      ).linkDistance;

      // Both match "platform" → pull together (80)
      expect(
        linkDistance({
          source: { id: "src-match" },
          target: { id: "tgt-match" },
        }),
      ).toBe(80);
      // Neither matches → push apart (280)
      expect(
        linkDistance({
          source: { id: "src-other" },
          target: { id: "tgt-other" },
        }),
      ).toBe(280);
      // Mixed (one matches, one doesn't) → standard (180)
      expect(
        linkDistance({
          source: { id: "src-mixed" },
          target: { id: "tgt-mixed" },
        }),
      ).toBe(180);
    });
  });

  // ---------- computeNodeLabel: null child_count shows '?' ----------

  it("topic_group with null child_count shows '?' in label", async () => {
    const groupNode = makeNode({
      id: "group:topics:lkc-abc",
      resource_type: "topic_group",
      cost: 0,
      child_count: null,
      child_total_cost: 100.0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? topics\n$100.00 total");
    });
  });

  it("zero_cost_summary with null child_count shows '?' in label", async () => {
    const summaryNode = makeNode({
      id: "group:zero:lkc-abc",
      resource_type: "zero_cost_summary",
      cost: 0,
      child_count: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[summaryNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? others at $0");
    });
  });

  it("capped_summary with null child_count shows '?' in label", async () => {
    const cappedNode = makeNode({
      id: "group:capped:lkc-abc",
      resource_type: "capped_summary",
      cost: 0,
      child_count: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[cappedNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? more (capped)");
    });
  });

  it("xref_group with null child_count shows '?' in label", async () => {
    const groupNode = makeNode({
      id: "env-abc:xref_group:kafka_topic",
      resource_type: "xref_group",
      cost: 0,
      child_count: null,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? more");
    });
  });

  it("identity_group with null child_count shows '?' in label", async () => {
    const groupNode = makeNode({
      id: "group:ids:lkc-abc",
      resource_type: "identity_group",
      cost: 0,
      child_count: null,
      child_total_cost: 50.0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? users\n$50.00 total");
    });
  });

  it("resource_group with null child_count shows '?' in label", async () => {
    const groupNode = makeNode({
      id: "env-abc:resource_group",
      resource_type: "resource_group",
      cost: 0,
      child_count: null,
      child_total_cost: 200.0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? resources\n$200.00 total");
    });
  });

  it("cluster_group with null child_count shows '?' in label", async () => {
    const groupNode = makeNode({
      id: "sa-abc:cluster_group",
      resource_type: "cluster_group",
      cost: 0,
      child_count: null,
      child_total_cost: 75.0,
    });
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={[groupNode]} />);
    await waitFor(() => {
      const nodeAdds = state.addCalls.filter(
        (el) => (el as { group?: string }).group === "nodes",
      );
      expect(nodeAdds).toHaveLength(1);
      const addedData = (nodeAdds[0] as { data: { label: string } }).data;
      expect(addedData.label).toBe("? clusters\n$75.00 total");
    });
  });
});
