import { render, act, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import type { GraphNode, GraphEdge } from "./types";

// vi.mock() calls are hoisted before const declarations — use vi.hoisted() so the
// mock factory can reference cytoscapeMock/mockCyInstance without a TDZ ReferenceError.
const { cytoscapeMock, mockCyInstance, state } = vi.hoisted(() => {
  // Shared mutable state — reset in beforeEach between tests.
  const state = {
    onHandlers: {} as Record<string, (evt: { target: { id: () => string; data: (k: string) => string } }) => void>,
    addCalls: [] as unknown[],
    destroyCalled: false,
    fadedClassApplied: false,
  };

  const mockCyInstance = {
    on: vi.fn(
      (
        event: string,
        selector: string,
        handler: (evt: { target: { id: () => string; data: (k: string) => string } }) => void,
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
    const nodes = [makeNode({ id: "n1" }), makeNode({ id: "n2" }), makeNode({ id: "n3" })];
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
    render(<CytoscapeRenderer {...DEFAULT_PROPS} nodes={nodes} edges={edges} />);
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
    const clusterNode = makeNode({ id: "lkc-abc", resource_type: "kafka_cluster" });
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
});
