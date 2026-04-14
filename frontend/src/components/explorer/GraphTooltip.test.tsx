import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { DiffOverlay, GraphNode, GraphNodeWithDiff } from "./renderers/types";
import { GraphTooltip } from "./GraphTooltip";

function makeNode(overrides: Partial<GraphNode> = {}): GraphNode {
  return {
    id: "env-abc",
    resource_type: "environment",
    display_name: "my-env",
    cost: 100.5,
    created_at: null,
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

describe("GraphTooltip", () => {
  it("renders nothing when hoveredNodeId is null", () => {
    const { container } = render(
      <GraphTooltip hoveredNodeId={null} nodes={[makeNode()]} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("renders nothing when hoveredNodeId is not found in nodes", () => {
    const { container } = render(
      <GraphTooltip
        hoveredNodeId="nonexistent-id"
        nodes={[makeNode({ id: "env-abc" })]}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("renders entity display_name when hoveredNode is found", () => {
    const { getByText } = render(
      <GraphTooltip
        hoveredNodeId="env-abc"
        nodes={[makeNode({ display_name: "my-env" })]}
      />,
    );
    expect(getByText("my-env")).toBeInTheDocument();
  });

  it("renders entity id as fallback when display_name is null", () => {
    const { getByText } = render(
      <GraphTooltip
        hoveredNodeId="env-abc"
        nodes={[makeNode({ id: "env-abc", display_name: null })]}
      />,
    );
    expect(getByText("env-abc")).toBeInTheDocument();
  });

  it("renders resource_type", () => {
    const { getByText } = render(
      <GraphTooltip
        hoveredNodeId="env-abc"
        nodes={[makeNode({ resource_type: "kafka_cluster" })]}
      />,
    );
    expect(getByText("kafka_cluster")).toBeInTheDocument();
  });

  it("formats cost as USD currency", () => {
    const { getByText } = render(
      <GraphTooltip
        hoveredNodeId="env-abc"
        nodes={[makeNode({ cost: 1234.56 })]}
      />,
    );
    expect(getByText(/Cost: \$1,234\.56/)).toBeInTheDocument();
  });

  it("renders tag key-value pairs when tags are present", () => {
    const { getByText } = render(
      <GraphTooltip
        hoveredNodeId="env-abc"
        nodes={[makeNode({ tags: { env: "prod", team: "platform" } })]}
      />,
    );
    expect(getByText("env: prod")).toBeInTheDocument();
    expect(getByText("team: platform")).toBeInTheDocument();
  });

  // GIT-R2-004: diff overlay rendering
  it("renders Before/After/Delta section when node has diff", () => {
    const diff: DiffOverlay = {
      cost_before: 100,
      cost_after: 150,
      cost_delta: 50,
      pct_change: 50,
      diff_status: "changed",
    };
    const node: GraphNodeWithDiff = { ...makeNode(), diff };
    const { getByText } = render(
      <GraphTooltip hoveredNodeId="env-abc" nodes={[node]} />,
    );
    expect(getByText(/Before:/)).toBeInTheDocument();
    expect(getByText(/After:/)).toBeInTheDocument();
    expect(getByText(/Delta:/)).toBeInTheDocument();
  });

  it("Delta text is colored red (#ff7875) when cost_delta > 0", () => {
    const diff: DiffOverlay = {
      cost_before: 100,
      cost_after: 200,
      cost_delta: 100,
      pct_change: 100,
      diff_status: "changed",
    };
    const node: GraphNodeWithDiff = { ...makeNode(), diff };
    const { container } = render(
      <GraphTooltip hoveredNodeId="env-abc" nodes={[node]} />,
    );
    // Find the innermost div whose textContent starts with "Delta:" — that's
    // the one with the inline color style, not a parent wrapper.
    const deltaEl = Array.from(container.querySelectorAll("div")).find((el) =>
      el.textContent?.trim().startsWith("Delta:"),
    );
    expect(deltaEl).not.toBeUndefined();
    // jsdom normalises hex to rgb when reading back from .style.color
    expect(deltaEl!.style.color).toBe("rgb(255, 120, 117)");
  });

  it("Delta text is colored green (#95de64) when cost_delta < 0", () => {
    const diff: DiffOverlay = {
      cost_before: 200,
      cost_after: 100,
      cost_delta: -100,
      pct_change: -50,
      diff_status: "changed",
    };
    const node: GraphNodeWithDiff = { ...makeNode(), diff };
    const { container } = render(
      <GraphTooltip hoveredNodeId="env-abc" nodes={[node]} />,
    );
    const deltaEl = Array.from(container.querySelectorAll("div")).find((el) =>
      el.textContent?.trim().startsWith("Delta:"),
    );
    expect(deltaEl).not.toBeUndefined();
    expect(deltaEl!.style.color).toBe("rgb(149, 222, 100)");
  });

  it("shows '(New)' in delta line when pct_change is null", () => {
    const diff: DiffOverlay = {
      cost_before: 0,
      cost_after: 75,
      cost_delta: 75,
      pct_change: null,
      diff_status: "new",
    };
    const node: GraphNodeWithDiff = { ...makeNode(), diff };
    const { getByText } = render(
      <GraphTooltip hoveredNodeId="env-abc" nodes={[node]} />,
    );
    expect(getByText(/\(New\)/)).toBeInTheDocument();
  });

  it("does not render diff section when node.diff is undefined", () => {
    const node: GraphNodeWithDiff = makeNode();
    const { queryByText } = render(
      <GraphTooltip hoveredNodeId="env-abc" nodes={[node]} />,
    );
    expect(queryByText(/Before:/)).toBeNull();
    expect(queryByText(/After:/)).toBeNull();
    expect(queryByText(/Delta:/)).toBeNull();
  });
});
