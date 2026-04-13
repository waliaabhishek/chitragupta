import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { GraphNode } from "./renderers/types";
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
});
