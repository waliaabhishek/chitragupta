import { describe, expect, it } from "vitest";
import {
  costToSize,
  getNodeShape,
  getNodeSize,
  isExpandableGroup,
  isGroupNode,
} from "./nodeShapes";

describe("nodeShapes — getNodeShape (group types)", () => {
  it("returns round-rectangle for topic_group", () => {
    expect(getNodeShape("topic_group")).toBe("round-rectangle");
  });

  it("returns round-rectangle for identity_group", () => {
    expect(getNodeShape("identity_group")).toBe("round-rectangle");
  });

  it("returns round-rectangle for zero_cost_summary", () => {
    expect(getNodeShape("zero_cost_summary")).toBe("round-rectangle");
  });

  it("returns round-rectangle for capped_summary", () => {
    expect(getNodeShape("capped_summary")).toBe("round-rectangle");
  });

  // TASK-245
  it("returns round-rectangle for resource_group", () => {
    expect(getNodeShape("resource_group")).toBe("round-rectangle");
  });

  it("returns round-rectangle for cluster_group", () => {
    expect(getNodeShape("cluster_group")).toBe("round-rectangle");
  });
});

describe("nodeShapes — getNodeSize", () => {
  it("scales group nodes via costToSize like regular nodes", () => {
    expect(getNodeSize("topic_group", 50, 0, 1000)).toBe(costToSize(50, 0, 1000));
    expect(getNodeSize("identity_group", 200, 0, 1000)).toBe(costToSize(200, 0, 1000));
    expect(getNodeSize("zero_cost_summary", 0, 0, 1000)).toBe(costToSize(0, 0, 1000));
    expect(getNodeSize("capped_summary", 0, 0, 1000)).toBe(costToSize(0, 0, 1000));
  });

  it("delegates to costToSize for kafka_topic", () => {
    expect(getNodeSize("kafka_topic", 50, 0, 100)).toBe(costToSize(50, 0, 100));
  });

  it("delegates to costToSize for kafka_cluster", () => {
    expect(getNodeSize("kafka_cluster", 100, 0, 500)).toBe(
      costToSize(100, 0, 500),
    );
  });

  it("delegates to costToSize for environment", () => {
    expect(getNodeSize("environment", 200, 0, 1000)).toBe(
      costToSize(200, 0, 1000),
    );
  });
});

describe("nodeShapes — isGroupNode", () => {
  it("returns true for topic_group", () => {
    expect(isGroupNode("topic_group")).toBe(true);
  });

  it("returns true for identity_group", () => {
    expect(isGroupNode("identity_group")).toBe(true);
  });

  it("returns true for zero_cost_summary", () => {
    expect(isGroupNode("zero_cost_summary")).toBe(true);
  });

  it("returns true for capped_summary", () => {
    expect(isGroupNode("capped_summary")).toBe(true);
  });

  it("returns false for kafka_topic", () => {
    expect(isGroupNode("kafka_topic")).toBe(false);
  });

  it("returns false for kafka_cluster", () => {
    expect(isGroupNode("kafka_cluster")).toBe(false);
  });

  it("returns false for environment", () => {
    expect(isGroupNode("environment")).toBe(false);
  });

  it("returns false for unknown type", () => {
    expect(isGroupNode("unknown_type")).toBe(false);
  });

  // TASK-245
  it("returns true for resource_group", () => {
    expect(isGroupNode("resource_group")).toBe(true);
  });

  it("returns true for cluster_group", () => {
    expect(isGroupNode("cluster_group")).toBe(true);
  });
});

describe("nodeShapes — isExpandableGroup", () => {
  it("returns true for topic_group", () => {
    expect(isExpandableGroup("topic_group")).toBe(true);
  });

  it("returns true for identity_group", () => {
    expect(isExpandableGroup("identity_group")).toBe(true);
  });

  it("returns false for zero_cost_summary", () => {
    expect(isExpandableGroup("zero_cost_summary")).toBe(false);
  });

  it("returns false for capped_summary", () => {
    expect(isExpandableGroup("capped_summary")).toBe(false);
  });

  it("returns false for kafka_topic", () => {
    expect(isExpandableGroup("kafka_topic")).toBe(false);
  });

  it("returns false for kafka_cluster", () => {
    expect(isExpandableGroup("kafka_cluster")).toBe(false);
  });

  it("returns false for unknown type", () => {
    expect(isExpandableGroup("unknown_resource")).toBe(false);
  });

  // TASK-245
  it("returns true for resource_group", () => {
    expect(isExpandableGroup("resource_group")).toBe(true);
  });

  it("returns true for cluster_group", () => {
    expect(isExpandableGroup("cluster_group")).toBe(true);
  });
});
