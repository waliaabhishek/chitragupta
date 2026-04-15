import { describe, expect, it } from "vitest";
import { getNodeIcon } from "./nodeIcons";

const KNOWN_TYPES = [
  "tenant",
  "environment",
  "kafka_cluster",
  "dedicated_cluster",
  "kafka_topic",
  "service_account",
  "identity",
  "api_key",
  "connector",
  "flink_compute_pool",
  "schema_registry",
  "ksqldb_cluster",
];

describe("nodeIcons — getNodeIcon", () => {
  it("returns a data URI string for a known type in light mode", () => {
    const result = getNodeIcon("tenant", false);
    expect(result).not.toBeNull();
    expect(result).toMatch(/^data:image\/svg\+xml,/);
  });

  it("returns a data URI string for a known type in dark mode", () => {
    const result = getNodeIcon("kafka_topic", true);
    expect(result).not.toBeNull();
    expect(result).toMatch(/^data:image\/svg\+xml,/);
  });

  it("returns null for an unknown resource type", () => {
    expect(getNodeIcon("unknown_resource", false)).toBeNull();
    expect(getNodeIcon("unknown_resource", true)).toBeNull();
  });

  it("light mode (isDark=false) uses the dark stroke #1a1a2e", () => {
    const result = getNodeIcon("tenant", false);
    expect(result).toContain(encodeURIComponent("#1a1a2e"));
  });

  it("dark mode (isDark=true) uses the light stroke #fff", () => {
    const result = getNodeIcon("tenant", true);
    expect(result).toContain(encodeURIComponent("#fff"));
  });

  it("dark and light mode icons differ for the same resource type", () => {
    const light = getNodeIcon("connector", false);
    const dark = getNodeIcon("connector", true);
    expect(light).not.toBe(dark);
  });

  it("all known resource types return icons in light mode", () => {
    for (const type of KNOWN_TYPES) {
      const result = getNodeIcon(type, false);
      expect(result, `expected icon for ${type}`).not.toBeNull();
      expect(result, `expected data URI for ${type}`).toMatch(/^data:image\/svg\+xml,/);
    }
  });

  it("all known resource types return icons in dark mode", () => {
    for (const type of KNOWN_TYPES) {
      const result = getNodeIcon(type, true);
      expect(result, `expected icon for ${type}`).not.toBeNull();
      expect(result, `expected data URI for ${type}`).toMatch(/^data:image\/svg\+xml,/);
    }
  });
});
