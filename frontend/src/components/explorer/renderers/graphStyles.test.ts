import { describe, expect, it } from "vitest";
import { getStylesheet } from "./graphStyles";

const GROUP_SELECTOR =
  'node[resource_type = "topic_group"], node[resource_type = "identity_group"], node[resource_type = "resource_group"], node[resource_type = "cluster_group"]';

function selectorsOf(stylesheet: ReturnType<typeof getStylesheet>): string[] {
  return stylesheet.map((entry) => entry.selector as string);
}

describe("graphStyles — getStylesheet", () => {
  describe("dark mode (isDark=true)", () => {
    const sheet = getStylesheet(true);
    const selectors = selectorsOf(sheet);

    it("returns an array", () => {
      expect(Array.isArray(sheet)).toBe(true);
      expect(sheet.length).toBeGreaterThan(0);
    });

    it("includes dark-mode node colors", () => {
      const nodeEntries = sheet.filter((e) => e.selector === "node");
      const styles = nodeEntries.map((e) => e.style);
      expect(styles.some((s) => (s as Record<string, unknown>)["color"] === "#e0e0e0")).toBe(true);
      expect(
        styles.some(
          (s) => (s as Record<string, unknown>)["background-color"] === "#3a3a5c",
        ),
      ).toBe(true);
    });

    it("includes base selectors: node, edge, phantom, faded", () => {
      expect(selectors).toContain("node");
      expect(selectors).toContain("edge");
      expect(selectors).toContain('node[status = "phantom"]');
      expect(selectors).toContain("node.faded");
      expect(selectors).toContain("edge.faded");
    });

    it("includes diff class selectors", () => {
      expect(selectors).toContain("node.diff-increase");
      expect(selectors).toContain("node.diff-decrease");
      expect(selectors).toContain("node.diff-new");
      expect(selectors).toContain("node.diff-deleted");
    });

    it("includes tagColor selector", () => {
      expect(selectors).toContain("node[tagColor]");
    });

    it("includes group node selector", () => {
      expect(selectors).toContain(GROUP_SELECTOR);
    });
  });

  describe("light mode (isDark=false)", () => {
    const sheet = getStylesheet(false);
    const selectors = selectorsOf(sheet);

    it("returns an array", () => {
      expect(Array.isArray(sheet)).toBe(true);
      expect(sheet.length).toBeGreaterThan(0);
    });

    it("includes light-mode node colors", () => {
      const nodeEntries = sheet.filter((e) => e.selector === "node");
      const styles = nodeEntries.map((e) => e.style);
      expect(styles.some((s) => (s as Record<string, unknown>)["color"] === "#262626")).toBe(true);
      expect(
        styles.some(
          (s) => (s as Record<string, unknown>)["background-color"] === "#e6f4ff",
        ),
      ).toBe(true);
    });

    it("includes base selectors: node, edge, phantom, faded", () => {
      expect(selectors).toContain("node");
      expect(selectors).toContain("edge");
      expect(selectors).toContain('node[status = "phantom"]');
      expect(selectors).toContain("node.faded");
      expect(selectors).toContain("edge.faded");
    });

    it("includes diff class selectors", () => {
      expect(selectors).toContain("node.diff-increase");
      expect(selectors).toContain("node.diff-decrease");
      expect(selectors).toContain("node.diff-new");
      expect(selectors).toContain("node.diff-deleted");
    });

    it("includes tagColor selector", () => {
      expect(selectors).toContain("node[tagColor]");
    });

    it("includes group node selector", () => {
      expect(selectors).toContain(GROUP_SELECTOR);
    });
  });

  it("dark and light stylesheets are distinct", () => {
    const dark = getStylesheet(true);
    const light = getStylesheet(false);
    expect(JSON.stringify(dark)).not.toBe(JSON.stringify(light));
  });
});
