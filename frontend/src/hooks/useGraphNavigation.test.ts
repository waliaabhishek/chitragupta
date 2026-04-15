import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useGraphNavigation } from "./useGraphNavigation";
import type { GraphNode } from "../components/explorer/renderers/types";

describe("useGraphNavigation", () => {
  it("starts with focusId=null and empty breadcrumbs", () => {
    const { result } = renderHook(() => useGraphNavigation());

    expect(result.current.state.focusId).toBeNull();
    expect(result.current.state.breadcrumbs).toHaveLength(0);
  });

  it("navigate() pushes to breadcrumbs and updates focusId", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
    });

    expect(result.current.state.focusId).toBe("env-abc");
    expect(result.current.state.breadcrumbs).toHaveLength(1);
    expect(result.current.state.breadcrumbs[0].id).toBe("env-abc");
    expect(result.current.state.breadcrumbs[0].label).toBe("my-env");
  });

  it("goBack() pops breadcrumbs and restores previous focusId", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
    });

    act(() => {
      result.current.goBack();
    });

    expect(result.current.state.focusId).toBe("env-abc");
    expect(result.current.state.breadcrumbs).toHaveLength(1);
  });

  it("goBack() at root is a no-op", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.goBack();
    });

    expect(result.current.state.focusId).toBeNull();
    expect(result.current.state.breadcrumbs).toHaveLength(0);
  });

  it("goToRoot() clears breadcrumbs and resets focusId to null", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
    });

    act(() => {
      result.current.goToRoot();
    });

    expect(result.current.state.focusId).toBeNull();
    expect(result.current.state.breadcrumbs).toHaveLength(0);
  });

  it("goToBreadcrumb(i) slices stack to index and restores focusId", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
      result.current.navigate("lkc-abc/topic/orders", "kafka_topic", "orders");
    });

    // breadcrumbs = [env-abc, lkc-abc, orders], current focus = orders
    // goToBreadcrumb(0) → go back to env-abc (slice to index 0 means only [env-abc])
    act(() => {
      result.current.goToBreadcrumb(0);
    });

    expect(result.current.state.focusId).toBe("env-abc");
    expect(result.current.state.breadcrumbs).toHaveLength(1);
  });

  it("goToBreadcrumb with invalid (negative) index is a no-op", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
    });

    const stateBefore = result.current.state;

    act(() => {
      result.current.goToBreadcrumb(-1);
    });

    expect(result.current.state.focusId).toBe(stateBefore.focusId);
    expect(result.current.state.breadcrumbs).toHaveLength(
      stateBefore.breadcrumbs.length,
    );
  });

  it("navigate() with null displayName uses nodeId as label", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("sa-001", "service_account", null);
    });

    expect(result.current.state.breadcrumbs[0].label).toBe("sa-001");
  });

  it("goToBreadcrumb(breadcrumbs.length - 1) is a no-op (current node)", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
    });

    // breadcrumbs = [env-abc, lkc-abc], length = 2
    // goToBreadcrumb(1) = breadcrumbs.length - 1 → no-op
    act(() => {
      result.current.goToBreadcrumb(1);
    });

    expect(result.current.state.focusId).toBe("lkc-abc");
    expect(result.current.state.breadcrumbs).toHaveLength(2);
  });

  it("goBack() from depth-1 stack returns to root (focusId=null)", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
    });

    act(() => {
      result.current.goBack();
    });

    expect(result.current.state.focusId).toBeNull();
    expect(result.current.state.breadcrumbs).toHaveLength(0);
  });

  it("goBack() from depth-2 stack restores depth-1 state", () => {
    const { result } = renderHook(() => useGraphNavigation());

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
    });

    act(() => {
      result.current.goBack();
    });

    expect(result.current.state.focusId).toBe("env-abc");
    expect(result.current.state.focusType).toBe("environment");
    expect(result.current.state.breadcrumbs).toHaveLength(1);
    expect(result.current.state.breadcrumbs[0].id).toBe("env-abc");
  });
});

// ---------------------------------------------------------------------------
// Tests for refactored URL-driven useGraphNavigation (TASK-223)
// These tests require the hook to accept UseGraphNavigationParams and call
// setFocus on navigate/goBack/goToRoot. They FAIL until implementation is done.
// ---------------------------------------------------------------------------

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

describe("useGraphNavigation (URL-driven refactor)", () => {
  it("focusId in returned state equals focusFromUrl param", () => {
    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: "env-abc",
        setFocus,
        currentNodes: null,
      }),
    );

    expect(result.current.state.focusId).toBe("env-abc");
  });

  it("navigate() calls setFocus with the new nodeId", () => {
    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: null,
        setFocus,
        currentNodes: null,
      }),
    );

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
    });

    expect(setFocus).toHaveBeenCalledWith("env-abc");
  });

  it("goBack() calls setFocus with the previous focusId", () => {
    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: null,
        setFocus,
        currentNodes: null,
      }),
    );

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
      result.current.navigate("lkc-abc", "kafka_cluster", "my-cluster");
    });

    setFocus.mockClear();

    act(() => {
      result.current.goBack();
    });

    expect(setFocus).toHaveBeenCalledWith("env-abc");
  });

  it("goToRoot() calls setFocus(null)", () => {
    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: null,
        setFocus,
        currentNodes: null,
      }),
    );

    act(() => {
      result.current.navigate("env-abc", "environment", "my-env");
    });

    setFocus.mockClear();

    act(() => {
      result.current.goToRoot();
    });

    expect(setFocus).toHaveBeenCalledWith(null);
  });

  it("breadcrumb reconstruction fires when focusFromUrl set and breadcrumbs empty and nodes loaded", async () => {
    const parentNode = makeNode({ id: "env-abc", resource_type: "environment", display_name: "my-env", parent_id: null });
    const focusedNode = makeNode({ id: "lkc-abc", resource_type: "kafka_cluster", display_name: "my-cluster", parent_id: "env-abc" });

    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: "lkc-abc",
        setFocus,
        currentNodes: [parentNode, focusedNode],
      }),
    );

    // After mount with nodes loaded, breadcrumb chain should be reconstructed
    // Expected: [env-abc (parent), lkc-abc (focused)]
    await new Promise((r) => setTimeout(r, 50));
    expect(result.current.state.breadcrumbs.length).toBeGreaterThan(0);
    expect(
      result.current.state.breadcrumbs.some((b) => b.id === "lkc-abc"),
    ).toBe(true);
  });

  it("breadcrumb reconstruction handles missing parent gracefully (no crash)", async () => {
    const focusedNode = makeNode({
      id: "lkc-abc",
      resource_type: "kafka_cluster",
      display_name: "my-cluster",
      parent_id: "env-missing",
    });

    const setFocus = vi.fn();
    const { result } = renderHook(() =>
      useGraphNavigation({
        focusFromUrl: "lkc-abc",
        setFocus,
        currentNodes: [focusedNode],
      }),
    );

    await new Promise((r) => setTimeout(r, 50));
    // Graceful degradation — only focused node in breadcrumbs, no crash
    expect(result.current.state.breadcrumbs).toBeDefined();
    expect(
      result.current.state.breadcrumbs.some((b) => b.id === "lkc-abc"),
    ).toBe(true);
  });
});
