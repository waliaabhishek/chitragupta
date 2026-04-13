import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useGraphNavigation } from "./useGraphNavigation";

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
