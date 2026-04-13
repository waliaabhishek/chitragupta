import { act, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { GraphEdge, GraphNode } from "./renderers/types";

vi.mock("./renderers/CytoscapeRenderer", () => ({
  CytoscapeRenderer: vi.fn(
    ({ width, height }: { width: number; height: number }) => (
      <div
        data-testid="cytoscape-renderer"
        data-width={String(width)}
        data-height={String(height)}
      />
    ),
  ),
}));

// Import after mock to capture the mocked module
import { GraphContainer } from "./GraphContainer";

const DEFAULT_PROPS = {
  nodes: [] as GraphNode[],
  edges: [] as GraphEdge[],
  focusId: null as string | null,
  fadedNodeIds: new Set<string>(),
  onNodeClick: vi.fn(),
  onNodeHover: vi.fn(),
  isLoading: false,
  isDark: false,
};

describe("GraphContainer", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("renders the outer container div", () => {
    render(<GraphContainer {...DEFAULT_PROPS} />);
    expect(
      document.querySelector("[data-testid='graph-container']"),
    ).not.toBeNull();
  });

  it("renders CytoscapeRenderer as child", () => {
    render(<GraphContainer {...DEFAULT_PROPS} />);
    expect(
      document.querySelector("[data-testid='cytoscape-renderer']"),
    ).not.toBeNull();
  });

  it("passes default dimensions (800×600) to CytoscapeRenderer before ResizeObserver fires", () => {
    render(<GraphContainer {...DEFAULT_PROPS} />);
    const renderer = document.querySelector(
      "[data-testid='cytoscape-renderer']",
    ) as HTMLElement;
    expect(renderer.getAttribute("data-width")).toBe("800");
    expect(renderer.getAttribute("data-height")).toBe("600");
  });

  it("updates dimensions when ResizeObserver fires", () => {
    let capturedCallback: ResizeObserverCallback | null = null;

    class MockResizeObserver {
      constructor(callback: ResizeObserverCallback) {
        capturedCallback = callback;
      }
      observe = vi.fn();
      disconnect = vi.fn();
    }
    vi.stubGlobal("ResizeObserver", MockResizeObserver);

    render(<GraphContainer {...DEFAULT_PROPS} />);

    act(() => {
      capturedCallback?.(
        [
          { contentRect: { width: 1200, height: 800 } },
        ] as unknown as ResizeObserverEntry[],
        {} as ResizeObserver,
      );
    });

    const renderer = document.querySelector(
      "[data-testid='cytoscape-renderer']",
    ) as HTMLElement;
    expect(renderer.getAttribute("data-width")).toBe("1200");
    expect(renderer.getAttribute("data-height")).toBe("800");
  });

  it("handles zero dimensions gracefully", () => {
    let capturedCallback: ResizeObserverCallback | null = null;

    class MockResizeObserver {
      constructor(callback: ResizeObserverCallback) {
        capturedCallback = callback;
      }
      observe = vi.fn();
      disconnect = vi.fn();
    }
    vi.stubGlobal("ResizeObserver", MockResizeObserver);

    render(<GraphContainer {...DEFAULT_PROPS} />);

    act(() => {
      capturedCallback?.(
        [
          { contentRect: { width: 0, height: 0 } },
        ] as unknown as ResizeObserverEntry[],
        {} as ResizeObserver,
      );
    });

    const renderer = document.querySelector(
      "[data-testid='cytoscape-renderer']",
    ) as HTMLElement;
    expect(renderer.getAttribute("data-width")).toBe("0");
    expect(renderer.getAttribute("data-height")).toBe("0");
  });
});
