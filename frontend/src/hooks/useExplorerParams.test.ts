import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { createElement } from "react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { useExplorerParams } from "./useExplorerParams";

function makeWrapper(
  initialSearch = "",
): ({ children }: { children: ReactNode }) => React.JSX.Element {
  return function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
    return createElement(MemoryRouter, { initialEntries: [`/${initialSearch}`] }, children);
  };
}

describe("useExplorerParams", () => {
  it("returns null/false defaults when URL has no params", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    expect(result.current.params.focus).toBeNull();
    expect(result.current.params.at).toBeNull();
    expect(result.current.params.tag).toBeNull();
    expect(result.current.params.tag_value).toBeNull();
    expect(result.current.params.diff).toBe(false);
    expect(result.current.params.from_start).toBeNull();
    expect(result.current.params.from_end).toBeNull();
    expect(result.current.params.to_start).toBeNull();
    expect(result.current.params.to_end).toBeNull();
  });

  it("reads all params from URL on mount", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(
        "?focus=lkc-abc&at=2026-03-15&tag=team&tag_value=platform&diff=true",
      ),
    });

    expect(result.current.params.focus).toBe("lkc-abc");
    expect(result.current.params.at).toBe("2026-03-15");
    expect(result.current.params.tag).toBe("team");
    expect(result.current.params.tag_value).toBe("platform");
    expect(result.current.params.diff).toBe(true);
  });

  it("diff param: 'true' string → boolean true", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?diff=true"),
    });

    expect(result.current.params.diff).toBe(true);
  });

  it("diff param: absent → false", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    expect(result.current.params.diff).toBe(false);
  });

  it("reads diff date range params from URL", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(
        "?from_start=2026-01-01&from_end=2026-01-31&to_start=2026-02-01&to_end=2026-02-28",
      ),
    });

    expect(result.current.params.from_start).toBe("2026-01-01");
    expect(result.current.params.from_end).toBe("2026-01-31");
    expect(result.current.params.to_start).toBe("2026-02-01");
    expect(result.current.params.to_end).toBe("2026-02-28");
  });

  it("pushParam updates URL (creates history entry via replace: false)", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.pushParam("focus", "env-xyz");
    });

    expect(result.current.params.focus).toBe("env-xyz");
  });

  it("replaceParam updates URL without history entry (replace: true)", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.replaceParam("at", "2026-04-01");
    });

    expect(result.current.params.at).toBe("2026-04-01");
  });

  it("pushParams batch-updates multiple params in single call", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.pushParams({
        diff: true,
        from_start: "2026-01-01",
        from_end: "2026-01-31",
      });
    });

    expect(result.current.params.diff).toBe(true);
    expect(result.current.params.from_start).toBe("2026-01-01");
    expect(result.current.params.from_end).toBe("2026-01-31");
  });

  it("pushParam with null removes the param", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?tag=team"),
    });

    act(() => {
      result.current.pushParam("tag", null);
    });

    expect(result.current.params.tag).toBeNull();
  });

  it("pushParam with false removes boolean param", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?diff=true"),
    });

    act(() => {
      result.current.pushParam("diff", false);
    });

    expect(result.current.params.diff).toBe(false);
  });

  // TASK-244: expand param
  it("reads expand param from URL", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?focus=lkc-abc&expand=topics"),
    });

    expect(result.current.params.expand).toBe("topics");
  });

  it("expand param absent → null", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    expect(result.current.params.expand).toBeNull();
  });

  it("pushParam sets expand param", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.pushParam("expand", "topics");
    });

    expect(result.current.params.expand).toBe("topics");
  });

  it("pushParam with null clears expand param", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?expand=topics"),
    });

    act(() => {
      result.current.pushParam("expand", null);
    });

    expect(result.current.params.expand).toBeNull();
  });

  it("reads expand=identities from URL", () => {
    const { result } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper("?expand=identities"),
    });

    expect(result.current.params.expand).toBe("identities");
  });

  it("setters are stable references (useCallback — no re-creation on re-render)", () => {
    const { result, rerender } = renderHook(() => useExplorerParams(), {
      wrapper: makeWrapper(),
    });

    const pushParamRef = result.current.pushParam;
    const replaceParamRef = result.current.replaceParam;
    const pushParamsRef = result.current.pushParams;
    const replaceParamsRef = result.current.replaceParams;

    rerender();

    expect(result.current.pushParam).toBe(pushParamRef);
    expect(result.current.replaceParam).toBe(replaceParamRef);
    expect(result.current.pushParams).toBe(pushParamsRef);
    expect(result.current.replaceParams).toBe(replaceParamsRef);
  });
});
