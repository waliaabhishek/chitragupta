import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useTagFilters } from "./useTagFilters";

function makeWrapper(
  initialSearch = "",
): ({ children }: { children: ReactNode }) => React.JSX.Element {
  return function Wrapper({
    children,
  }: {
    children: ReactNode;
  }): React.JSX.Element {
    return createElement(
      MemoryRouter,
      { initialEntries: [`/${initialSearch}`] },
      children,
    );
  };
}

afterEach(() => {
  localStorage.clear();
});

describe("useTagFilters", () => {
  it("provides null defaults when URL has no params", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper(),
    });
    expect(result.current.filters.tag_key).toBeNull();
    expect(result.current.filters.entity_type).toBeNull();
  });

  it("reads filters from URL search params", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper("?tag_key=env&entity_type=resource"),
    });
    expect(result.current.filters.tag_key).toBe("env");
    expect(result.current.filters.entity_type).toBe("resource");
  });

  it("setFilter updates a single filter", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      result.current.setFilter("tag_key", "team");
    });
    expect(result.current.filters.tag_key).toBe("team");
  });

  it("setFilter with null removes the param", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper("?tag_key=env"),
    });
    act(() => {
      result.current.setFilter("tag_key", null);
    });
    expect(result.current.filters.tag_key).toBeNull();
  });

  it("resetFilters clears all filter params", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper("?tag_key=env&entity_type=resource"),
    });
    act(() => {
      result.current.resetFilters();
    });
    expect(result.current.filters.tag_key).toBeNull();
    expect(result.current.filters.entity_type).toBeNull();
  });

  it("queryParams excludes null values", () => {
    const { result } = renderHook(() => useTagFilters(), {
      wrapper: makeWrapper("?tag_key=env"),
    });
    const params = result.current.queryParams;
    expect(params["tag_key"]).toBe("env");
    expect("entity_type" in params).toBe(false);
  });
});
