import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useResourceFilters } from "./useResourceFilters";

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

describe("useResourceFilters", () => {
  it("provides null defaults when URL has no params", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper(),
    });
    expect(result.current.filters.search).toBeNull();
    expect(result.current.filters.resource_type).toBeNull();
    expect(result.current.filters.status).toBeNull();
    expect(result.current.filters.tag_key).toBeNull();
    expect(result.current.filters.tag_value).toBeNull();
  });

  it("reads filters from URL search params", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper("?search=lkc-abc&status=active"),
    });
    expect(result.current.filters.search).toBe("lkc-abc");
    expect(result.current.filters.status).toBe("active");
  });

  it("setFilter updates a single filter", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      result.current.setFilter("resource_type", "kafka");
    });
    expect(result.current.filters.resource_type).toBe("kafka");
  });

  it("setFilter with null removes the param", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper("?status=active"),
    });
    act(() => {
      result.current.setFilter("status", null);
    });
    expect(result.current.filters.status).toBeNull();
  });

  it("resetFilters clears all filter params", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper("?search=lkc&resource_type=kafka"),
    });
    act(() => {
      result.current.resetFilters();
    });
    expect(result.current.filters.search).toBeNull();
    expect(result.current.filters.resource_type).toBeNull();
  });

  it("queryParams excludes null values", () => {
    const { result } = renderHook(() => useResourceFilters(), {
      wrapper: makeWrapper("?status=active"),
    });
    const params = result.current.queryParams;
    expect(params["status"]).toBe("active");
    expect("search" in params).toBe(false);
  });
});
