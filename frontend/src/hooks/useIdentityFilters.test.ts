import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useIdentityFilters } from "./useIdentityFilters";

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

describe("useIdentityFilters", () => {
  it("provides null defaults when URL has no params", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper(),
    });
    expect(result.current.filters.search).toBeNull();
    expect(result.current.filters.identity_type).toBeNull();
    expect(result.current.filters.tag_key).toBeNull();
    expect(result.current.filters.tag_value).toBeNull();
  });

  it("reads filters from URL search params", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper("?search=alice&identity_type=service_account"),
    });
    expect(result.current.filters.search).toBe("alice");
    expect(result.current.filters.identity_type).toBe("service_account");
  });

  it("setFilter updates a single filter", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      result.current.setFilter("search", "bob");
    });
    expect(result.current.filters.search).toBe("bob");
  });

  it("setFilter with null removes the param", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper("?search=alice"),
    });
    act(() => {
      result.current.setFilter("search", null);
    });
    expect(result.current.filters.search).toBeNull();
  });

  it("resetFilters clears all filter params", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper("?search=alice&identity_type=user"),
    });
    act(() => {
      result.current.resetFilters();
    });
    expect(result.current.filters.search).toBeNull();
    expect(result.current.filters.identity_type).toBeNull();
  });

  it("queryParams excludes null values", () => {
    const { result } = renderHook(() => useIdentityFilters(), {
      wrapper: makeWrapper("?search=alice"),
    });
    const params = result.current.queryParams;
    expect(params["search"]).toBe("alice");
    expect("identity_type" in params).toBe(false);
  });
});
