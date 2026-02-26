import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { createElement } from "react";
import { describe, expect, it } from "vitest";
import { useChargebackFilters } from "./useChargebackFilters";

function makeWrapper(initialSearch = ""): ({ children }: { children: ReactNode }) => JSX.Element {
  return function Wrapper({ children }: { children: ReactNode }): JSX.Element {
    return createElement(
      MemoryRouter,
      { initialEntries: [`/${initialSearch}`], future: { v7_startTransition: true, v7_relativeSplatPath: true } },
      children,
    );
  };
}

describe("useChargebackFilters", () => {
  it("provides default date values when URL has no params", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    // start_date defaults to 30 days ago (non-null string)
    expect(result.current.filters.start_date).toBeTruthy();
    expect(result.current.filters.start_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);

    // end_date defaults to today
    expect(result.current.filters.end_date).toBeTruthy();
    expect(result.current.filters.end_date).toMatch(/^\d{4}-\d{2}-\d{2}$/);

    // others default to null
    expect(result.current.filters.identity_id).toBeNull();
    expect(result.current.filters.product_type).toBeNull();
    expect(result.current.filters.resource_id).toBeNull();
    expect(result.current.filters.cost_type).toBeNull();
  });

  it("reads initial values from URL search params", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?start_date=2026-01-01&end_date=2026-01-31&identity_id=user-1"),
    });

    expect(result.current.filters.start_date).toBe("2026-01-01");
    expect(result.current.filters.end_date).toBe("2026-01-31");
    expect(result.current.filters.identity_id).toBe("user-1");
  });

  it("setFilter updates a single filter", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.setFilter("identity_id", "sa-123");
    });

    expect(result.current.filters.identity_id).toBe("sa-123");
  });

  it("setFilter with null removes the param", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?identity_id=user-1"),
    });

    act(() => {
      result.current.setFilter("identity_id", null);
    });

    expect(result.current.filters.identity_id).toBeNull();
  });

  it("resetFilters clears all filter params", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?identity_id=user-1&cost_type=usage&product_type=kafka"),
    });

    act(() => {
      result.current.resetFilters();
    });

    expect(result.current.filters.identity_id).toBeNull();
    expect(result.current.filters.cost_type).toBeNull();
    expect(result.current.filters.product_type).toBeNull();
  });

  it("toQueryParams excludes null values", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?identity_id=user-1"),
    });

    const params = result.current.toQueryParams();
    expect(params["identity_id"]).toBe("user-1");
    // null filters should not appear
    expect("cost_type" in params).toBe(false);
    expect("product_type" in params).toBe(false);
    expect("resource_id" in params).toBe(false);
  });

  it("toQueryParams includes default date values", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    const params = result.current.toQueryParams();
    expect(params["start_date"]).toBeTruthy();
    expect(params["end_date"]).toBeTruthy();
  });
});
