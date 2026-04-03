import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useChargebackFilters } from "./useChargebackFilters";

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
      wrapper: makeWrapper(
        "?start_date=2026-01-01&end_date=2026-01-31&identity_id=user-1",
      ),
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
      wrapper: makeWrapper(
        "?identity_id=user-1&cost_type=usage&product_type=kafka",
      ),
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

  it("useChargebackFilters_localStorage_fallback_loads_dates_when_url_has_no_params", () => {
    localStorage.setItem(
      "chargeback_date_range",
      JSON.stringify({ start_date: "2025-01-01", end_date: "2025-01-31" }),
    );
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    expect(result.current.filters.start_date).toBe("2025-01-01");
    expect(result.current.filters.end_date).toBe("2025-01-31");
  });

  it("useChargebackFilters_url_takes_precedence_over_localStorage", () => {
    localStorage.setItem(
      "chargeback_date_range",
      JSON.stringify({ start_date: "2025-01-01", end_date: "2025-01-31" }),
    );
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?start_date=2026-03-01&end_date=2026-03-14"),
    });

    expect(result.current.filters.start_date).toBe("2026-03-01");
    expect(result.current.filters.end_date).toBe("2026-03-14");
  });

  it("useChargebackFilters_setFilter_writes_date_to_localStorage", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.setFilter("start_date", "2026-02-01");
    });

    const stored = JSON.parse(localStorage.getItem("chargeback_date_range")!);
    expect(stored.start_date).toBe("2026-02-01");
  });

  it("useChargebackFilters_setFilters_batch_writes_dates_to_localStorage", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.setFilters({
        start_date: "2026-01-01",
        end_date: "2026-01-31",
      });
    });

    const stored = JSON.parse(localStorage.getItem("chargeback_date_range")!);
    expect(stored.start_date).toBe("2026-01-01");
    expect(stored.end_date).toBe("2026-01-31");
  });

  it("useChargebackFilters_resetFilters_clears_localStorage", () => {
    localStorage.setItem(
      "chargeback_date_range",
      JSON.stringify({ start_date: "2025-01-01", end_date: "2025-01-31" }),
    );
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.resetFilters();
    });

    expect(localStorage.getItem("chargeback_date_range")).toBeNull();
  });

  it("useChargebackFilters_non_date_fields_not_written_to_localStorage", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });

    act(() => {
      result.current.setFilter("identity_id", "sa-123");
    });

    expect(localStorage.getItem("chargeback_date_range")).toBeNull();
  });
});

describe("useChargebackFilters — timezone", () => {
  it("defaults timezone to browser locale when no URL param and no localStorage", () => {
    const browserTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe(browserTz);
  });

  it("reads timezone from URL search params", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?timezone=America%2FChicago"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("America/Chicago");
  });

  it("restores timezone from localStorage when URL has no timezone param", () => {
    localStorage.setItem("user_timezone", "America/Chicago");
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("America/Chicago");
  });

  it("URL timezone param takes precedence over localStorage", () => {
    localStorage.setItem("user_timezone", "America/Chicago");
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?timezone=Europe%2FLondon"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("Europe/London");
  });

  it("setFilter with timezone saves to localStorage under user_timezone", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilter as any)("timezone", "America/Chicago");
    });
    expect(localStorage.getItem("user_timezone")).toBe("America/Chicago");
  });

  it("resetFilters removes user_timezone from localStorage", () => {
    localStorage.setItem("user_timezone", "America/Chicago");
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?timezone=America%2FChicago"),
    });
    act(() => {
      result.current.resetFilters();
    });
    expect(localStorage.getItem("user_timezone")).toBeNull();
  });

  it("queryParams includes timezone when set via URL", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?timezone=America%2FDenver"),
    });
    const params = result.current.toQueryParams();
    expect(params["timezone"]).toBe("America/Denver");
  });

  it("setFilters with timezone saves to localStorage", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilters as any)({ timezone: "Asia/Tokyo" });
    });
    expect(localStorage.getItem("user_timezone")).toBe("Asia/Tokyo");
  });

  it("setFilter with empty string for timezone clears localStorage", () => {
    localStorage.setItem("user_timezone", "America/Chicago");
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilter as any)("timezone", "");
    });
    expect(localStorage.getItem("user_timezone")).toBeNull();
  });

  it("setFilters with empty string for timezone clears localStorage", () => {
    localStorage.setItem("user_timezone", "America/Chicago");
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilters as any)({ timezone: "" });
    });
    expect(localStorage.getItem("user_timezone")).toBeNull();
  });

  it("setFilters with null value removes the param", () => {
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?identity_id=sa-123"),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilters as any)({ identity_id: null });
    });
    expect(result.current.filters.identity_id).toBeNull();
  });
});
