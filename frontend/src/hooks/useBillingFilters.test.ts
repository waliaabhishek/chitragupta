import type React from "react";
import { act, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { useBillingFilters } from "./useBillingFilters";

function makeWrapper(initialSearch = ""): ({ children }: { children: ReactNode }) => React.JSX.Element {
  return function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
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

describe("useBillingFilters — timezone", () => {
  it("defaults timezone to browser locale when no URL param and no localStorage", () => {
    const browserTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper(),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe(browserTz);
  });

  it("reads timezone from URL search params", () => {
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper("?timezone=America%2FChicago"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("America/Chicago");
  });

  it("restores timezone from localStorage when URL has no timezone param", () => {
    localStorage.setItem("user_timezone", "America/Denver");
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper(),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("America/Denver");
  });

  it("URL timezone param takes precedence over localStorage", () => {
    localStorage.setItem("user_timezone", "America/Denver");
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper("?timezone=Europe%2FParis"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).timezone).toBe("Europe/Paris");
  });

  it("setFilter with timezone saves to localStorage under user_timezone", () => {
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilter as any)("timezone", "Asia/Shanghai");
    });
    expect(localStorage.getItem("user_timezone")).toBe("Asia/Shanghai");
  });

  it("resetFilters removes user_timezone from localStorage", () => {
    localStorage.setItem("user_timezone", "America/Denver");
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper("?timezone=America%2FDenver"),
    });
    act(() => {
      result.current.resetFilters();
    });
    expect(localStorage.getItem("user_timezone")).toBeNull();
  });

  it("queryParams includes timezone when set via URL", () => {
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper("?timezone=America%2FLos_Angeles"),
    });
    const params = result.current.toQueryParams();
    expect(params["timezone"]).toBe("America/Los_Angeles");
  });

  it("setFilters with timezone saves to localStorage", () => {
    const { result } = renderHook(() => useBillingFilters(), {
      wrapper: makeWrapper(),
    });
    act(() => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (result.current.setFilters as any)({ timezone: "Europe/London" });
    });
    expect(localStorage.getItem("user_timezone")).toBe("Europe/London");
  });
});
