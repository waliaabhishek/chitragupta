import type React from "react";
// GAP-100 TDD red phase — verification items 4 & 5
// Tests MUST fail until useChargebackFilters memoizes `filters` and returns `queryParams`.
// TASK-160.02 — added tag_key/tag_value filter support tests.
import { renderHook } from "@testing-library/react";
import { createElement } from "react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";
import { useChargebackFilters } from "./useChargebackFilters";

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <MemoryRouter>
      {children}
    </MemoryRouter>
  );
}

function makeWrapper(initialSearch = ""): ({ children }: { children: ReactNode }) => React.JSX.Element {
  return function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
    return createElement(
      MemoryRouter,
      { initialEntries: [`/${initialSearch}`] },
      children,
    );
  };
}

describe("useChargebackFilters — object reference stability (GAP-100)", () => {
  it("returns the same filters object reference across re-renders when searchParams have not changed", () => {
    // Verification item 4: filters must be memoized so that downstream useMemo/useEffect
    // deps that reference `filters` are not invalidated on every parent re-render.
    const { result, rerender } = renderHook(() => useChargebackFilters(), { wrapper });

    const firstRef = result.current.filters;
    rerender();
    const secondRef = result.current.filters;

    // FAILS in red state: current code creates a new object literal every render.
    expect(secondRef).toBe(firstRef);
  });

  it("returns queryParams as a stable memoized value on the return object", () => {
    // Verification item 5: queryParams must be a memoized value (not a function call result)
    // so that ChargebackGrid and ExportButton receive a stable reference in JSX.
    const { result, rerender } = renderHook(() => useChargebackFilters(), { wrapper });

    // FAILS in red state: queryParams is not in the current return type at all.
    expect(result.current.queryParams).toBeDefined();

    const firstRef = result.current.queryParams;
    rerender();
    const secondRef = result.current.queryParams;

    expect(secondRef).toBe(firstRef);
  });

  it("queryParams reflects filter field values as string-string pairs (nulls excluded)", () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    // queryParams must contain non-null filter values as plain strings (no nulls).
    const { result } = renderHook(() => useChargebackFilters(), { wrapper });

    // FAILS in red state: queryParams not returned.
    const qp = result.current.queryParams;
    expect(typeof qp).toBe("object");
    // All present values must be strings
    for (const v of Object.values(qp)) {
      expect(typeof v).toBe("string");
    }
    // Null-valued filters must be absent
    expect("identity_id" in qp).toBe(false);
    expect("product_type" in qp).toBe(false);
  });
});

describe("useChargebackFilters — tag filters (TASK-160.02)", () => {
  it("useChargebackFilters_reads_tag_key_from_url", () => {
    // FAILS: ChargebackFilters does not have tag_key field yet.
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?tag_key=env"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).tag_key).toBe("env");
  });

  it("useChargebackFilters_tag_key_included_in_queryParams", () => {
    // FAILS: tag_key not in queryParams / toQueryParams result.
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?tag_key=env"),
    });
    const params = result.current.toQueryParams();
    expect(params["tag_key"]).toBe("env");
  });

  it("useChargebackFilters_reads_tag_value_from_url", () => {
    // FAILS: ChargebackFilters does not have tag_value field yet.
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?tag_value=prod"),
    });
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    expect((result.current.filters as any).tag_value).toBe("prod");
  });

  it("useChargebackFilters_tag_value_included_in_queryParams", () => {
    // FAILS: tag_value not in queryParams / toQueryParams result.
    const { result } = renderHook(() => useChargebackFilters(), {
      wrapper: makeWrapper("?tag_value=prod"),
    });
    const params = result.current.toQueryParams();
    expect(params["tag_value"]).toBe("prod");
  });
});
