import type React from "react";
// GAP-100 TDD red phase — verification items 4 & 5
// Tests MUST fail until useChargebackFilters memoizes `filters` and returns `queryParams`.
import { renderHook } from "@testing-library/react";
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
