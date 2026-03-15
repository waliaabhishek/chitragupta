// GAP-100 TDD red phase — verification item 6
// Test MUST fail until useAllocationIssues uses primitive filter fields in useEffect deps.
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useAllocationIssues } from "./useAllocationIssues";
import type { ChargebackFilters } from "../types/filters";

// MSW server is already set up globally via src/test/setup.ts.

describe("useAllocationIssues — effect stability (GAP-100)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("does not re-fetch when parent re-renders with a new filters object reference but same field values", async () => {
    // Verification item 6: effect deps must be primitive filter fields, not the whole
    // `filters` object. A new object with identical field values must NOT trigger a fetch.

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({ items: [], total: 0, page: 1, page_size: 25, pages: 0 }),
        { headers: { "Content-Type": "application/json" } },
      ),
    );

    const filtersV1: ChargebackFilters = {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      identity_id: null,
      product_type: null,
      resource_id: null,
      cost_type: null,
    };

    const { rerender, result } = renderHook(
      ({ filters }: { filters: ChargebackFilters }) =>
        useAllocationIssues({ tenantName: "acme", filters, page: 1, pageSize: 25 }),
      { initialProps: { filters: filtersV1 } },
    );

    // Wait for initial fetch to complete.
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const fetchCountAfterInit = fetchSpy.mock.calls.length;
    expect(fetchCountAfterInit).toBeGreaterThan(0);

    // Re-render with a NEW object that has identical field values.
    // FAILS in red state: `filters` is whole object in deps → object ref changed → re-fetch fires.
    const filtersV2: ChargebackFilters = { ...filtersV1 };
    rerender({ filters: filtersV2 });

    await act(async () => {
      // Let any pending effects settle.
    });

    // Should NOT have triggered another fetch — only primitive field values changed check matters.
    expect(fetchSpy.mock.calls.length).toBe(fetchCountAfterInit);
  });
});
