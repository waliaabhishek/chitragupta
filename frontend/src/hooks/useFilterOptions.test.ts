import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";
import { server } from "../test/mocks/server";
import type {
  AggregationResponse,
  IdentityResponse,
  PaginatedResponse,
  ResourceResponse,
} from "../types/api";
import { useFilterOptions } from "./useFilterOptions";

const identityFixture: PaginatedResponse<IdentityResponse> = {
  items: [
    {
      identity_id: "u-1",
      display_name: "Alice",
      ecosystem: "ccloud",
      tenant_id: "t-001",
      identity_type: "user",
      created_at: null,
      deleted_at: null,
      last_seen_at: null,
      metadata: {},
    },
    {
      identity_id: "u-2",
      display_name: null,
      ecosystem: "ccloud",
      tenant_id: "t-001",
      identity_type: "user",
      created_at: null,
      deleted_at: null,
      last_seen_at: null,
      metadata: {},
    },
  ],
  total: 2,
  page: 1,
  page_size: 1000,
  pages: 1,
};

const resourceFixture: PaginatedResponse<ResourceResponse> = {
  items: [
    {
      resource_id: "r-1",
      display_name: "Cluster 1",
      ecosystem: "ccloud",
      tenant_id: "t-001",
      resource_type: "kafka_cluster",
      parent_id: null,
      owner_id: null,
      status: "active",
      created_at: null,
      deleted_at: null,
      last_seen_at: null,
      metadata: {},
    },
    {
      resource_id: "r-2",
      display_name: null,
      ecosystem: "ccloud",
      tenant_id: "t-001",
      resource_type: "kafka_cluster",
      parent_id: null,
      owner_id: null,
      status: "active",
      created_at: null,
      deleted_at: null,
      last_seen_at: null,
      metadata: {},
    },
  ],
  total: 2,
  page: 1,
  page_size: 1000,
  pages: 2,
};

const aggregateFixture: AggregationResponse = {
  buckets: [
    {
      dimensions: { product_type: "KAFKA" },
      time_bucket: "2026-01-01",
      total_amount: "10.00",
      usage_amount: "8.00",
      shared_amount: "2.00",
      row_count: 1,
    },
    {
      dimensions: { product_type: "SCHEMA_REGISTRY" },
      time_bucket: "2026-01-01",
      total_amount: "5.00",
      usage_amount: "4.00",
      shared_amount: "1.00",
      row_count: 1,
    },
  ],
  total_amount: "15.00",
  usage_amount: "12.00",
  shared_amount: "3.00",
  total_rows: 2,
};

describe("useFilterOptions", () => {
  it("useFilterOptions_fetches_all_3_endpoints_and_maps_options_correctly", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/identities", () =>
        HttpResponse.json(identityFixture),
      ),
      http.get("/api/v1/tenants/acme/resources", () =>
        HttpResponse.json(resourceFixture),
      ),
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () =>
        HttpResponse.json(aggregateFixture),
      ),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));

    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.identityOptions).toEqual([
      { label: "Alice (u-1)", value: "u-1" },
      { label: "u-2", value: "u-2" },
    ]);
    expect(result.current.resourceOptions).toEqual([
      { label: "Cluster 1 (r-1)", value: "r-1" },
      { label: "r-2", value: "r-2" },
    ]);
    expect(result.current.productTypeOptions).toEqual([
      { label: "KAFKA", value: "KAFKA" },
      { label: "SCHEMA_REGISTRY", value: "SCHEMA_REGISTRY" },
    ]);
  });

  it("useFilterOptions_deduplicates_product_types_from_aggregate_buckets", async () => {
    const duplicateAggregateFixture: AggregationResponse = {
      ...aggregateFixture,
      buckets: [
        {
          dimensions: { product_type: "KAFKA" },
          time_bucket: "2026-01-01",
          total_amount: "10.00",
          usage_amount: "8.00",
          shared_amount: "2.00",
          row_count: 1,
        },
        {
          dimensions: { product_type: "KAFKA" },
          time_bucket: "2026-01-02",
          total_amount: "8.00",
          usage_amount: "6.00",
          shared_amount: "2.00",
          row_count: 1,
        },
        {
          dimensions: { product_type: "SCHEMA_REGISTRY" },
          time_bucket: "2026-01-01",
          total_amount: "5.00",
          usage_amount: "4.00",
          shared_amount: "1.00",
          row_count: 1,
        },
      ],
    };

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () =>
        HttpResponse.json(duplicateAggregateFixture),
      ),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.productTypeOptions).toHaveLength(2);
    expect(result.current.productTypeOptions).toEqual([
      { label: "KAFKA", value: "KAFKA" },
      { label: "SCHEMA_REGISTRY", value: "SCHEMA_REGISTRY" },
    ]);
  });

  it("useFilterOptions_handles_fetch_failure_gracefully", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/identities", () => HttpResponse.error()),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.identityOptions).toEqual([]);
    expect(result.current.resourceOptions).toEqual([]);
    expect(result.current.productTypeOptions).toEqual([]);
  });

  it("useFilterOptions_does_not_call_fetch_when_tenantName_is_empty_string", () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");

    const { result } = renderHook(() => useFilterOptions("", null, null));

    expect(result.current.isLoading).toBe(false);
    expect(result.current.identityOptions).toEqual([]);
    expect(fetchSpy).not.toHaveBeenCalled();

    fetchSpy.mockRestore();
  });

  it("useFilterOptions_re_fetches_when_startDate_changes", async () => {
    let callCount = 0;

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () => {
        callCount++;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          usage_amount: "0",
          shared_amount: "0",
          total_rows: 0,
        });
      }),
    );

    const { rerender } = renderHook(
      ({ startDate }: { startDate: string | null }) =>
        useFilterOptions("acme", startDate, null),
      { initialProps: { startDate: null } },
    );

    await waitFor(() => expect(callCount).toBeGreaterThanOrEqual(1));
    const countAfterFirst = callCount;

    rerender({ startDate: "2026-01-01" });
    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });

  it("useFilterOptions_re_fetches_when_endDate_changes", async () => {
    let callCount = 0;

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () => {
        callCount++;
        return HttpResponse.json({
          buckets: [],
          total_amount: "0",
          usage_amount: "0",
          shared_amount: "0",
          total_rows: 0,
        });
      }),
    );

    const { rerender } = renderHook(
      ({ endDate }: { endDate: string | null }) =>
        useFilterOptions("acme", null, endDate),
      { initialProps: { endDate: null } },
    );

    await waitFor(() => expect(callCount).toBeGreaterThanOrEqual(1));
    const countAfterFirst = callCount;

    rerender({ endDate: "2026-01-31" });
    await waitFor(() => expect(callCount).toBeGreaterThan(countAfterFirst));
  });

  it("useFilterOptions_sets_error_on_http_error_status", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/identities", () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.identityOptions).toEqual([]);
    expect(result.current.resourceOptions).toEqual([]);
  });

  it("useFilterOptions_sets_error_on_aggregate_http_error_status", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/aggregate", () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.productTypeOptions).toEqual([]);
  });

  it("useFilterOptions_sets_error_when_resource_returns_http_error_status", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/identities", () =>
        HttpResponse.json(identityFixture),
      ),
      http.get("/api/v1/tenants/acme/resources", () =>
        HttpResponse.json({}, { status: 500 }),
      ),
    );

    const { result } = renderHook(() => useFilterOptions("acme", null, null));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    expect(result.current.error).not.toBeNull();
    expect(result.current.resourceOptions).toEqual([]);
  });

  it("unmounting before fetch resolves does not cause state update errors", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Promise executor runs synchronously — resolveIdentities is set before any fetch
    let resolveIdentities!: () => void;
    const pendingIdentities = new Promise<void>((resolve) => {
      resolveIdentities = resolve;
    });

    server.use(
      http.get("/api/v1/tenants/acme/identities", async () => {
        await pendingIdentities;
        return HttpResponse.json(identityFixture);
      }),
    );

    const { unmount } = renderHook(() => useFilterOptions("acme", null, null));

    // Unmount before the delayed identity fetch resolves
    unmount();

    // Now resolve — cancelled=true guard should prevent state updates
    resolveIdentities();
    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(errorSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });
});
