import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  useIdentityFilterOptions,
  useResourceFilterOptions,
  ENTITY_TYPE_OPTIONS,
} from "./useEntityFilterOptions";

vi.mock("./useInventorySummary", () => ({
  useInventorySummary: vi.fn(() => ({
    data: {
      identity_counts: { service_account: 5, user: 10 },
      resource_counts: { kafka: 3, connector: 1 },
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

describe("useIdentityFilterOptions", () => {
  it("returns identity type options derived from inventory data", () => {
    const { result } = renderHook(() => useIdentityFilterOptions("acme"));
    expect(result.current.identityTypeOptions).toHaveLength(2);
    expect(result.current.identityTypeOptions.map((o) => o.value)).toContain(
      "service_account",
    );
    expect(result.current.isLoading).toBe(false);
  });
});

describe("useResourceFilterOptions", () => {
  it("returns resource type options and fixed status options", () => {
    const { result } = renderHook(() => useResourceFilterOptions("acme"));
    expect(result.current.resourceTypeOptions).toHaveLength(2);
    expect(result.current.resourceStatusOptions.map((o) => o.value)).toContain(
      "active",
    );
    expect(result.current.isLoading).toBe(false);
  });
});

describe("ENTITY_TYPE_OPTIONS", () => {
  it("exports identity and resource options", () => {
    expect(ENTITY_TYPE_OPTIONS.map((o) => o.value)).toContain("identity");
    expect(ENTITY_TYPE_OPTIONS.map((o) => o.value)).toContain("resource");
  });
});
