import type React from "react";
import { act, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../test/mocks/server";
import { ResourceLinkProvider, useResourceLinks } from "./ResourceLinkContext";
import { TenantProvider, useTenant } from "./TenantContext";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeWrapper() {
  return function Wrapper({
    children,
  }: {
    children: ReactNode;
  }): React.JSX.Element {
    return (
      <TenantProvider>
        <ResourceLinkProvider>{children}</ResourceLinkProvider>
      </TenantProvider>
    );
  };
}

const RESOURCE_API = "/api/v1/tenants/acme/resources";
const IDENTITY_API = "/api/v1/tenants/acme/identities";

/** Minimal resource shape returned by the API for index building. */
function makeResourcesResponse(
  items: Array<{
    resource_id: string;
    resource_type: string;
    parent_id: string | null;
    metadata?: Record<string, unknown>;
  }>,
) {
  return {
    items: items.map((r) => ({
      ecosystem: "ccloud",
      tenant_id: "t-001",
      resource_id: r.resource_id,
      resource_type: r.resource_type,
      display_name: null,
      parent_id: r.parent_id,
      owner_id: null,
      status: "active",
      created_at: null,
      deleted_at: null,
      last_seen_at: null,
      metadata: r.metadata ?? {},
    })),
    total: items.length,
    page: 1,
    page_size: 100,
    pages: 1,
  };
}

/** Minimal identity shape returned by the identities API. */
function makeIdentitiesResponse(
  items: Array<{
    identity_id: string;
    identity_type: string;
  }>,
) {
  return {
    items: items.map((r) => ({
      identity_id: r.identity_id,
      identity_type: r.identity_type,
      display_name: null,
      deleted_at: null,
    })),
    total: items.length,
    page: 1,
    page_size: 100,
    pages: 1,
  };
}

afterEach(() => {
  localStorage.clear();
});

// ---------------------------------------------------------------------------
// Feature flag defaults
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — feature flag", () => {
  it("feature flag defaults to off", async () => {
    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());
    expect(result.current.enabled).toBe(false);
  });

  it("feature flag can be toggled on via setEnabled", async () => {
    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());

    act(() => {
      result.current.setEnabled(true);
    });

    expect(result.current.enabled).toBe(true);
  });

  it("feature flag state persists across page refreshes via localStorage", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());
    expect(result.current.enabled).toBe(true);
  });

  it("feature flag toggle updates localStorage", async () => {
    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());

    act(() => {
      result.current.setEnabled(true);
    });

    expect(localStorage.getItem("chargeback_deep_links_enabled")).toBe("true");

    act(() => {
      result.current.setEnabled(false);
    });

    expect(localStorage.getItem("chargeback_deep_links_enabled")).toBe("false");
  });

  it("resolveUrl returns null when feature flag is off", async () => {
    // Exercises the if (!enabled) return null branch inside resolveUrl
    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(false));
    expect(result.current.resolveUrl("sa-anything")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Resource fetch behaviour
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — resource fetching", () => {
  it("does not fetch resources when feature flag is off", async () => {
    let fetchCount = 0;
    server.use(
      http.get(RESOURCE_API, () => {
        fetchCount++;
        return HttpResponse.json(makeResourcesResponse([]));
      }),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());
    // Wait a tick to ensure any async fetch would have fired
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(fetchCount).toBe(0);
  });

  it("fetches resources when feature flag is turned on", async () => {
    let fetchCount = 0;
    server.use(
      http.get(RESOURCE_API, () => {
        fetchCount++;
        return HttpResponse.json(makeResourcesResponse([]));
      }),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current).toBeDefined());

    act(() => {
      result.current.setEnabled(true);
    });

    await waitFor(() => expect(fetchCount).toBeGreaterThan(0));
  });

  it("non-ok API response does not crash — index remains empty", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    let fetchAttempted = false;
    server.use(
      http.get(RESOURCE_API, () => {
        fetchAttempted = true;
        return new HttpResponse(null, { status: 500 });
      }),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // Wait for fetch to fire and complete (isLoading starts false, goes true, then false again)
    await waitFor(() => expect(fetchAttempted).toBe(true));
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.resolveUrl("lkc-any")).toBeNull();
  });

  it("deleted resources are excluded from the index", async () => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json({
          items: [
            {
              ecosystem: "ccloud",
              tenant_id: "t-001",
              resource_id: "mock-deleted-001",
              resource_type: "service_account",
              display_name: null,
              parent_id: null,
              owner_id: null,
              status: "deleted",
              created_at: null,
              deleted_at: "2024-01-01T00:00:00Z",
              last_seen_at: null,
              metadata: {},
            },
            {
              ecosystem: "ccloud",
              tenant_id: "t-001",
              resource_id: "mock-active-001",
              resource_type: "service_account",
              display_name: null,
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
          page_size: 100,
          pages: 1,
        }),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // mock-active-001 is in index → resolves; mock-deleted-001 is excluded
    await waitFor(() =>
      expect(result.current.resolveUrl("mock-active-001")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("mock-deleted-001")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — index lookups
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — resolveUrl index lookups", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );
  });

  it("env-xxx resolves to environments URL", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "env-abc123",
              resource_type: "environment",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      expect(result.current.resolveUrl("env-abc123")).toBe(
        "https://confluent.cloud/environments/env-abc123",
      );
    });
  });

  it("environment resource with non-env- prefix resolves via index to environment URL", async () => {
    // mock-environment-001 has no env- prefix — prefix fallback won't fire.
    // resolveFromEntry's case "environment" is exercised via the index.
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-environment-001",
              resource_type: "environment",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      expect(result.current.resolveUrl("mock-environment-001")).toBe(
        "https://confluent.cloud/environments/mock-environment-001",
      );
    });
  });

  it("lkc-xxx resolves to cluster URL when parent env is in index", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "env-abc123",
              resource_type: "environment",
              parent_id: null,
            },
            {
              resource_id: "lkc-def456",
              resource_type: "kafka_cluster",
              parent_id: "env-abc123",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      const url = result.current.resolveUrl("lkc-def456");
      expect(url).toBeTruthy();
    });

    const url = result.current.resolveUrl("lkc-def456");
    expect(url).toBe(
      "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456",
    );
  });

  it("lkc-xxx not in index returns null (parent unknown, no fallback)", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    const url = result.current.resolveUrl("lkc-not-in-index");
    expect(url).toBeNull();
  });

  it("schema_registry resolves to schema registry URL when parent env is in index", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "env-abc123",
              resource_type: "environment",
              parent_id: null,
            },
            {
              resource_id: "lsrc-def456",
              resource_type: "schema_registry",
              parent_id: "env-abc123",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      const url = result.current.resolveUrl("lsrc-def456");
      expect(url).toBeTruthy();
    });

    const url = result.current.resolveUrl("lsrc-def456");
    expect(url).toBe(
      "https://confluent.cloud/environments/env-abc123/stream-governance/schema-registry/overview",
    );
  });

  it("connector in index returns null (broken link disabled)", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "env-abc123",
              resource_type: "environment",
              parent_id: null,
            },
            {
              resource_id: "lkc-def456",
              resource_type: "kafka_cluster",
              parent_id: "env-abc123",
            },
            {
              resource_id: "lcc-conn01",
              resource_type: "connector",
              parent_id: "lkc-def456",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // Wait for resource index to load. Must use lkc- (no prefix fallback) not env- (has prefix fallback line 245).
    await waitFor(() => {
      expect(result.current.resolveUrl("lkc-def456")).toBeTruthy();
    });

    expect(result.current.resolveUrl("lcc-conn01")).toBeNull();
  });

  it("service_account in index resolves via service_account type to per-principal URL", async () => {
    // Use an ID with no prefix fallback so waitFor can only pass after index loads,
    // ensuring resolveFromEntry's service_account case is exercised.
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-sa-001",
              resource_type: "service_account",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      expect(result.current.resolveUrl("mock-sa-001")).toBe(
        "https://confluent.cloud/settings/principals/mock-sa-001?view=identity",
      );
    });
  });

  it("unknown resource_type in index returns null (default branch)", async () => {
    // Include a lkc- resource so we can gate on index load (lkc- has no prefix fallback).
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "env-abc123",
              resource_type: "environment",
              parent_id: null,
            },
            {
              resource_id: "lkc-def456",
              resource_type: "kafka_cluster",
              parent_id: "env-abc123",
            },
            {
              resource_id: "unknown-001",
              resource_type: "unknown_type",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // lkc-def456 only resolves after the index loads — guarantees resolveFromEntry is reached
    await waitFor(() => {
      expect(result.current.resolveUrl("lkc-def456")).toBeTruthy();
    });

    const url = result.current.resolveUrl("unknown-001");
    expect(url).toBeNull();
  });

  it("kafka_cluster with null parent_id in index returns null", async () => {
    // mock-gate-001 is service_account — no prefix fallback, resolves only after index loads.
    // When it resolves, lkc-orphan is also indexed.
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-001",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lkc-orphan",
              resource_type: "kafka_cluster",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-001")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lkc-orphan")).toBeNull();
  });

  it("schema_registry with null parent_id in index returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-002",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lsrc-orphan",
              resource_type: "schema_registry",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-002")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lsrc-orphan")).toBeNull();
  });

  it("connector with null parent_id in index returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-003",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lcc-orphan",
              resource_type: "connector",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-003")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lcc-orphan")).toBeNull();
  });

  it("connector with valid parent_id but cluster not in index returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-005",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lcc-missing-cluster",
              resource_type: "connector",
              parent_id: "lkc-not-in-index",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-005")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lcc-missing-cluster")).toBeNull();
  });

  it("connector with valid parent_id but cluster has null parent_id returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-004",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lkc-no-env",
              resource_type: "kafka_cluster",
              parent_id: null,
            },
            {
              resource_id: "lcc-broken",
              resource_type: "connector",
              parent_id: "lkc-no-env",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-004")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lcc-broken")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — prefix fallback (sa-)
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — prefix fallback for sa-", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );
  });

  it("sa-xxx not in index resolves via prefix fallback to org service-accounts URL", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    const url = result.current.resolveUrl("sa-abc123");
    expect(url).toBe(
      "https://confluent.cloud/settings/principals/sa-abc123?view=identity",
    );
  });

  it("sa- identity_id in chargebacks context resolves via prefix fallback", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    // Simulates chargebacks grid identity_id column with sa- prefix
    const url = result.current.resolveUrl("sa-service-account-001");
    expect(url).toBe(
      "https://confluent.cloud/settings/principals/sa-service-account-001?view=identity",
    );
  });

  it("sa- identity_id in allocation issues context resolves via prefix fallback", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    const url = result.current.resolveUrl("sa-xyz999");
    expect(url).toBe(
      "https://confluent.cloud/settings/principals/sa-xyz999?view=identity",
    );
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — no broken links for unknown IDs
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — no broken links", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );
  });

  it("resource ID with no index entry and no prefix fallback returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    // Unknown prefix (xyz- has no fallback), not in index
    const url = result.current.resolveUrl("xyz-unknownprefix");
    expect(url).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — flink_compute_pool
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — resolveUrl flink_compute_pool", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );
  });

  it("flink_compute_pool in index resolves to flink URL", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-flink",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lfcp-def456",
              resource_type: "flink_compute_pool",
              parent_id: "env-abc123",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-flink")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lfcp-def456")).toBe(
      "https://confluent.cloud/environments/env-abc123/flink/pools/lfcp-def456/overview",
    );
  });

  it("flink_compute_pool with null parent_id returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-flink2",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lfcp-orphan",
              resource_type: "flink_compute_pool",
              parent_id: null,
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-flink2")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lfcp-orphan")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — ksqldb_cluster
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — resolveUrl ksqldb_cluster", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );
  });

  it("ksqldb_cluster in index resolves to ksqlDB editor URL", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-ksql",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lksqlc-ghi789",
              resource_type: "ksqldb_cluster",
              parent_id: "env-abc123",
              metadata: { kafka_cluster_id: "lkc-def456" },
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-ksql")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lksqlc-ghi789")).toBe(
      "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/ksql/lksqlc-ghi789/editor",
    );
  });

  it("ksqldb_cluster with null parent_id returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-ksql2",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lksqlc-orphan",
              resource_type: "ksqldb_cluster",
              parent_id: null,
              metadata: { kafka_cluster_id: "lkc-def456" },
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-ksql2")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lksqlc-orphan")).toBeNull();
  });

  it("ksqldb_cluster with missing kafka_cluster_id in metadata returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-ksql3",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lksqlc-no-kafka",
              resource_type: "ksqldb_cluster",
              parent_id: "env-abc123",
              metadata: {},
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-ksql3")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lksqlc-no-kafka")).toBeNull();
  });

  it("ksqldb_cluster with non-string kafka_cluster_id in metadata returns null", async () => {
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(
          makeResourcesResponse([
            {
              resource_id: "mock-gate-ksql4",
              resource_type: "service_account",
              parent_id: null,
            },
            {
              resource_id: "lksqlc-bad-kafka",
              resource_type: "ksqldb_cluster",
              parent_id: "env-abc123",
              metadata: { kafka_cluster_id: 42 },
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() =>
      expect(result.current.resolveUrl("mock-gate-ksql4")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("lksqlc-bad-kafka")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveUrl — identity index
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — identity index", () => {
  beforeEach(() => {
    localStorage.setItem("chargeback_deep_links_enabled", "true");
    server.use(
      http.get(RESOURCE_API, () =>
        HttpResponse.json(makeResourcesResponse([])),
      ),
    );
  });

  it("service_account in identity index resolves to per-principal URL", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(
          makeIdentitiesResponse([
            {
              identity_id: "mock-sa-via-identity",
              identity_type: "service_account",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(result.current.resolveUrl("mock-sa-via-identity")).toBe(
      "https://confluent.cloud/settings/principals/mock-sa-via-identity?view=identity",
    );
  });

  it("user in identity index resolves to principals URL", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(
          makeIdentitiesResponse([
            { identity_id: "mock-user-via-identity", identity_type: "user" },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      expect(result.current.resolveUrl("mock-user-via-identity")).toBe(
        "https://confluent.cloud/settings/principals/mock-user-via-identity?view=identity",
      );
    });
  });

  it("identity_provider in identity index resolves to workload_identities URL", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(
          makeIdentitiesResponse([
            {
              identity_id: "mock-op-via-identity",
              identity_type: "identity_provider",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => {
      expect(result.current.resolveUrl("mock-op-via-identity")).toBe(
        "https://confluent.cloud/settings/org/workload_identities/provider/oidc/view/mock-op-via-identity",
      );
    });
  });

  it("identity_pool in identity index returns null (broken link disabled)", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(
          makeIdentitiesResponse([
            {
              identity_id: "mock-pool-via-identity",
              identity_type: "identity_pool",
            },
            { identity_id: "mock-gate-pool", identity_type: "api_key" },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // Wait for identity index to load. Must use api_key (no prefix fallback) not sa- (has prefix fallback line 244).
    await waitFor(() => {
      expect(result.current.resolveUrl("mock-gate-pool")).toBeTruthy();
    });

    expect(result.current.resolveUrl("mock-pool-via-identity")).toBeNull();
  });

  it("api_key in identity index resolves via identity fallback", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(
          makeIdentitiesResponse([
            { identity_id: "TRFPF55LGU5RBQIT", identity_type: "api_key" },
            // non-prefixed gate to confirm identity index loaded
            {
              identity_id: "mock-gate-apikey",
              identity_type: "service_account",
            },
          ]),
        ),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // Gate on the non-prefixed identity entry — only resolves after identity index loads
    await waitFor(() => {
      expect(result.current.resolveUrl("mock-gate-apikey")).toBeTruthy();
    });

    expect(result.current.resolveUrl("TRFPF55LGU5RBQIT")).toBe(
      "https://confluent.cloud/settings/api-keys/edit/TRFPF55LGU5RBQIT",
    );
  });

  it("deleted identities are excluded from the identity index", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json({
          items: [
            {
              identity_id: "mock-deleted-identity",
              identity_type: "service_account",
              display_name: null,
              deleted_at: "2024-01-01T00:00:00Z",
            },
            {
              identity_id: "mock-active-identity",
              identity_type: "user",
              display_name: null,
              deleted_at: null,
            },
          ],
          total: 2,
          page: 1,
          page_size: 100,
          pages: 1,
        }),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    // Gate: mock-active-identity has no prefix, resolves only after identity index loads
    await waitFor(() =>
      expect(result.current.resolveUrl("mock-active-identity")).toBeTruthy(),
    );

    expect(result.current.resolveUrl("mock-deleted-identity")).toBeNull();
  });

  it("u-* prefix fallback resolves without identity index", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(result.current.resolveUrl("u-xyz999")).toBe(
      "https://confluent.cloud/settings/principals/u-xyz999?view=identity",
    );
  });

  it("op-* prefix fallback resolves to workload_identities URL", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(result.current.resolveUrl("op-xyz999")).toBe(
      "https://confluent.cloud/settings/org/workload_identities/provider/oidc/view/op-xyz999",
    );
  });

  it("pool-* prefix fallback returns null (broken link disabled)", async () => {
    server.use(
      http.get(IDENTITY_API, () =>
        HttpResponse.json(makeIdentitiesResponse([])),
      ),
    );

    const { result } = renderHook(() => useResourceLinks(), {
      wrapper: makeWrapper(),
    });

    await waitFor(() => expect(result.current.enabled).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    expect(result.current.resolveUrl("pool-xyz999")).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Tenant switch
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — tenant switch", () => {
  it("tenant switch clears and rebuilds the resource index", async () => {
    // Track fetch calls to ANY tenant's resources and identities endpoints.
    let resourceFetchCount = 0;
    let identityFetchCount = 0;
    server.use(
      http.get("/api/v1/tenants/:tenant/resources", () => {
        resourceFetchCount++;
        return HttpResponse.json(makeResourcesResponse([]));
      }),
      http.get("/api/v1/tenants/:tenant/identities", () => {
        identityFetchCount++;
        return HttpResponse.json(makeIdentitiesResponse([]));
      }),
    );

    localStorage.setItem("chargeback_deep_links_enabled", "true");

    const { result } = renderHook(
      () => ({
        links: useResourceLinks(),
        tenant: useTenant(),
      }),
      { wrapper: makeWrapper() },
    );

    await waitFor(() => expect(result.current.links.enabled).toBe(true));

    const resourceFetchCountAfterInit = resourceFetchCount;
    const identityFetchCountAfterInit = identityFetchCount;

    // Simulate tenant switch by changing the active tenant
    act(() => {
      result.current.tenant.setCurrentTenant({
        tenant_name: "globex",
        tenant_id: "t-002",
        ecosystem: "self_managed",
        dates_pending: 0,
        dates_calculated: 5,
        last_calculated_date: "2024-01-08",
        topic_attribution_status: "disabled" as const,
        topic_attribution_error: null,
      });
    });

    await waitFor(() =>
      expect(resourceFetchCount).toBeGreaterThan(resourceFetchCountAfterInit),
    );
    await waitFor(() =>
      expect(identityFetchCount).toBeGreaterThan(identityFetchCountAfterInit),
    );
  });
});

// ---------------------------------------------------------------------------
// useResourceLinks outside provider
// ---------------------------------------------------------------------------

describe("ResourceLinkContext — guard", () => {
  it("useResourceLinks called outside ResourceLinkProvider throws with descriptive error", () => {
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => undefined);
    try {
      expect(() => renderHook(() => useResourceLinks())).toThrow(
        /ResourceLinkProvider/,
      );
    } finally {
      consoleError.mockRestore();
    }
  });
});
