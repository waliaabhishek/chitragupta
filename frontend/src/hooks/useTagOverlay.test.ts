import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { server } from "../test/mocks/server";
import { useTagOverlay } from "./useTagOverlay";
import type { GraphNode } from "../components/explorer/renderers/types";

function createTestQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
}

function createWrapper(): ({ children }: { children: ReactNode }) => React.JSX.Element {
  const queryClient = createTestQueryClient();
  return function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
    return createElement(QueryClientProvider, { client: queryClient }, children);
  };
}

function makeNode(id: string, tagValue: string | null, cost: number): GraphNode {
  return {
    id,
    resource_type: "kafka_cluster",
    display_name: id,
    cost,
    created_at: "2026-01-01T00:00:00Z",
    deleted_at: null,
    tags: tagValue !== null ? { team: tagValue } : {},
    parent_id: null,
    cloud: null,
    region: null,
    status: "active",
    cross_references: [],
  };
}

const TAG_PALETTE = [
  "#1677ff",
  "#52c41a",
  "#faad14",
  "#ff4d4f",
  "#722ed1",
  "#13c2c2",
  "#eb2f96",
  "#fa8c16",
  "#a0d911",
  "#ff6e33",
];

describe("useTagOverlay", () => {
  it("returns available keys from API when tenant is set", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team", "env"] }),
      ),
    );

    const { result } = renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes: [],
          activeKey: null,
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    expect(result.current.availableKeys).toEqual(["team", "env"]);
  });

  it("returns empty availableKeys and isLoadingKeys=false when API returns empty keys", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: [] }),
      ),
    );

    const { result } = renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes: [],
          activeKey: null,
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    expect(result.current.availableKeys).toEqual([]);
  });

  it("computes colorMap: top values by total cost get palette colors (index order)", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team"] }),
      ),
    );
    const nodes = [
      makeNode("n1", "platform", 500),
      makeNode("n2", "data", 200),
      makeNode("n3", "platform", 300), // platform total = 800 → rank 1 → palette[0]
    ];

    const { result } = renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes,
          activeKey: "team",
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    expect(result.current.colorMap["platform"]).toBe(TAG_PALETTE[0]);
    expect(result.current.colorMap["data"]).toBe(TAG_PALETTE[1]);
  });

  it("values beyond top 10 get #8c8c8c (other)", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team"] }),
      ),
    );
    // 11 distinct values — 11th gets grey
    const nodes = Array.from({ length: 11 }, (_, i) =>
      makeNode(`n${i}`, `value-${i}`, 100 - i),
    );

    const { result } = renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes,
          activeKey: "team",
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    // value-10 has cost 90 — lowest → 11th slot → grey
    expect(result.current.colorMap["value-10"]).toBe("#8c8c8c");
  });

  it("nodes without active key tag produce UNTAGGED — colorMap[\"UNTAGGED\"] === #d9d9d9", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team"] }),
      ),
    );
    // makeNode with null tagValue → tags = {} → tags["team"] = undefined → lookup key = "UNTAGGED"
    const nodes = [makeNode("n1", null, 100)];

    const { result } = renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes,
          activeKey: "team",
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    // enrichWithTagColor uses colorMap[n.tags[activeKey] ?? "UNTAGGED"] — UNTAGGED nodes produce key "UNTAGGED"
    // The hook must store "UNTAGGED" → "#d9d9d9" so the legend can render an UNTAGGED entry
    expect(result.current.colorMap["UNTAGGED"]).toBe("#d9d9d9");
  });

  it("colorMap recomputes when activeKey changes", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team", "env"] }),
      ),
    );
    const nodes = [makeNode("n1", "platform", 100)];

    const { result, rerender } = renderHook(
      ({ activeKey }: { activeKey: string | null }) =>
        useTagOverlay({
          tenantName: "acme",
          nodes,
          activeKey,
          onClearValue: vi.fn(),
        }),
      { wrapper: createWrapper(), initialProps: { activeKey: null as string | null } },
    );

    await waitFor(() => expect(result.current.isLoadingKeys).toBe(false));
    expect(Object.keys(result.current.colorMap)).toHaveLength(0);

    act(() => {
      rerender({ activeKey: "team" });
    });
    await waitFor(() => expect(Object.keys(result.current.colorMap).length).toBeGreaterThan(0));
  });

  it("does NOT call onClearValue on initial mount (isFirstKeyEffect ref guard)", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team"] }),
      ),
    );
    const onClearValue = vi.fn();

    renderHook(
      () =>
        useTagOverlay({
          tenantName: "acme",
          nodes: [],
          activeKey: "team",
          onClearValue,
        }),
      { wrapper: createWrapper() },
    );

    // Wait for effects to settle
    await new Promise((r) => setTimeout(r, 50));
    expect(onClearValue).not.toHaveBeenCalled();
  });

  it("calls onClearValue when activeKey changes (not on initial mount)", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags/keys", () =>
        HttpResponse.json({ keys: ["team", "env"] }),
      ),
    );
    const onClearValue = vi.fn();

    const { rerender } = renderHook(
      ({ activeKey }: { activeKey: string | null }) =>
        useTagOverlay({
          tenantName: "acme",
          nodes: [],
          activeKey,
          onClearValue,
        }),
      { wrapper: createWrapper(), initialProps: { activeKey: "team" } },
    );

    // Wait for initial effects — onClearValue must NOT fire
    await new Promise((r) => setTimeout(r, 50));
    expect(onClearValue).not.toHaveBeenCalled();

    // Change activeKey → must call onClearValue once
    act(() => {
      rerender({ activeKey: "env" });
    });
    await waitFor(() => expect(onClearValue).toHaveBeenCalledTimes(1));
  });
});
