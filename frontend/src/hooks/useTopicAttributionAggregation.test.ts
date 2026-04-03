import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createElement } from "react";
import type { ReactNode } from "react";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import { useTopicAttributionAggregation } from "./useTopicAttributionAggregation";

function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
}

function createWrapper() {
  const queryClient = createTestQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(
      QueryClientProvider,
      { client: queryClient },
      children,
    );
  };
}

const BASE_PARAMS = {
  tenantName: "acme",
  groupBy: ["topic_name"],
  timeBucket: "day" as const,
  startDate: "2026-02-01",
  endDate: "2026-02-28",
};

const AGG_RESPONSE = {
  buckets: [
    {
      dimensions: { topic_name: "my-topic" },
      time_bucket: "2026-02-01",
      total_amount: "25.00",
      row_count: 3,
    },
  ],
  total_amount: "25.00",
  total_rows: 3,
};

describe("useTopicAttributionAggregation", () => {
  it("starts in loading state", () => {
    server.use(
      http.get("/api/v1/tenants/acme/topic-attributions/aggregate", () =>
        HttpResponse.json(AGG_RESPONSE),
      ),
    );
    const { result } = renderHook(
      () => useTopicAttributionAggregation(BASE_PARAMS),
      {
        wrapper: createWrapper(),
      },
    );
    expect(result.current.isLoading).toBe(true);
    expect(result.current.data).toBeNull();
    expect(result.current.error).toBeNull();
  });

  it("returns data after successful fetch", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/topic-attributions/aggregate", () =>
        HttpResponse.json(AGG_RESPONSE),
      ),
    );
    const { result } = renderHook(
      () => useTopicAttributionAggregation(BASE_PARAMS),
      {
        wrapper: createWrapper(),
      },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.data).not.toBeNull();
    expect(result.current.data?.buckets).toHaveLength(1);
    expect(result.current.error).toBeNull();
  });

  it("returns error when server responds with 500", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/topic-attributions/aggregate", () =>
        HttpResponse.json({ detail: "Internal Server Error" }, { status: 500 }),
      ),
    );
    const { result } = renderHook(
      () => useTopicAttributionAggregation(BASE_PARAMS),
      {
        wrapper: createWrapper(),
      },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.error).not.toBeNull();
    expect(result.current.data).toBeNull();
  });

  it("appends group_by and time_bucket to request URL", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        "/api/v1/tenants/acme/topic-attributions/aggregate",
        ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({
            buckets: [],
            total_amount: "0",
            total_rows: 0,
          });
        },
      ),
    );
    const { result } = renderHook(
      () => useTopicAttributionAggregation(BASE_PARAMS),
      {
        wrapper: createWrapper(),
      },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).toContain("group_by=topic_name");
    expect(capturedUrl).toContain("time_bucket=day");
  });

  it("appends optional filter params when provided", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        "/api/v1/tenants/acme/topic-attributions/aggregate",
        ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({
            buckets: [],
            total_amount: "0",
            total_rows: 0,
          });
        },
      ),
    );
    const { result } = renderHook(
      () =>
        useTopicAttributionAggregation({
          ...BASE_PARAMS,
          clusterResourceId: "lkc-abc",
          topicName: "my-topic",
          productType: "KAFKA_STORAGE",
          timezone: "America/Chicago",
        }),
      { wrapper: createWrapper() },
    );
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(capturedUrl).toContain("cluster_resource_id=lkc-abc");
    expect(capturedUrl).toContain("topic_name=my-topic");
    expect(capturedUrl).toContain("product_type=KAFKA_STORAGE");
    expect(capturedUrl).toContain("timezone=America%2FChicago");
  });

  it("is disabled when tenantName is empty", () => {
    const { result } = renderHook(
      () => useTopicAttributionAggregation({ ...BASE_PARAMS, tenantName: "" }),
      { wrapper: createWrapper() },
    );
    expect(result.current.isLoading).toBe(false);
    expect(result.current.data).toBeNull();
  });
});
