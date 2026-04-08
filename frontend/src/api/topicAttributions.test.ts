import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { server } from "../test/mocks/server";
import {
  exportTopicAttributions,
  fetchTopicAttributionAggregation,
  fetchTopicAttributionDates,
  fetchTopicAttributions,
} from "./topicAttributions";

const BASE = "/api/v1";

describe("fetchTopicAttributions", () => {
  it("fetches paginated topic attributions", async () => {
    const mockData = {
      items: [
        {
          dimension_id: 1,
          ecosystem: "ccloud",
          tenant_id: "t-001",
          timestamp: "2026-01-01T00:00:00Z",
          env_id: "env-1",
          cluster_resource_id: "lkc-abc",
          topic_name: "my-topic",
          product_category: "KAFKA",
          product_type: "KAFKA_STORAGE",
          attribution_method: "bytes_ratio",
          amount: "10.00",
        },
      ],
      total: 1,
      page: 1,
      page_size: 100,
      pages: 1,
    };
    server.use(
      http.get(`${BASE}/tenants/acme/topic-attributions`, () =>
        HttpResponse.json(mockData),
      ),
    );

    const result = await fetchTopicAttributions("acme", {
      page: 1,
      page_size: 100,
    });
    expect(result.items).toHaveLength(1);
    expect(result.total).toBe(1);
  });

  it("constructs URL with filter query params", async () => {
    let capturedUrl = "";
    server.use(
      http.get(`${BASE}/tenants/acme/topic-attributions`, ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          items: [],
          total: 0,
          page: 1,
          page_size: 100,
          pages: 0,
        });
      }),
    );

    await fetchTopicAttributions("acme", {
      page: 1,
      page_size: 50,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      cluster_resource_id: "lkc-abc",
      topic_name: "my-topic",
      product_type: "KAFKA_STORAGE",
      attribution_method: "bytes_ratio",
      timezone: "America/Chicago",
    });

    expect(capturedUrl).toContain("page=1");
    expect(capturedUrl).toContain("page_size=50");
    expect(capturedUrl).toContain("start_date=2026-01-01");
    expect(capturedUrl).toContain("end_date=2026-01-31");
    expect(capturedUrl).toContain("cluster_resource_id=lkc-abc");
    expect(capturedUrl).toContain("topic_name=my-topic");
    expect(capturedUrl).toContain("product_type=KAFKA_STORAGE");
    expect(capturedUrl).toContain("attribution_method=bytes_ratio");
    expect(capturedUrl).toContain("timezone=America%2FChicago");
  });

  it("throws on HTTP error", async () => {
    server.use(
      http.get(
        `${BASE}/tenants/acme/topic-attributions`,
        () => new HttpResponse(null, { status: 500 }),
      ),
    );
    await expect(fetchTopicAttributions("acme", {})).rejects.toThrow(
      "HTTP 500",
    );
  });
});

describe("fetchTopicAttributionAggregation", () => {
  it("fetches aggregation data with group_by and time_bucket", async () => {
    const mockData = {
      buckets: [
        {
          dimensions: { topic_name: "my-topic" },
          time_bucket: "2026-01-01",
          total_amount: "25.00",
          row_count: 5,
        },
      ],
      total_amount: "25.00",
      total_rows: 5,
    };
    server.use(
      http.get(`${BASE}/tenants/acme/topic-attributions/aggregate`, () =>
        HttpResponse.json(mockData),
      ),
    );

    const result = await fetchTopicAttributionAggregation("acme", {
      group_by: ["topic_name"],
      time_bucket: "day",
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    });
    expect(result.buckets).toHaveLength(1);
    expect(result.total_amount).toBe("25.00");
  });

  it("appends multiple group_by params", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        `${BASE}/tenants/acme/topic-attributions/aggregate`,
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

    await fetchTopicAttributionAggregation("acme", {
      group_by: ["topic_name", "product_type"],
      time_bucket: "month",
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    });

    // Multiple group_by params should be present
    expect(capturedUrl).toContain("group_by=topic_name");
    expect(capturedUrl).toContain("group_by=product_type");
    expect(capturedUrl).toContain("time_bucket=month");
  });

  it("throws on HTTP error", async () => {
    server.use(
      http.get(
        `${BASE}/tenants/acme/topic-attributions/aggregate`,
        () => new HttpResponse(null, { status: 404 }),
      ),
    );
    await expect(
      fetchTopicAttributionAggregation("acme", {
        group_by: ["topic_name"],
        time_bucket: "day",
        start_date: "2026-01-01",
        end_date: "2026-01-31",
      }),
    ).rejects.toThrow();
  });
});

describe("exportTopicAttributions", () => {
  it("returns a Blob on success", async () => {
    server.use(
      http.post(
        `${BASE}/tenants/acme/topic-attributions/export`,
        () =>
          new HttpResponse("date,amount\n2026-01-01,10.00\n", {
            headers: { "Content-Type": "text/csv" },
          }),
      ),
    );

    const blob = await exportTopicAttributions("acme", {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
    });
    // Verify the returned object is functionally a Blob. Avoid `instanceof Blob`
    // because jsdom installs its own Blob while undici's fetch returns Node's
    // native Blob — the two classes are not identity-equal across realms, and
    // the exact behavior varies between Node versions (passes on 25, fails on
    // 22). Checking shape + content is Node-version-independent.
    expect(blob.type).toBe("text/csv");
    expect(blob.size).toBeGreaterThan(0);
    expect(await blob.text()).toContain("date,amount");
  });

  it("uses POST method with query params on URL — no request body", async () => {
    let capturedMethod = "";
    let capturedUrl = "";
    let capturedBody: string | null = null;

    server.use(
      http.post(
        `${BASE}/tenants/acme/topic-attributions/export`,
        async ({ request }) => {
          capturedMethod = request.method;
          capturedUrl = request.url;
          capturedBody = await request.text();
          return new HttpResponse("date,amount\n", {
            headers: { "Content-Type": "text/csv" },
          });
        },
      ),
    );

    await exportTopicAttributions("acme", {
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      timezone: "America/Chicago",
    });

    expect(capturedMethod).toBe("POST");
    expect(capturedUrl).toContain("start_date=2026-01-01");
    expect(capturedUrl).toContain("end_date=2026-01-31");
    expect(capturedUrl).toContain("timezone=America%2FChicago");
    // No request body — query params only
    expect(capturedBody).toBe("");
  });

  it("throws on HTTP error", async () => {
    server.use(
      http.post(
        `${BASE}/tenants/acme/topic-attributions/export`,
        () => new HttpResponse(null, { status: 500 }),
      ),
    );
    await expect(exportTopicAttributions("acme", {})).rejects.toThrow();
  });
});

describe("fetchTopicAttributionDates", () => {
  it("returns dates array", async () => {
    server.use(
      http.get(`${BASE}/tenants/acme/topic-attributions/dates`, () =>
        HttpResponse.json({ dates: ["2026-01-01", "2026-01-02"] }),
      ),
    );

    const result = await fetchTopicAttributionDates("acme");
    expect(result.dates).toHaveLength(2);
    expect(result.dates[0]).toBe("2026-01-01");
  });

  it("returns empty dates array when no data", async () => {
    server.use(
      http.get(`${BASE}/tenants/acme/topic-attributions/dates`, () =>
        HttpResponse.json({ dates: [] }),
      ),
    );

    const result = await fetchTopicAttributionDates("acme");
    expect(result.dates).toEqual([]);
  });

  it("throws on HTTP error", async () => {
    server.use(
      http.get(
        `${BASE}/tenants/acme/topic-attributions/dates`,
        () => new HttpResponse(null, { status: 403 }),
      ),
    );
    await expect(fetchTopicAttributionDates("acme")).rejects.toThrow();
  });
});
