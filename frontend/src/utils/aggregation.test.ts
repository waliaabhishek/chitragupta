import { describe, expect, it } from "vitest";
import type { AggregationBucket } from "../types/api";
import {
  aggregateByDimension,
  aggregateByTime,
  formatCurrency,
  topNWithOther,
} from "./aggregation";

function makeBucket(
  overrides: Partial<AggregationBucket> = {},
): AggregationBucket {
  return {
    dimensions: { identity_id: "user-1" },
    time_bucket: "2026-02-01",
    total_amount: "10.00",
    usage_amount: "10.00",
    shared_amount: "0.00",
    row_count: 1,
    ...overrides,
  };
}

describe("aggregateByTime", () => {
  it("returns empty array for empty input", () => {
    expect(aggregateByTime([])).toEqual([]);
  });

  it("sums amounts for same time_bucket", () => {
    const buckets = [
      makeBucket({ time_bucket: "2026-02-01", total_amount: "10.00" }),
      makeBucket({ time_bucket: "2026-02-01", total_amount: "5.00" }),
    ];
    const result = aggregateByTime(buckets);
    expect(result).toHaveLength(1);
    expect(result[0].amount).toBeCloseTo(15.0);
  });

  it("keeps separate entries for different time_buckets", () => {
    const buckets = [
      makeBucket({ time_bucket: "2026-02-01", total_amount: "10.00" }),
      makeBucket({ time_bucket: "2026-02-02", total_amount: "5.00" }),
    ];
    const result = aggregateByTime(buckets);
    expect(result).toHaveLength(2);
  });

  it("sorts results by time ascending", () => {
    const buckets = [
      makeBucket({ time_bucket: "2026-02-03", total_amount: "30.00" }),
      makeBucket({ time_bucket: "2026-02-01", total_amount: "10.00" }),
      makeBucket({ time_bucket: "2026-02-02", total_amount: "20.00" }),
    ];
    const result = aggregateByTime(buckets);
    expect(result.map((r) => r.time)).toEqual([
      "2026-02-01",
      "2026-02-02",
      "2026-02-03",
    ]);
  });
});

describe("aggregateByDimension", () => {
  it("returns empty array for empty input", () => {
    expect(aggregateByDimension([], "identity_id")).toEqual([]);
  });

  it("sums amounts for same dimension value", () => {
    const buckets = [
      makeBucket({
        dimensions: { identity_id: "user-1" },
        total_amount: "10.00",
      }),
      makeBucket({
        dimensions: { identity_id: "user-1" },
        total_amount: "5.00",
      }),
    ];
    const result = aggregateByDimension(buckets, "identity_id");
    expect(result).toHaveLength(1);
    expect(result[0].key).toBe("user-1");
    expect(result[0].amount).toBeCloseTo(15.0);
  });

  it("keeps separate entries for different dimension values", () => {
    const buckets = [
      makeBucket({
        dimensions: { identity_id: "user-1" },
        total_amount: "10.00",
      }),
      makeBucket({
        dimensions: { identity_id: "user-2" },
        total_amount: "5.00",
      }),
    ];
    const result = aggregateByDimension(buckets, "identity_id");
    expect(result).toHaveLength(2);
  });

  it("uses 'Unknown' for missing dimension key", () => {
    const bucket = makeBucket({ dimensions: {} });
    const result = aggregateByDimension([bucket], "identity_id");
    expect(result[0].key).toBe("Unknown");
  });
});

describe("topNWithOther", () => {
  it("returns empty array for empty input", () => {
    expect(topNWithOther([], 10)).toEqual([]);
  });

  it("returns all items when count <= n (no Other bucket)", () => {
    const items = [
      { key: "a", amount: 100 },
      { key: "b", amount: 50 },
    ];
    const result = topNWithOther(items, 10);
    expect(result).toHaveLength(2);
    expect(result.find((r) => r.key === "Other")).toBeUndefined();
  });

  it("sorts by amount descending", () => {
    const items = [
      { key: "low", amount: 10 },
      { key: "high", amount: 100 },
      { key: "mid", amount: 50 },
    ];
    const result = topNWithOther(items, 10);
    expect(result[0].key).toBe("high");
    expect(result[1].key).toBe("mid");
    expect(result[2].key).toBe("low");
  });

  it("groups items beyond n into Other bucket", () => {
    const items = Array.from({ length: 12 }, (_, i) => ({
      key: `item-${i}`,
      amount: 100 - i * 5,
    }));
    const result = topNWithOther(items, 10);
    expect(result).toHaveLength(11); // 10 + Other
    expect(result[10].key).toBe("Other");
    // Items 10 and 11 have amounts 50 and 45
    expect(result[10].amount).toBe(95);
  });
});

describe("formatCurrency", () => {
  it("formats zero", () => {
    expect(formatCurrency(0)).toBe("$0.00");
  });

  it("formats a positive amount with 2 decimal places", () => {
    expect(formatCurrency(12.5)).toMatch(/^\$12\.50$/);
  });
});

// TASK-164: BucketLike structural interface tests
// TopicAttributionAggregationBucket satisfies BucketLike without usage_amount/shared_amount

describe("BucketLike structural interface (TASK-164)", () => {
  it("aggregateByTime accepts TopicAttributionAggregationBucket (no usage_amount/shared_amount)", () => {
    // TopicAttributionAggregationBucket shape — no usage_amount/shared_amount
    const topicBuckets = [
      {
        dimensions: { topic_name: "my-topic" },
        time_bucket: "2026-01-01",
        total_amount: "25.00",
        row_count: 3,
      },
      {
        dimensions: { topic_name: "other-topic" },
        time_bucket: "2026-01-01",
        total_amount: "10.00",
        row_count: 1,
      },
    ];
    // Must not throw — BucketLike is structurally satisfied
    const result = aggregateByTime(topicBuckets);
    expect(result).toHaveLength(1);
    expect(result[0].amount).toBeCloseTo(35.0);
  });

  it("aggregateByDimension accepts TopicAttributionAggregationBucket", () => {
    const topicBuckets = [
      {
        dimensions: { topic_name: "topic-a" },
        time_bucket: "2026-01-01",
        total_amount: "30.00",
        row_count: 2,
      },
      {
        dimensions: { topic_name: "topic-b" },
        time_bucket: "2026-01-01",
        total_amount: "20.00",
        row_count: 1,
      },
    ];
    const result = aggregateByDimension(topicBuckets, "topic_name");
    expect(result).toHaveLength(2);
    const topicA = result.find((r) => r.key === "topic-a");
    expect(topicA?.amount).toBeCloseTo(30.0);
  });
});
