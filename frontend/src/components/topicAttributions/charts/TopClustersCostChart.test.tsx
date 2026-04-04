import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { TopClustersCostChart } from "./TopClustersCostChart";
import {
  buildTopClustersCostData,
  aggregateByCluster,
} from "./clusterUtils";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{ type: string; name: string }>;
    const yAxis = option.yAxis as { data?: string[] } | undefined;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-series-count={String(series?.length ?? 0)}
        data-yaxis-count={String(yAxis?.data?.length ?? 0)}
      />
    );
  },
}));

function makeBucket(
  clusterId: string,
  topicName: string,
  amount: string,
): TopicAttributionAggregationBucket {
  return {
    dimensions: { cluster_resource_id: clusterId, topic_name: topicName },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("TopClustersCostChart", () => {
  it("renders horizontal stacked bars with 6 series for 20-cluster dataset", () => {
    const data = Array.from({ length: 20 }, (_, i) =>
      makeBucket(`lkc-cluster-${i}`, `topic-${i % 6}`, String((i + 1) * 10)),
    );
    render(<TopClustersCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("bar");
    expect(Number(chart.getAttribute("data-series-count"))).toBe(6);
  });

  it("top-10 filter: y-axis has exactly 10 entries for 15-cluster input", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket(`lkc-cluster-${i}`, "topic-a", String((i + 1) * 10)),
    );
    render(<TopClustersCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(Number(chart.getAttribute("data-yaxis-count"))).toBe(10);
  });

  it("Other bucket: 1 cluster × 7 topics yields series count of 6", () => {
    const data = Array.from({ length: 7 }, (_, i) =>
      makeBucket("lkc-only", `topic-${i}`, String((7 - i) * 10)),
    );
    render(<TopClustersCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(Number(chart.getAttribute("data-series-count"))).toBe(6);
  });

  it("renders without crashing when data is empty", () => {
    render(<TopClustersCostChart data={[]} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(Number(chart.getAttribute("data-series-count"))).toBe(1);
  });
});

describe("buildTopClustersCostData", () => {
  it("top-N: returns exactly 10 clusters from a 15-cluster dataset", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket(`lkc-cluster-${i}`, "topic-a", String((i + 1) * 10)),
    );
    const { clusters } = buildTopClustersCostData(data);
    expect(clusters.length).toBe(10);
  });
});

describe("aggregateByCluster", () => {
  it("sums two buckets for the same cluster correctly in clusterTotals", () => {
    const data = [
      makeBucket("lkc-abc", "topic-a", "100.00"),
      makeBucket("lkc-abc", "topic-b", "50.00"),
    ];
    const { clusterTotals } = aggregateByCluster(data);
    expect(clusterTotals["lkc-abc"]).toBeCloseTo(150.0);
  });

  it("tracks separate clusters independently", () => {
    const data = [
      makeBucket("lkc-abc", "topic-a", "200.00"),
      makeBucket("lkc-def", "topic-a", "75.00"),
    ];
    const { clusterTotals } = aggregateByCluster(data);
    expect(clusterTotals["lkc-abc"]).toBeCloseTo(200.0);
    expect(clusterTotals["lkc-def"]).toBeCloseTo(75.0);
  });

  it("maps missing cluster_resource_id and topic_name to 'Unknown'", () => {
    const bucket: TopicAttributionAggregationBucket = {
      dimensions: {},
      time_bucket: "2026-01-01",
      total_amount: "99.00",
      row_count: 1,
    };
    const { clusterTotals, clusterTopic } = aggregateByCluster([bucket]);
    expect(clusterTotals["Unknown"]).toBeCloseTo(99.0);
    expect(clusterTopic["Unknown"]["Unknown"]).toBeCloseTo(99.0);
  });
});
