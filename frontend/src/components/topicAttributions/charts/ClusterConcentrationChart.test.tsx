import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ClusterConcentrationChart } from "./ClusterConcentrationChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{ type: string; name: string }>;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-series-count={String(series?.length ?? 0)}
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

describe("ClusterConcentrationChart", () => {
  it("renders an ECharts stacked bar chart", () => {
    const data = [
      makeBucket("lkc-abc", "topic-a", "100.00"),
      makeBucket("lkc-def", "topic-b", "50.00"),
    ];
    render(<ClusterConcentrationChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("bar");
  });

  it("includes an Other series for topics outside top N", () => {
    // Build 6 distinct topics to push some into "Other" (topN=5)
    const data = Array.from({ length: 6 }, (_, i) =>
      makeBucket("lkc-abc", `topic-${i}`, String((6 - i) * 10)),
    );
    render(<ClusterConcentrationChart data={data} />);
    const chart = screen.getByTestId("echarts");
    // 5 top topics + 1 Other = 6 series
    expect(Number(chart.getAttribute("data-series-count"))).toBe(6);
  });

  it("renders without crashing when data is empty", () => {
    render(<ClusterConcentrationChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });
});
