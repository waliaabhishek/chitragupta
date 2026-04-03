import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { CostVelocityChart } from "./CostVelocityChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => (
    <div
      data-testid="echarts"
      data-series-type={
        Array.isArray(option.series) && option.series.length > 0
          ? (option.series[0] as { type: string }).type
          : ""
      }
    />
  ),
}));

function makeBucket(
  topicName: string,
  amount: string,
  timeBucket = "2026-01-01",
): TopicAttributionAggregationBucket {
  return {
    dimensions: { topic_name: topicName },
    time_bucket: timeBucket,
    total_amount: amount,
    row_count: 1,
  };
}

describe("CostVelocityChart", () => {
  it("renders an ECharts line chart", () => {
    const data = [
      makeBucket("topic-a", "100.00", "2026-01-01"),
      makeBucket("topic-a", "200.00", "2026-01-02"),
    ];
    render(<CostVelocityChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("line");
  });

  it("renders without crashing when data is empty", () => {
    render(<CostVelocityChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("renders without crashing with single time bucket (no deltas)", () => {
    const data = [makeBucket("topic-a", "100.00", "2026-01-01")];
    render(<CostVelocityChart data={data} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });
});
