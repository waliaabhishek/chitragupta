import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CostVelocityChart } from "./CostVelocityChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

// Capture the full ECharts option so tests can inspect tooltip.formatter, series, yAxis, etc.
let capturedOption: Record<string, unknown> = {};

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    capturedOption = option;
    return (
      <div
        data-testid="echarts"
        data-series-type={
          Array.isArray(option.series) && option.series.length > 0
            ? (option.series[0] as { type: string }).type
            : ""
        }
      />
    );
  },
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
  beforeEach(() => {
    capturedOption = {};
  });

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

  it("topN default is 10 — 11 distinct topics yield 10 series", () => {
    // 11 topics each with entries in 2 time buckets to produce deltas
    const topics = Array.from({ length: 11 }, (_, i) => `topic-${i + 1}`);
    const data: TopicAttributionAggregationBucket[] = topics.flatMap(
      (topic, i) => [
        makeBucket(topic, String((i + 1) * 10), "2026-01-01"),
        makeBucket(topic, String((i + 1) * 20), "2026-01-02"),
      ],
    );
    render(<CostVelocityChart data={data} />);
    const series = capturedOption.series as unknown[];
    expect(series).toHaveLength(10);
  });

  it("tooltip formatter — positive delta shows '+$X.XX increase'", () => {
    const data = [
      makeBucket("topic-a", "100.00", "2026-01-01"),
      makeBucket("topic-a", "250.75", "2026-01-02"),
    ];
    render(<CostVelocityChart data={data} />);
    const tooltip = capturedOption.tooltip as {
      formatter: (params: unknown) => string;
    };
    const result = tooltip.formatter([
      {
        value: 150.75,
        seriesName: "topic-a",
        marker: "",
        axisValue: "2026-01-02",
      },
    ]);
    expect(result).toContain("+$150.75 increase");
  });

  it("tooltip formatter — negative delta shows '-$X.XX decrease'", () => {
    const data = [
      makeBucket("topic-a", "100.00", "2026-01-01"),
      makeBucket("topic-a", "57.50", "2026-01-02"),
    ];
    render(<CostVelocityChart data={data} />);
    const tooltip = capturedOption.tooltip as {
      formatter: (params: unknown) => string;
    };
    const result = tooltip.formatter([
      {
        value: -42.5,
        seriesName: "topic-a",
        marker: "",
        axisValue: "2026-01-02",
      },
    ]);
    expect(result).toContain("-$42.50 decrease");
  });

  it("tooltip formatter — zero delta shows '$0.00 no change'", () => {
    const data = [
      makeBucket("topic-a", "100.00", "2026-01-01"),
      makeBucket("topic-a", "100.00", "2026-01-02"),
    ];
    render(<CostVelocityChart data={data} />);
    const tooltip = capturedOption.tooltip as {
      formatter: (params: unknown) => string;
    };
    const result = tooltip.formatter([
      { value: 0, seriesName: "topic-a", marker: "", axisValue: "2026-01-02" },
    ]);
    expect(result).toContain("$0.00 no change");
  });

  it("yAxis axisLabel formatter uses formatCurrency", () => {
    const data = [
      makeBucket("topic-a", "100.00", "2026-01-01"),
      makeBucket("topic-a", "200.00", "2026-01-02"),
    ];
    render(<CostVelocityChart data={data} />);
    const yAxis = capturedOption.yAxis as {
      axisLabel: { formatter: (v: number) => string };
    };
    expect(yAxis.axisLabel.formatter(1234.56)).toBe("$1,234.56");
  });
});
