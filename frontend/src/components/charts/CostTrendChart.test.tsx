import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { CostTrendChart } from "./CostTrendChart";

// echarts-for-react tries to render a canvas — mock it
vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
      showLoading,
    }: {
      option: { xAxis?: { data?: string[] }; series?: { data?: number[] }[] };
      style?: object;
      showLoading?: boolean;
    }) => (
      <div
        data-testid="echarts"
        data-loading={String(showLoading ?? false)}
        data-xaxis={JSON.stringify(option.xAxis?.data ?? [])}
        data-series={JSON.stringify(option.series?.[0]?.data ?? [])}
      />
    ),
  ),
}));

function makeBucket(
  timeBucket: string,
  identityId: string,
  amount: string,
): AggregationBucket {
  return {
    dimensions: { identity_id: identityId },
    time_bucket: timeBucket,
    total_amount: amount,
    row_count: 1,
  };
}

describe("CostTrendChart", () => {
  it("aggregates multiple buckets with same time_bucket into single data point", () => {
    const data = [
      makeBucket("2026-02-01", "user-1", "10.00"),
      makeBucket("2026-02-01", "user-2", "5.00"),
    ];
    render(<CostTrendChart data={data} timeBucket="day" />);
    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(chart.getAttribute("data-xaxis") ?? "[]") as string[];
    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as number[];
    expect(xAxis).toHaveLength(1);
    expect(series[0]).toBeCloseTo(15.0);
  });

  it("renders correct number of points (one per unique time_bucket)", () => {
    const data = [
      makeBucket("2026-02-01", "user-1", "10.00"),
      makeBucket("2026-02-02", "user-1", "20.00"),
      makeBucket("2026-02-02", "user-2", "5.00"),
    ];
    render(<CostTrendChart data={data} timeBucket="day" />);
    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(chart.getAttribute("data-xaxis") ?? "[]") as string[];
    expect(xAxis).toHaveLength(2);
  });

  it("sorts data by time_bucket ascending", () => {
    const data = [
      makeBucket("2026-02-03", "user-1", "30.00"),
      makeBucket("2026-02-01", "user-1", "10.00"),
      makeBucket("2026-02-02", "user-1", "20.00"),
    ];
    render(<CostTrendChart data={data} timeBucket="day" />);
    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(chart.getAttribute("data-xaxis") ?? "[]") as string[];
    expect(xAxis[0]).toBe("2026-02-01");
    expect(xAxis[1]).toBe("2026-02-02");
    expect(xAxis[2]).toBe("2026-02-03");
  });

  it("renders empty state for empty data with no data points", () => {
    render(<CostTrendChart data={[]} timeBucket="day" />);
    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(chart.getAttribute("data-xaxis") ?? "[]") as string[];
    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as number[];
    expect(xAxis).toHaveLength(0);
    expect(series).toHaveLength(0);
  });
});
