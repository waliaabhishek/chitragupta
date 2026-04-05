import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { CostCompositionChart } from "./CostCompositionChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

let lastOption: Record<string, unknown> = {};

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    lastOption = option;
    return (
      <div
        data-testid="echarts"
        data-series-type={
          Array.isArray(option.series) && option.series.length > 0
            ? (option.series[0] as { type: string }).type
            : ""
        }
        data-series-count={String(
          Array.isArray(option.series) ? option.series.length : 0,
        )}
      />
    );
  },
}));

function makeBucket(
  productType: string,
  amount: string,
  timeBucket = "2026-01-01",
): TopicAttributionAggregationBucket {
  return {
    dimensions: { product_type: productType },
    time_bucket: timeBucket,
    total_amount: amount,
    row_count: 1,
  };
}

describe("CostCompositionChart", () => {
  it("renders an ECharts bar chart", () => {
    const data = [
      makeBucket("KAFKA_STORAGE", "100.00"),
      makeBucket("KAFKA_NETWORK_READ", "50.00"),
    ];
    render(<CostCompositionChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("bar");
  });

  it("renders without crashing when data is empty", () => {
    render(<CostCompositionChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("creates one series per product type", () => {
    const data = [
      makeBucket("KAFKA_STORAGE", "100.00", "2026-01-01"),
      makeBucket("KAFKA_NETWORK_READ", "50.00", "2026-01-01"),
      makeBucket("KAFKA_STORAGE", "80.00", "2026-01-02"),
    ];
    render(<CostCompositionChart data={data} />);
    const chart = screen.getByTestId("echarts");
    // 2 distinct product types → 2 series
    expect(chart.getAttribute("data-series-count")).toBe("2");
  });
});

describe("TASK-203: tooltip valueFormatter", () => {
  it("CostCompositionChart — valueFormatter present", () => {
    render(<CostCompositionChart data={[makeBucket("KAFKA_STORAGE", "100.00")]} />);
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    expect(typeof tooltip?.valueFormatter).toBe("function");
  });

  it("CostCompositionChart — valueFormatter output", () => {
    render(<CostCompositionChart data={[makeBucket("KAFKA_STORAGE", "100.00")]} />);
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    const valueFormatter = tooltip?.valueFormatter as (v: number) => string;
    expect(valueFormatter(1996.9649999999929)).toBe("$1,996.96");
  });

  it("CostCompositionChart — axisLabel formatter unchanged", () => {
    render(<CostCompositionChart data={[makeBucket("KAFKA_STORAGE", "100.00")]} />);
    type YAxis = { axisLabel?: { formatter?: (v: number) => string } };
    const yAxis = lastOption.yAxis as YAxis;
    const formatter = yAxis?.axisLabel?.formatter;
    expect(typeof formatter).toBe("function");
    expect(formatter!(1000)).toBe("$1,000.00");
  });
});
