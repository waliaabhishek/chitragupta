import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { CostCompositionChart } from "./CostCompositionChart";
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
      data-series-count={String(
        Array.isArray(option.series) ? option.series.length : 0,
      )}
    />
  ),
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
