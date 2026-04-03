import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ProductTypeMixChart } from "./ProductTypeMixChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{ type: string; data: number[] }>;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-series-data={JSON.stringify(series?.map((s) => s.data) ?? [])}
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

describe("ProductTypeMixChart", () => {
  it("renders an ECharts stacked area (line) chart", () => {
    const data = [
      makeBucket("KAFKA_STORAGE", "100.00"),
      makeBucket("KAFKA_NETWORK_READ", "50.00"),
    ];
    render(<ProductTypeMixChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("line");
  });

  it("renders without crashing when data is empty", () => {
    render(<ProductTypeMixChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("normalises values to 100% per time bucket", () => {
    const data = [
      makeBucket("KAFKA_STORAGE", "75.00", "2026-01-01"),
      makeBucket("KAFKA_NETWORK_READ", "25.00", "2026-01-01"),
    ];
    render(<ProductTypeMixChart data={data} />);
    const chart = screen.getByTestId("echarts");
    // Each series has data[0] = percentage for the single time bucket
    const seriesData = JSON.parse(
      chart.getAttribute("data-series-data") ?? "[]",
    ) as number[][];
    // Sum of all series values at index 0 should equal 100%
    const bucketSum = seriesData.reduce((acc, d) => acc + (d[0] ?? 0), 0);
    expect(bucketSum).toBeCloseTo(100, 5);
  });
});
