import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AttributionMethodDonut } from "./AttributionMethodDonut";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{
      type: string;
      data: Array<{ name: string; value: number }>;
    }>;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-slice-values={JSON.stringify(series?.[0]?.data ?? [])}
      />
    );
  },
}));

function makeBucket(
  attributionMethod: string,
  amount: string,
): TopicAttributionAggregationBucket {
  return {
    dimensions: { attribution_method: attributionMethod },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("AttributionMethodDonut", () => {
  it("renders an ECharts pie chart", () => {
    const data = [
      makeBucket("bytes_ratio", "100.00"),
      makeBucket("even_split", "50.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("pie");
  });

  it("renders without crashing when data is empty", () => {
    render(<AttributionMethodDonut data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("aggregates amounts by attribution method", () => {
    const data = [
      makeBucket("bytes_ratio", "100.00"),
      makeBucket("bytes_ratio", "50.00"),
      makeBucket("even_split", "200.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    const chart = screen.getByTestId("echarts");
    const slices = JSON.parse(
      chart.getAttribute("data-slice-values") ?? "[]",
    ) as Array<{ name: string; value: number }>;
    const bytesRatio = slices.find((s) => s.name === "bytes_ratio");
    const evenSplit = slices.find((s) => s.name === "even_split");
    // bytes_ratio: 100 + 50 = 150; even_split: 200
    expect(bytesRatio?.value).toBe(150);
    expect(evenSplit?.value).toBe(200);
  });
});
