import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AttributionMethodDonut } from "./AttributionMethodDonut";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{
      type: string;
      data: Array<{ name: string; value: number; itemStyle: { color: string }; methodKey: string }>;
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

  it("maps attribution methods to confidence tier labels", () => {
    const data = [
      makeBucket("bytes_ratio", "100.00"),
      makeBucket("retained_bytes_ratio", "50.00"),
      makeBucket("even_split", "200.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    const chart = screen.getByTestId("echarts");
    const slices = JSON.parse(
      chart.getAttribute("data-slice-values") ?? "[]",
    ) as Array<{ name: string; value: number; itemStyle: { color: string } }>;
    expect(slices.find((s) => s.name === "High Confidence")).toBeTruthy();
    expect(slices.find((s) => s.name === "Medium Confidence")).toBeTruthy();
    expect(slices.find((s) => s.name === "Low Confidence")).toBeTruthy();
  });

  it("uses semantic colors for tiers", () => {
    const data = [
      makeBucket("bytes_ratio", "100.00"),
      makeBucket("retained_bytes_ratio", "50.00"),
      makeBucket("even_split", "200.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    const chart = screen.getByTestId("echarts");
    const slices = JSON.parse(
      chart.getAttribute("data-slice-values") ?? "[]",
    ) as Array<{ name: string; value: number; itemStyle: { color: string } }>;
    expect(slices.find((s) => s.name === "High Confidence")?.itemStyle.color).toBe("#52c41a");
    expect(slices.find((s) => s.name === "Medium Confidence")?.itemStyle.color).toBe("#faad14");
    expect(slices.find((s) => s.name === "Low Confidence")?.itemStyle.color).toBe("#f5222d");
  });

  it("shows correct headline confidence percentage", () => {
    const data = [
      makeBucket("bytes_ratio", "100.00"),
      makeBucket("even_split", "100.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    expect(screen.getByTestId("confidence-headline").textContent).toBe("50%");
  });

  it("shows 100% confidence when no even_split data", () => {
    const data = [
      makeBucket("bytes_ratio", "300.00"),
      makeBucket("retained_bytes_ratio", "100.00"),
    ];
    render(<AttributionMethodDonut data={data} />);
    expect(screen.getByTestId("confidence-headline").textContent).toBe("100%");
  });

  it("shows em dash headline when data is empty", () => {
    render(<AttributionMethodDonut data={[]} />);
    expect(screen.getByTestId("confidence-headline").textContent).toBe("—");
  });

  it("renders subtitle text", () => {
    render(<AttributionMethodDonut data={[makeBucket("bytes_ratio", "100.00")]} />);
    expect(screen.getByText("of cost attributed with metrics data")).toBeTruthy();
  });

  it("falls back to Low Confidence for unknown attribution methods", () => {
    const data = [makeBucket("something_new", "100.00")];
    render(<AttributionMethodDonut data={data} />);
    const chart = screen.getByTestId("echarts");
    const slices = JSON.parse(
      chart.getAttribute("data-slice-values") ?? "[]",
    ) as Array<{ name: string; itemStyle: { color: string } }>;
    expect(slices[0].name).toBe("Low Confidence");
    expect(slices[0].itemStyle.color).toBe("#f5222d");
  });

  it("aggregates amounts by attribution method before tier mapping", () => {
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
    expect(slices.find((s) => s.name === "High Confidence")?.value).toBe(150);
    expect(slices.find((s) => s.name === "Low Confidence")?.value).toBe(200);
  });
});
