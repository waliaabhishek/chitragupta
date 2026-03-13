import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { DimensionPieChart } from "./DimensionPieChart";

vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
    }: {
      option: {
        series?: { type?: string; data?: unknown[] }[];
        graphic?: unknown[];
      };
      style?: object;
      showLoading?: boolean;
    }) => (
      <div
        data-testid="echarts"
        data-series-type={option.series?.[0]?.type ?? ""}
        data-series-count={String(option.series?.[0]?.data?.length ?? 0)}
        data-has-graphic={option.graphic ? "true" : "false"}
      />
    ),
  ),
}));

function makeBucket(dimension: string, value: string, amount: string): AggregationBucket {
  return {
    dimensions: { [dimension]: value },
    time_bucket: "2026-02-01",
    total_amount: amount,
    usage_amount: amount,
    shared_amount: "0.00",
    row_count: 1,
  };
}

describe("DimensionPieChart", () => {
  it("shows no-data graphic for empty data", () => {
    render(<DimensionPieChart data={[]} dimension="product_type" />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-has-graphic")).toBe("true");
    expect(chart.getAttribute("data-series-type")).toBe("");
  });

  it("renders pie chart with data", () => {
    const data = [
      makeBucket("product_type", "kafka", "100.00"),
      makeBucket("product_type", "connect", "50.00"),
    ];
    render(<DimensionPieChart data={data} dimension="product_type" />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("pie");
    expect(chart.getAttribute("data-series-count")).toBe("2");
  });

  it("caps at topN + Other when more than topN items", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket("product_type", `type-${i}`, String(100 - i * 5)),
    );
    render(<DimensionPieChart data={data} dimension="product_type" topN={10} />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-count")).toBe("11"); // 10 + Other
  });
});
