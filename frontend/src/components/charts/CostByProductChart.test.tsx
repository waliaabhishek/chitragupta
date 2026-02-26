import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { CostByProductChart } from "./CostByProductChart";

vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
    }: {
      option: { series?: { type?: string; data?: unknown[] }[] };
      style?: object;
      showLoading?: boolean;
    }) => (
      <div
        data-testid="echarts"
        data-series-type={option.series?.[0]?.type ?? ""}
        data-series-count={String(option.series?.[0]?.data?.length ?? 0)}
      />
    ),
  ),
}));

function makeBucket(productType: string, amount: string): AggregationBucket {
  return {
    dimensions: { product_type: productType },
    time_bucket: "2026-02-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("CostByProductChart", () => {
  it("renders pie chart by default", () => {
    const data = [makeBucket("kafka", "100.00"), makeBucket("connect", "50.00")];
    render(<CostByProductChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("pie");
  });

  it("renders treemap when chartType='treemap'", () => {
    const data = [makeBucket("kafka", "100.00")];
    render(<CostByProductChart data={data} chartType="treemap" />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("treemap");
  });

  it("passes correct number of data points", () => {
    const data = [
      makeBucket("kafka", "100.00"),
      makeBucket("connect", "50.00"),
      makeBucket("ksqldb", "25.00"),
    ];
    render(<CostByProductChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-count")).toBe("3");
  });

  it("renders empty state for empty data with no series items", () => {
    render(<CostByProductChart data={[]} />);
    const chart = screen.getByTestId("echarts");
    // Empty data results in graphic overlay option (no series)
    expect(chart.getAttribute("data-series-type")).toBe("");
    expect(chart.getAttribute("data-series-count")).toBe("0");
  });
});
