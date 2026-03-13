import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { CostByProductChart } from "./CostByProductChart";

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

vi.mock("./DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(
    ({ dimension }: { dimension: string; data: unknown[] }) => (
      <div data-testid="dimension-pie-chart" data-dimension={dimension} />
    ),
  ),
}));

function makeBucket(productType: string, amount: string): AggregationBucket {
  return {
    dimensions: { product_type: productType },
    time_bucket: "2026-02-01",
    total_amount: amount,
    usage_amount: amount,
    shared_amount: "0.00",
    row_count: 1,
  };
}

describe("CostByProductChart", () => {
  it("delegates to DimensionPieChart in pie mode", () => {
    const data = [makeBucket("kafka", "100.00"), makeBucket("connect", "50.00")];
    render(<CostByProductChart data={data} />);
    expect(screen.getByTestId("dimension-pie-chart")).toBeInTheDocument();
    expect(screen.queryByTestId("echarts")).toBeNull();
  });

  it("renders treemap via ReactECharts directly when chartType='treemap'", () => {
    const data = [makeBucket("kafka", "100.00")];
    render(<CostByProductChart data={data} chartType="treemap" />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("treemap");
    expect(screen.queryByTestId("dimension-pie-chart")).toBeNull();
  });

  it("delegates to DimensionPieChart for empty data in pie mode", () => {
    render(<CostByProductChart data={[]} />);
    expect(screen.getByTestId("dimension-pie-chart")).toBeInTheDocument();
    expect(screen.queryByTestId("echarts")).toBeNull();
  });

  it("renders empty state for treemap with no data", () => {
    render(<CostByProductChart data={[]} chartType="treemap" />);
    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-has-graphic")).toBe("true");
    expect(chart.getAttribute("data-series-type")).toBe("");
  });
});
