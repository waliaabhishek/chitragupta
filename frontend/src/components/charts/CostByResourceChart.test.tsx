import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { CostByResourceChart } from "./CostByResourceChart";

vi.mock("./DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(
    ({ dimension, data }: { dimension: string; data: unknown[] }) => (
      <div
        data-testid="dimension-pie-chart"
        data-dimension={dimension}
        data-count={data.length}
      />
    ),
  ),
}));

function makeBucket(resourceId: string, amount: string): AggregationBucket {
  return {
    dimensions: { resource_id: resourceId },
    time_bucket: "2026-02-01",
    total_amount: amount,
    usage_amount: amount,
    shared_amount: "0.00",
    row_count: 1,
  };
}

describe("CostByResourceChart", () => {
  it("renders DimensionPieChart with resource_id dimension", () => {
    const data = [makeBucket("res-1", "100.00")];
    render(<CostByResourceChart data={data} />);
    const chart = screen.getByTestId("dimension-pie-chart");
    expect(chart.getAttribute("data-dimension")).toBe("resource_id");
  });

  it("does not render a Table", () => {
    const data = [makeBucket("res-1", "100.00")];
    render(<CostByResourceChart data={data} />);
    expect(screen.queryByRole("table")).toBeNull();
  });
});
