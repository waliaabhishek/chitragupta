import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { AggregationBucket } from "../../types/api";
import { CostByIdentityChart } from "./CostByIdentityChart";

vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
    }: {
      option: { yAxis?: { data?: string[] }; series?: { data?: number[] }[] };
      style?: object;
      showLoading?: boolean;
    }) => (
      <div
        data-testid="echarts"
        data-yaxis={JSON.stringify(option.yAxis?.data ?? [])}
        data-series={JSON.stringify(option.series?.[0]?.data ?? [])}
      />
    ),
  ),
}));

function makeBucket(identityId: string, amount: string): AggregationBucket {
  return {
    dimensions: { identity_id: identityId },
    time_bucket: "2026-02-01",
    total_amount: amount,
    usage_amount: amount,
    shared_amount: "0.00",
    row_count: 1,
  };
}

describe("CostByIdentityChart", () => {
  it("renders top N identities sorted by amount descending", () => {
    const data = [
      makeBucket("user-cheap", "1.00"),
      makeBucket("user-expensive", "100.00"),
      makeBucket("user-mid", "50.00"),
    ];
    render(<CostByIdentityChart data={data} topN={3} />);
    const chart = screen.getByTestId("echarts");
    const yAxis = JSON.parse(
      chart.getAttribute("data-yaxis") ?? "[]",
    ) as string[];
    // Reversed so highest is at top (last in yAxis array for horizontal bar)
    expect(yAxis[yAxis.length - 1]).toBe("user-expensive");
  });

  it("limits to topN identities", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket(`user-${i}`, String(i + 1)),
    );
    render(<CostByIdentityChart data={data} topN={5} />);
    const chart = screen.getByTestId("echarts");
    const yAxis = JSON.parse(
      chart.getAttribute("data-yaxis") ?? "[]",
    ) as string[];
    expect(yAxis).toHaveLength(5);
  });

  it("renders empty state for empty data with no bars", () => {
    render(<CostByIdentityChart data={[]} />);
    const chart = screen.getByTestId("echarts");
    const yAxis = JSON.parse(
      chart.getAttribute("data-yaxis") ?? "[]",
    ) as string[];
    const series = JSON.parse(
      chart.getAttribute("data-series") ?? "[]",
    ) as number[];
    expect(yAxis).toHaveLength(0);
    expect(series).toHaveLength(0);
  });
});
