import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ReactNode } from "react";
import type { AggregationBucket } from "../../types/api";
import { CostByResourceChart } from "./CostByResourceChart";

// Mock antd table components
vi.mock("antd", () => ({
  Table: ({
    dataSource,
    loading,
  }: {
    dataSource: { resource_id: string; amount: number }[];
    loading?: boolean;
    columns?: unknown[];
    pagination?: boolean;
    size?: string;
    locale?: object;
  }) => (
    <div data-testid="resource-table" data-loading={String(loading ?? false)}>
      {dataSource.map((row, i) => (
        <div key={i} data-testid="resource-row">
          {row.resource_id}
        </div>
      ))}
    </div>
  ),
  Progress: ({ percent }: { percent: number; showInfo?: boolean; size?: string }) => (
    <div data-testid="progress" data-percent={percent} />
  ),
  Typography: {
    Text: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  },
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
  it("renders table with resource rows", () => {
    const data = [
      makeBucket("cluster-1", "100.00"),
      makeBucket("cluster-2", "50.00"),
    ];
    render(<CostByResourceChart data={data} />);
    expect(screen.getAllByTestId("resource-row")).toHaveLength(2);
    expect(screen.getByText("cluster-1")).toBeTruthy();
    expect(screen.getByText("cluster-2")).toBeTruthy();
  });

  it("limits to topN resources", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket(`cluster-${i}`, String((15 - i) * 10)),
    );
    render(<CostByResourceChart data={data} topN={5} />);
    expect(screen.getAllByTestId("resource-row")).toHaveLength(5);
  });

  it("shows loading state", () => {
    render(<CostByResourceChart data={[]} loading />);
    const table = screen.getByTestId("resource-table");
    expect(table.getAttribute("data-loading")).toBe("true");
  });

  it("handles empty data", () => {
    render(<CostByResourceChart data={[]} />);
    expect(screen.queryAllByTestId("resource-row")).toHaveLength(0);
  });
});
