import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ZombieTopicsTable } from "./ZombieTopicsTable";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

// Mock AG Grid
vi.mock("ag-grid-react", () => ({
  AgGridReact: ({
    rowData,
    columnDefs,
  }: {
    rowData: Array<Record<string, unknown>>;
    columnDefs: Array<{ field?: string; headerName?: string }>;
  }) => (
    <div data-testid="ag-grid">
      <div data-testid="row-count" data-value={rowData?.length ?? 0} />
      <div data-testid="columns">
        {columnDefs?.map((c) => (
          <span key={c.field ?? c.headerName} data-testid={`col-${c.field}`}>
            {c.headerName}
          </span>
        ))}
      </div>
      {rowData?.map((row, i) => (
        <div
          key={i}
          data-testid={`row-${i}`}
          data-topic={String(row.topic_name)}
        />
      ))}
    </div>
  ),
}));

// Mock antd Empty
vi.mock("antd", () => ({
  Empty: () => <div data-testid="empty-state" />,
}));

function makeBucket(
  topicName: string,
  productType: string,
  amount: string,
): TopicAttributionAggregationBucket {
  return {
    dimensions: { topic_name: topicName, product_type: productType },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("ZombieTopicsTable", () => {
  it("renders Empty component when no zombie topics exist", () => {
    // All topics have network activity — not zombies
    const data = [
      makeBucket("active-topic", "KAFKA_STORAGE", "10.00"),
      makeBucket("active-topic", "KAFKA_NETWORK_READ", "5.00"),
      makeBucket("active-topic", "KAFKA_NETWORK_WRITE", "3.00"),
    ];
    render(<ZombieTopicsTable data={data} />);
    expect(screen.getByTestId("empty-state")).toBeTruthy();
  });

  it("filters to only topics with storage > 0 and total_network < 0.01", () => {
    const data = [
      // zombie: storage but no network
      makeBucket("zombie-topic", "KAFKA_STORAGE", "15.00"),
      makeBucket("zombie-topic", "KAFKA_NETWORK_READ", "0.00"),
      // active: has network
      makeBucket("active-topic", "KAFKA_STORAGE", "10.00"),
      makeBucket("active-topic", "KAFKA_NETWORK_READ", "5.00"),
      makeBucket("active-topic", "KAFKA_NETWORK_WRITE", "3.00"),
    ];
    render(<ZombieTopicsTable data={data} />);

    expect(screen.queryByTestId("empty-state")).toBeNull();
    // Only zombie-topic should appear
    const grid = screen.getByTestId("ag-grid");
    expect(grid).toBeTruthy();
    expect(screen.getByTestId("row-0").getAttribute("data-topic")).toBe(
      "zombie-topic",
    );
  });

  it("shows Empty when data array is empty", () => {
    render(<ZombieTopicsTable data={[]} />);
    expect(screen.getByTestId("empty-state")).toBeTruthy();
  });

  it("renders expected columns", () => {
    const data = [makeBucket("zombie", "KAFKA_STORAGE", "10.00")];
    render(<ZombieTopicsTable data={data} />);
    // Should have topic_name, kafka_storage, kafka_network_read, kafka_network_write, total_network columns
    expect(screen.getByTestId("col-topic_name")).toBeTruthy();
    expect(screen.getByTestId("col-kafka_storage")).toBeTruthy();
    expect(screen.getByTestId("col-total_network")).toBeTruthy();
  });
});
