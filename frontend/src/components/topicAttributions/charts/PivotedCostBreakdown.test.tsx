import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { PivotedCostBreakdown } from "./PivotedCostBreakdown";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

type CapturedRow = {
  topic_name: string;
  write: number;
  write_pct: number;
  read: number;
  read_pct: number;
  storage: number;
  storage_pct: number;
  other: number;
  other_pct: number;
  total: number;
};

let capturedRowData: CapturedRow[] = [];
let capturedColDefs: Array<{
  field?: string;
  headerName?: string;
  pinned?: string;
  sort?: string;
}> = [];

vi.mock("ag-grid-react", () => ({
  AgGridReact: ({
    rowData,
    columnDefs,
  }: {
    rowData: CapturedRow[];
    columnDefs: Array<{
      field?: string;
      headerName?: string;
      pinned?: string;
      sort?: string;
    }>;
  }) => {
    capturedRowData = rowData ?? [];
    capturedColDefs = columnDefs ?? [];
    return <div data-testid="ag-grid" data-row-count={rowData?.length ?? 0} />;
  },
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

describe("PivotedCostBreakdown", () => {
  it("renders AG Grid", () => {
    render(<PivotedCostBreakdown data={[]} />);
    expect(screen.getByTestId("ag-grid")).toBeTruthy();
  });

  it("has fixed column definitions with expected fields", () => {
    render(<PivotedCostBreakdown data={[]} />);
    const fields = capturedColDefs.map((c) => c.field);
    expect(fields).toContain("topic_name");
    expect(fields).toContain("total");
    expect(fields).toContain("write");
    expect(fields).toContain("write_pct");
    expect(fields).toContain("read");
    expect(fields).toContain("read_pct");
    expect(fields).toContain("storage");
    expect(fields).toContain("storage_pct");
    expect(fields).toContain("other");
    expect(fields).toContain("other_pct");
  });

  it("topic_name column is pinned left", () => {
    render(<PivotedCostBreakdown data={[]} />);
    const topicCol = capturedColDefs.find((c) => c.field === "topic_name");
    expect(topicCol?.pinned).toBe("left");
  });

  it("total column has sort=desc", () => {
    render(<PivotedCostBreakdown data={[]} />);
    const totalCol = capturedColDefs.find((c) => c.field === "total");
    expect(totalCol?.sort).toBe("desc");
  });

  it("percentage columns sum to 100 for each row", () => {
    const data = [
      makeBucket("topic-a", "KAFKA_NETWORK_WRITE", "30.00"),
      makeBucket("topic-a", "KAFKA_NETWORK_READ", "20.00"),
      makeBucket("topic-a", "KAFKA_STORAGE", "40.00"),
      makeBucket("topic-a", "KAFKA_CONNECT", "10.00"), // other
    ];
    render(<PivotedCostBreakdown data={data} />);

    expect(capturedRowData).toHaveLength(1);
    const row = capturedRowData[0];
    const pctSum =
      row.write_pct + row.read_pct + row.storage_pct + row.other_pct;
    expect(pctSum).toBeCloseTo(100, 1);
  });

  it("correctly maps KAFKA_NETWORK_WRITE to write column", () => {
    const data = [
      makeBucket("topic-a", "KAFKA_NETWORK_WRITE", "60.00"),
      makeBucket("topic-a", "KAFKA_NETWORK_READ", "40.00"),
    ];
    render(<PivotedCostBreakdown data={data} />);

    const row = capturedRowData[0];
    expect(row.write).toBeCloseTo(60, 2);
    expect(row.read).toBeCloseTo(40, 2);
    expect(row.storage).toBe(0);
  });

  it("sorts rows by total descending", () => {
    const data = [
      makeBucket("cheap-topic", "KAFKA_STORAGE", "10.00"),
      makeBucket("expensive-topic", "KAFKA_STORAGE", "100.00"),
    ];
    render(<PivotedCostBreakdown data={data} />);

    expect(capturedRowData[0].topic_name).toBe("expensive-topic");
    expect(capturedRowData[1].topic_name).toBe("cheap-topic");
  });
});
