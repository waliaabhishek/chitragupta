import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { BucketLike } from "../../utils/aggregation";
import { TagPivotChart } from "./TagPivotChart";

vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
    }: {
      option: {
        xAxis?: { data?: string[] };
        series?: {
          data?: { value: number; itemStyle?: { color: string } }[];
        }[];
      };
      style?: object;
      onEvents?: object;
    }) => (
      <div
        data-testid="echarts"
        data-xaxis={JSON.stringify(option.xAxis?.data ?? [])}
        data-series={JSON.stringify(option.series ?? [])}
      />
    ),
  ),
}));

function makeBucket(
  tagOwner: string,
  productType: string,
  amount: string,
): BucketLike {
  return {
    dimensions: { "tag:owner": tagOwner, product_type: productType },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("TagPivotChart", () => {
  it("renders alice bar (colored) and UNTAGGED bar (gray), UNTAGGED last on x-axis", () => {
    const buckets = [
      makeBucket("alice", "KAFKA_STORAGE", "100"),
      makeBucket("UNTAGGED", "KAFKA_STORAGE", "50"),
    ];
    render(<TagPivotChart buckets={buckets} tagDimension="tag:owner" />);

    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(
      chart.getAttribute("data-xaxis") ?? "[]",
    ) as string[];

    expect(xAxis).toContain("alice");
    expect(xAxis).toContain("UNTAGGED");
    expect(xAxis[xAxis.length - 1]).toBe("UNTAGGED");

    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as {
      data?: { value: number; itemStyle?: { color: string } }[];
    }[];
    expect(series.length).toBeGreaterThan(0);

    const kafkaStorageSeries = series[0];
    const untaggedDataPoint = kafkaStorageSeries.data?.find(
      (_, i) => xAxis[i] === "UNTAGGED",
    );
    expect(untaggedDataPoint?.itemStyle?.color).toBe("#d9d9d9");
  });

  it("renders single gray bar when all buckets are UNTAGGED", () => {
    const buckets = [
      makeBucket("UNTAGGED", "KAFKA_STORAGE", "50"),
      makeBucket("UNTAGGED", "KAFKA_NETWORK", "25"),
    ];
    render(<TagPivotChart buckets={buckets} tagDimension="tag:owner" />);

    const chart = screen.getByTestId("echarts");
    const xAxis = JSON.parse(
      chart.getAttribute("data-xaxis") ?? "[]",
    ) as string[];

    expect(xAxis).toEqual(["UNTAGGED"]);
  });

  it("renders no series when buckets array is empty", () => {
    render(<TagPivotChart buckets={[]} tagDimension="tag:owner" />);

    const chart = screen.getByTestId("echarts");
    const series = JSON.parse(
      chart.getAttribute("data-series") ?? "[]",
    ) as unknown[];

    expect(series).toHaveLength(0);
  });
});
