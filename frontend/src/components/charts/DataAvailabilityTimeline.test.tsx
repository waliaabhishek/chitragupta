import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { DataAvailabilityTimeline } from "./DataAvailabilityTimeline";

// echarts-for-react tries to render a canvas — mock it
vi.mock("echarts-for-react", () => ({
  default: vi.fn(
    ({
      option,
      showLoading,
    }: {
      option: {
        xAxis?: { min?: string; max?: string };
        series?: { data?: [number, number][] }[];
        graphic?: unknown[];
      };
      style?: object;
      showLoading?: boolean;
    }) => (
      <div
        data-testid="echarts"
        data-loading={String(showLoading ?? false)}
        data-xaxis-min={option.xAxis?.min ?? ""}
        data-xaxis-max={option.xAxis?.max ?? ""}
        data-series={JSON.stringify(option.series?.[0]?.data ?? [])}
        data-has-graphic={String((option.graphic?.length ?? 0) > 0)}
      />
    ),
  ),
}));

const BASE_PROPS = {
  dates: ["2026-01-15", "2026-01-17"],
  startDate: "2026-01-14",
  endDate: "2026-01-18",
};

describe("DataAvailabilityTimeline", () => {
  it("renders one scatter point per date in API response", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} />);

    const chart = screen.getByTestId("echarts");
    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as [
      number,
      number,
    ][];

    expect(series).toHaveLength(2);
  });

  it("2026-01-16 is not in scatter series (visible gap in x-axis)", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} />);

    const chart = screen.getByTestId("echarts");
    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as [
      number,
      number,
    ][];

    // The x-axis values are timestamps; verify only 2 points exist (no 2026-01-16)
    expect(series).toHaveLength(2);
    const timestamps = series.map(([x]) =>
      new Date(x).toISOString().slice(0, 10),
    );
    expect(timestamps).not.toContain("2026-01-16");
    expect(timestamps).toContain("2026-01-15");
    expect(timestamps).toContain("2026-01-17");
  });

  it("x-axis is bounded to exactly the filter window", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} />);

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-xaxis-min")).toBe("2026-01-14");
    expect(chart.getAttribute("data-xaxis-max")).toBe("2026-01-18");
  });

  it("dates outside filter range are excluded from scatter series", () => {
    render(
      <DataAvailabilityTimeline
        {...BASE_PROPS}
        dates={["2026-01-10", "2026-01-15", "2026-01-17", "2026-01-25"]}
      />,
    );

    const chart = screen.getByTestId("echarts");
    const series = JSON.parse(chart.getAttribute("data-series") ?? "[]") as [
      number,
      number,
    ][];

    // Only 2026-01-15 and 2026-01-17 are within [2026-01-14, 2026-01-18]
    expect(series).toHaveLength(2);
    const timestamps = series.map(([x]) =>
      new Date(x).toISOString().slice(0, 10),
    );
    expect(timestamps).not.toContain("2026-01-10");
    expect(timestamps).not.toContain("2026-01-25");
  });

  it("renders graphic overlay when dates is empty (no scatter series)", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} dates={[]} />);

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-has-graphic")).toBe("true");
    expect(chart.getAttribute("data-series")).toBe("[]");
  });

  it("passes showLoading=true to ReactECharts when loading prop is true", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} loading={true} />);

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-loading")).toBe("true");
  });

  it("passes showLoading=false to ReactECharts when loading prop is false", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} loading={false} />);

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-loading")).toBe("false");
  });

  it("renders chart even when loading (ChartCard handles the spinner overlay)", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} loading={true} />);

    expect(screen.getByTestId("echarts")).toBeInTheDocument();
  });

  it("renders chart element for all states (error handling is ChartCard's responsibility)", () => {
    render(<DataAvailabilityTimeline {...BASE_PROPS} />);
    expect(screen.getByTestId("echarts")).toBeInTheDocument();
  });
});
