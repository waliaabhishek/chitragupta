import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EnvironmentCostChart } from "./EnvironmentCostChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

let lastOption: Record<string, unknown> = {};
let lastStyle: Record<string, unknown> = {};

vi.mock("echarts-for-react", () => ({
  default: ({
    option,
    style,
  }: {
    option: Record<string, unknown>;
    style: Record<string, unknown>;
  }) => {
    lastOption = option;
    lastStyle = style ?? {};
    const series = option.series as Array<{ type: string; name: string }>;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-series-names={series?.map((s) => s.name).join(",")}
      />
    );
  },
}));

function makeBucket(
  topicName: string,
  envId: string,
  amount: string,
): TopicAttributionAggregationBucket {
  return {
    dimensions: { topic_name: topicName, env_id: envId },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("EnvironmentCostChart", () => {
  it("renders an ECharts bar chart", () => {
    const data = [
      makeBucket("topic-a", "env-1", "100.00"),
      makeBucket("topic-b", "env-2", "50.00"),
    ];
    render(<EnvironmentCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("bar");
  });

  it("groups series by env_id dimension", () => {
    const data = [
      makeBucket("topic-a", "env-prod", "100.00"),
      makeBucket("topic-a", "env-staging", "50.00"),
      makeBucket("topic-b", "env-prod", "80.00"),
    ];
    render(<EnvironmentCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    const seriesNames = chart.getAttribute("data-series-names") ?? "";
    expect(seriesNames).toContain("env-prod");
    expect(seriesNames).toContain("env-staging");
  });

  it("creates one series per unique env_id", () => {
    const data = [
      makeBucket("topic-a", "env-1", "100.00"),
      makeBucket("topic-b", "env-1", "60.00"),
      makeBucket("topic-a", "env-2", "40.00"),
    ];
    render(<EnvironmentCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    const seriesNames = chart.getAttribute("data-series-names") ?? "";
    // Two unique env_ids → two series
    expect(seriesNames.split(",").filter(Boolean).length).toBe(2);
  });

  it("renders without crashing when data is empty", () => {
    render(<EnvironmentCostChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });
});

describe("TASK-200: label overflow fix", () => {
  it("truncates long labels", () => {
    const longName = "this-is-a-very-long-topic-name-indeed";
    render(<EnvironmentCostChart data={[makeBucket(longName, "env-1", "10.00")]} />);
    type XAxis = { axisLabel?: { formatter?: (v: string) => string } };
    const xAxis = lastOption.xAxis as XAxis;
    const formatter = xAxis?.axisLabel?.formatter;
    expect(typeof formatter).toBe("function");
    expect(formatter!(longName)).toBe("this-is-a-very-long-…");
  });

  it("leaves short labels unchanged", () => {
    const shortName = "short-topic";
    render(<EnvironmentCostChart data={[makeBucket(shortName, "env-1", "10.00")]} />);
    type XAxis = { axisLabel?: { formatter?: (v: string) => string } };
    const xAxis = lastOption.xAxis as XAxis;
    const formatter = xAxis?.axisLabel?.formatter;
    expect(typeof formatter).toBe("function");
    expect(formatter!(shortName)).toBe(shortName);
  });

  it("dataZoom slider configured", () => {
    render(<EnvironmentCostChart data={[]} />);
    const dataZoom = lastOption.dataZoom as Array<Record<string, unknown>>;
    expect(Array.isArray(dataZoom)).toBe(true);
    expect(dataZoom.length).toBeGreaterThan(0);
    expect(dataZoom[0].type).toBe("slider");
    expect(dataZoom[0].xAxisIndex).toBe(0);
  });

  it("dataZoom window shows 50%", () => {
    render(<EnvironmentCostChart data={[]} />);
    const dataZoom = lastOption.dataZoom as Array<Record<string, unknown>>;
    expect(dataZoom[0].start).toBe(0);
    expect(dataZoom[0].end).toBe(50);
  });

  it("default height is 350", () => {
    render(<EnvironmentCostChart data={[]} />);
    expect(lastStyle.height).toBe(350);
  });

  it("custom height accepted", () => {
    render(<EnvironmentCostChart data={[]} height={400} />);
    expect(lastStyle.height).toBe(400);
  });

  it("legend positioned above slider", () => {
    render(<EnvironmentCostChart data={[]} />);
    const legend = lastOption.legend as Record<string, unknown>;
    expect(legend?.bottom).toBe(45);
  });

  it("grid reserves space for slider and legend", () => {
    render(<EnvironmentCostChart data={[]} />);
    const grid = lastOption.grid as Record<string, unknown>;
    expect(grid?.bottom).toBe(80);
  });

  it("tooltip trigger unchanged", () => {
    render(<EnvironmentCostChart data={[]} />);
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    expect(tooltip?.trigger).toBe("axis");
  });
});

describe("TASK-203: tooltip valueFormatter", () => {
  it("EnvironmentCostChart — valueFormatter present", () => {
    render(<EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />);
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    expect(typeof tooltip?.valueFormatter).toBe("function");
  });

  it("EnvironmentCostChart — valueFormatter output", () => {
    render(<EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />);
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    const valueFormatter = tooltip?.valueFormatter as (v: number) => string;
    expect(valueFormatter(1996.9649999999929)).toBe("$1,996.96");
  });

  it("EnvironmentCostChart — axisLabel formatter unchanged", () => {
    render(<EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />);
    type YAxis = { axisLabel?: { formatter?: (v: number) => string } };
    const yAxis = lastOption.yAxis as YAxis;
    const formatter = yAxis?.axisLabel?.formatter;
    expect(typeof formatter).toBe("function");
    expect(formatter!(1000)).toBe("$1,000.00");
  });
});
