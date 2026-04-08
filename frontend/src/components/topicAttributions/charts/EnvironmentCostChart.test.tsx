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
    render(
      <EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />,
    );
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("bar");
  });

  it("stacks topics per environment (one series per union top topic + Other)", () => {
    const data = [
      makeBucket("topic-a", "env-prod", "100.00"),
      makeBucket("topic-b", "env-prod", "80.00"),
      makeBucket("topic-a", "env-staging", "50.00"),
    ];
    render(<EnvironmentCostChart data={data} />);
    const chart = screen.getByTestId("echarts");
    const names = (chart.getAttribute("data-series-names") ?? "")
      .split(",")
      .filter(Boolean);
    // Topics become stacked series (union of per-env top-5), with trailing "Other".
    expect(names).toContain("topic-a");
    expect(names).toContain("topic-b");
    expect(names[names.length - 1]).toBe("Other");
  });

  it("yAxis contains unique env_id values as categories", () => {
    const data = [
      makeBucket("topic-a", "env-prod", "100.00"),
      makeBucket("topic-a", "env-staging", "50.00"),
    ];
    render(<EnvironmentCostChart data={data} />);
    type YAxis = { type?: string; data?: string[] };
    const yAxis = lastOption.yAxis as YAxis;
    expect(yAxis.type).toBe("category");
    expect(yAxis.data).toContain("env-prod");
    expect(yAxis.data).toContain("env-staging");
  });

  it("limits yAxis to top 10 envs by total cost", () => {
    const data = Array.from({ length: 15 }, (_, i) =>
      makeBucket("topic-a", `env-${i}`, String((i + 1) * 10)),
    );
    render(<EnvironmentCostChart data={data} />);
    type YAxis = { data?: string[] };
    const yAxis = lastOption.yAxis as YAxis;
    expect(yAxis.data?.length).toBe(10);
  });

  it("renders without crashing when data is empty", () => {
    render(<EnvironmentCostChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("default height is 300", () => {
    render(<EnvironmentCostChart data={[]} />);
    expect(lastStyle.height).toBe(300);
  });

  it("custom height accepted", () => {
    render(<EnvironmentCostChart data={[]} height={400} />);
    expect(lastStyle.height).toBe(400);
  });
});

describe("EnvironmentCostChart axis configuration", () => {
  it("xAxis is numeric with currency formatter", () => {
    render(
      <EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />,
    );
    type XAxis = {
      type?: string;
      axisLabel?: { formatter?: (v: number) => string };
    };
    const xAxis = lastOption.xAxis as XAxis;
    expect(xAxis.type).toBe("value");
    const formatter = xAxis.axisLabel?.formatter;
    expect(typeof formatter).toBe("function");
    expect(formatter!(1000)).toBe("$1,000.00");
    expect(formatter!(1996.9649999999929)).toBe("$1,996.96");
  });

  it("yAxis truncates overflowing env labels", () => {
    render(<EnvironmentCostChart data={[]} />);
    type YAxis = { axisLabel?: { width?: number; overflow?: string } };
    const yAxis = lastOption.yAxis as YAxis;
    expect(yAxis.axisLabel?.width).toBe(120);
    expect(yAxis.axisLabel?.overflow).toBe("truncate");
  });
});

describe("EnvironmentCostChart tooltip", () => {
  it("uses axis trigger with shadow pointer", () => {
    render(<EnvironmentCostChart data={[]} />);
    type Tooltip = { trigger?: string; axisPointer?: { type?: string } };
    const tooltip = lastOption.tooltip as Tooltip;
    expect(tooltip.trigger).toBe("axis");
    expect(tooltip.axisPointer?.type).toBe("shadow");
  });

  it("formatter renders non-zero series with currency values and env header", () => {
    render(
      <EnvironmentCostChart data={[makeBucket("topic-a", "env-1", "100.00")]} />,
    );
    type Tooltip = { formatter?: (params: unknown) => string };
    const tooltip = lastOption.tooltip as Tooltip;
    expect(typeof tooltip.formatter).toBe("function");
    const html = tooltip.formatter!([
      {
        seriesName: "topic-a",
        value: 1996.9649999999929,
        marker: "•",
        axisValueLabel: "env-1",
      },
      { seriesName: "topic-b", value: 0, marker: "•" },
    ]);
    expect(html).toContain("<b>env-1</b>");
    expect(html).toContain("topic-a");
    expect(html).toContain("$1,996.96");
    // Zero-value series are filtered out of the tooltip.
    expect(html).not.toContain("topic-b");
  });

  it("formatter returns empty string when all values are zero", () => {
    render(<EnvironmentCostChart data={[]} />);
    type Tooltip = { formatter?: (params: unknown) => string };
    const tooltip = lastOption.tooltip as Tooltip;
    const html = tooltip.formatter!([
      { seriesName: "topic-a", value: 0, marker: "•" },
    ]);
    expect(html).toBe("");
  });
});
