import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EnvironmentCostChart } from "./EnvironmentCostChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
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
