import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ClusterConcentrationRiskChart } from "./ClusterConcentrationRiskChart";
import {
  buildConcentrationRiskData,
  riskColor,
  formatRiskTooltip,
} from "./clusterUtils";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    const series = option.series as Array<{ type: string; name: string }>;
    return (
      <div
        data-testid="echarts"
        data-series-type={series?.[0]?.type ?? ""}
        data-series-count={String(series?.length ?? 0)}
      />
    );
  },
}));

function makeBucket(
  clusterId: string,
  topicName: string,
  amount: string,
): TopicAttributionAggregationBucket {
  return {
    dimensions: { cluster_resource_id: clusterId, topic_name: topicName },
    time_bucket: "2026-01-01",
    total_amount: amount,
    row_count: 1,
  };
}

describe("ClusterConcentrationRiskChart", () => {
  it("renders a single bar series", () => {
    const data = [makeBucket("lkc-abc", "topic-a", "100.00")];
    render(<ClusterConcentrationRiskChart data={data} />);
    const chart = screen.getByTestId("echarts");
    expect(Number(chart.getAttribute("data-series-count"))).toBe(1);
  });

  it("renders without crashing when data is empty", () => {
    render(<ClusterConcentrationRiskChart data={[]} />);
    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(Number(chart.getAttribute("data-series-count"))).toBe(1);
  });
});

describe("riskColor", () => {
  it("returns green for low concentration (0.3)", () => {
    expect(riskColor(0.3)).toBe("#52c41a");
  });

  it("returns yellow for medium concentration (0.6)", () => {
    expect(riskColor(0.6)).toBe("#faad14");
  });

  it("returns red for high concentration (0.8)", () => {
    expect(riskColor(0.8)).toBe("#f5222d");
  });

  it("returns yellow at exact boundary 0.5", () => {
    expect(riskColor(0.5)).toBe("#faad14");
  });

  it("returns red at exact boundary 0.75", () => {
    expect(riskColor(0.75)).toBe("#f5222d");
  });
});

describe("buildConcentrationRiskData", () => {
  it("cluster with one topic has ratio=1.0", () => {
    const data = [makeBucket("lkc-abc", "topic-a", "200.00")];
    const risks = buildConcentrationRiskData(data);
    expect(risks).toHaveLength(1);
    expect(risks[0].ratio).toBeCloseTo(1.0);
  });

  it("cluster with two equal-cost topics has ratio=0.5", () => {
    const data = [
      makeBucket("lkc-abc", "topic-a", "100.00"),
      makeBucket("lkc-abc", "topic-b", "100.00"),
    ];
    const risks = buildConcentrationRiskData(data);
    expect(risks).toHaveLength(1);
    expect(risks[0].ratio).toBeCloseTo(0.5);
  });

  it("top-15 filter: returns at most 15 clusters from a 20-cluster dataset", () => {
    const data = Array.from({ length: 20 }, (_, i) =>
      makeBucket(`lkc-cluster-${i}`, "topic-a", String((i + 1) * 10)),
    );
    const risks = buildConcentrationRiskData(data);
    expect(risks).toHaveLength(15);
  });
});

describe("formatRiskTooltip", () => {
  it("includes cluster ID, top topic, and concentration percentage", () => {
    const risks = buildConcentrationRiskData([
      makeBucket("lkc-abc", "topic-x", "300.00"),
      makeBucket("lkc-abc", "topic-y", "100.00"),
    ]);
    const html = formatRiskTooltip(risks, 0);
    expect(html).toContain("lkc-abc");
    expect(html).toContain("topic-x");
    expect(html).toContain("Concentration:");
    expect(html).toContain("75.0%");
  });
});
