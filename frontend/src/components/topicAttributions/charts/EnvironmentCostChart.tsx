import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";

interface EnvironmentCostChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

function buildEnvCostData(
  buckets: TopicAttributionAggregationBucket[],
  topNEnvs: number = 10,
  topNTopicsPerEnv: number = 5,
): {
  envs: string[];
  series: { name: string; type: "bar"; stack: string; data: number[] }[];
} {
  const envTopic: Record<string, Record<string, number>> = {};
  const envTotals: Record<string, number> = {};
  for (const b of buckets) {
    const e = b.dimensions.env_id ?? "Unknown";
    const t = b.dimensions.topic_name ?? "Unknown";
    const amt = parseFloat(b.total_amount);
    envTopic[e] ??= {};
    envTopic[e][t] = (envTopic[e][t] ?? 0) + amt;
    envTotals[e] = (envTotals[e] ?? 0) + amt;
  }

  // Top N envs by total cost; ascending so highest renders at top of horizontal chart
  const envs = Object.entries(envTotals)
    .sort((a, b) => a[1] - b[1])
    .slice(-topNEnvs)
    .map(([e]) => e);

  // Collect the union of per-env top topics so each env shows its own top contributors
  const allTopTopics = new Set<string>();
  for (const env of envs) {
    const topics = envTopic[env] ?? {};
    const sorted = Object.entries(topics)
      .sort((a, b) => b[1] - a[1])
      .slice(0, topNTopicsPerEnv);
    for (const [t] of sorted) {
      allTopTopics.add(t);
    }
  }
  const topicList = [...allTopTopics];

  return {
    envs,
    series: [
      ...topicList.map((topic) => ({
        name: topic,
        type: "bar" as const,
        stack: "total",
        data: envs.map((e) => envTopic[e]?.[topic] ?? 0),
      })),
      {
        name: "Other",
        type: "bar" as const,
        stack: "total",
        data: envs.map((e) => {
          const topSum = topicList.reduce(
            (s, t) => s + (envTopic[e]?.[t] ?? 0),
            0,
          );
          return Math.max(0, (envTotals[e] ?? 0) - topSum);
        }),
      },
    ],
  };
}

export function EnvironmentCostChart({
  data,
  height = 300,
}: EnvironmentCostChartProps): React.JSX.Element {
  const { envs, series } = useMemo(() => buildEnvCostData(data), [data]);

  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: unknown) => {
        const list = params as {
          seriesName: string;
          value: number;
          marker: string;
        }[];
        const nonZero = list.filter((p) => p.value > 0);
        if (!nonZero.length) return "";
        const header =
          (list[0] as { axisValueLabel?: string }).axisValueLabel ?? "";
        return [
          `<b>${header}</b>`,
          ...nonZero.map(
            (p) => `${p.marker} ${p.seriesName}: ${formatCurrency(p.value)}`,
          ),
        ].join("<br/>");
      },
    },
    legend: { bottom: 0, type: "scroll" },
    grid: { left: "25%", right: "5%", containLabel: false },
    xAxis: {
      type: "value",
      axisLabel: { formatter: (v: number) => formatCurrency(v) },
    },
    yAxis: {
      type: "category",
      data: envs,
      axisLabel: { width: 120, overflow: "truncate" },
    },
    series,
  };

  return <ReactECharts option={option} notMerge style={{ height }} />;
}
