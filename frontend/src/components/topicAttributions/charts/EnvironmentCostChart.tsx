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

function buildGroupedBarData(
  buckets: TopicAttributionAggregationBucket[],
  topN: number = 10,
): {
  topics: string[];
  series: { name: string; type: "bar"; data: number[] }[];
} {
  const topicTotals: Record<string, number> = {};
  for (const b of buckets) {
    const t = b.dimensions.topic_name ?? "Unknown";
    topicTotals[t] = (topicTotals[t] ?? 0) + parseFloat(b.total_amount);
  }
  const topTopics = Object.entries(topicTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([t]) => t);
  const envIds = [
    ...new Set(buckets.map((b) => b.dimensions.env_id ?? "Unknown")),
  ];
  const dataMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const t = b.dimensions.topic_name ?? "Unknown";
    const e = b.dimensions.env_id ?? "Unknown";
    dataMap[t] ??= {};
    dataMap[t][e] = (dataMap[t][e] ?? 0) + parseFloat(b.total_amount);
  }
  return {
    topics: topTopics,
    series: envIds.map((env) => ({
      name: env,
      type: "bar" as const,
      data: topTopics.map((t) => dataMap[t]?.[env] ?? 0),
    })),
  };
}

export function EnvironmentCostChart({
  data,
  height = 300,
}: EnvironmentCostChartProps): React.JSX.Element {
  const { topics, series } = useMemo(() => buildGroupedBarData(data), [data]);

  const option: EChartsOption = {
    tooltip: { trigger: "axis" },
    legend: { bottom: 0 },
    xAxis: {
      type: "category",
      data: topics,
      axisLabel: { rotate: 45, hideOverlap: true },
    },
    yAxis: {
      type: "value",
      axisLabel: { formatter: (v: number) => formatCurrency(v) },
    },
    series,
  };

  return <ReactECharts option={option} style={{ height }} />;
}
