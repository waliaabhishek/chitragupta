import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";

interface ClusterConcentrationChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

function buildClusterData(
  buckets: TopicAttributionAggregationBucket[],
  topN: number = 5,
): {
  clusters: string[];
  series: { name: string; type: "bar"; stack: string; data: number[] }[];
} {
  const clusterTopic: Record<string, Record<string, number>> = {};
  const clusterTotals: Record<string, number> = {};
  const globalTopicTotals: Record<string, number> = {};
  for (const b of buckets) {
    const c = b.dimensions.cluster_resource_id ?? "Unknown";
    const t = b.dimensions.topic_name ?? "Unknown";
    const amt = parseFloat(b.total_amount);
    clusterTopic[c] ??= {};
    clusterTopic[c][t] = (clusterTopic[c][t] ?? 0) + amt;
    clusterTotals[c] = (clusterTotals[c] ?? 0) + amt;
    globalTopicTotals[t] = (globalTopicTotals[t] ?? 0) + amt;
  }
  const clusters = Object.keys(clusterTopic);
  const topTopics = Object.entries(globalTopicTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([t]) => t);
  return {
    clusters,
    series: [
      ...topTopics.map((topic) => ({
        name: topic,
        type: "bar" as const,
        stack: "total",
        data: clusters.map((c) => clusterTopic[c]?.[topic] ?? 0),
      })),
      {
        name: "Other",
        type: "bar" as const,
        stack: "total",
        data: clusters.map((c) => {
          const topSum = topTopics.reduce(
            (s, t) => s + (clusterTopic[c]?.[t] ?? 0),
            0,
          );
          return (clusterTotals[c] ?? 0) - topSum;
        }),
      },
    ],
  };
}

export function ClusterConcentrationChart({
  data,
  height = 300,
}: ClusterConcentrationChartProps): React.JSX.Element {
  const { clusters, series } = useMemo(() => buildClusterData(data), [data]);

  const option: EChartsOption = {
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { bottom: 0 },
    xAxis: {
      type: "category",
      data: clusters,
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
