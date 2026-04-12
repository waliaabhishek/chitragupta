import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type {
  EChartsOption,
  DefaultLabelFormatterCallbackParams,
} from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";

interface CostVelocityChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

function buildVelocityData(
  buckets: TopicAttributionAggregationBucket[],
  topN: number = 10,
): { times: string[]; series: { name: string; deltas: number[] }[] } {
  const topicTimeMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const topic = b.dimensions.topic_name ?? "Unknown";
    topicTimeMap[topic] ??= {};
    topicTimeMap[topic][b.time_bucket] =
      (topicTimeMap[topic][b.time_bucket] ?? 0) + parseFloat(b.total_amount);
  }
  const times = [...new Set(buckets.map((b) => b.time_bucket))].sort();
  const topicMaxDelta: Record<string, number> = {};
  for (const [topic, timeMap] of Object.entries(topicTimeMap)) {
    let maxDelta = 0;
    for (let i = 1; i < times.length; i++) {
      const delta = Math.abs(
        (timeMap[times[i]] ?? 0) - (timeMap[times[i - 1]] ?? 0),
      );
      if (delta > maxDelta) maxDelta = delta;
    }
    topicMaxDelta[topic] = maxDelta;
  }
  const topTopics = Object.entries(topicMaxDelta)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([topic]) => topic);
  const deltaLabels = times.slice(1);
  return {
    times: deltaLabels,
    series: topTopics.map((topic) => ({
      name: topic,
      deltas: deltaLabels.map(
        (t, i) =>
          (topicTimeMap[topic][t] ?? 0) - (topicTimeMap[topic][times[i]] ?? 0),
      ),
    })),
  };
}

function formatDelta(value: number): string {
  if (value === 0) return `${formatCurrency(0)} no change`;
  if (value > 0) return `+${formatCurrency(value)} increase`;
  return `-${formatCurrency(Math.abs(value))} decrease`;
}

export function CostVelocityChart({
  data,
  height = 300,
}: CostVelocityChartProps): React.JSX.Element {
  const { times, series } = useMemo(() => buildVelocityData(data), [data]);

  const option: EChartsOption = useMemo(
    () => ({
      tooltip: {
        trigger: "axis",
        formatter: (
          params:
            | DefaultLabelFormatterCallbackParams
            | DefaultLabelFormatterCallbackParams[],
        ) => {
          const items = Array.isArray(params) ? params : [params];
          if (items.length === 0) return "";
          const lines = items.map(
            (p) =>
              `${p.marker ?? ""} ${p.seriesName ?? ""}: ${formatDelta(p.value as number)}`,
          );
          const axisValue =
            (items[0] as { axisValue?: string }).axisValue ?? "";
          return `<strong>${axisValue}</strong><br/>${lines.join("<br/>")}`;
        },
      },
      legend: { bottom: 0 },
      xAxis: { type: "category", data: times },
      yAxis: {
        type: "value",
        axisLabel: { formatter: (v: number) => formatCurrency(v) },
      },
      series: series.map((s) => ({
        name: s.name,
        type: "line",
        data: s.deltas,
        smooth: true,
      })),
    }),
    [times, series],
  );

  return <ReactECharts option={option} notMerge style={{ height }} />;
}
