import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

interface ProductTypeMixChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

function buildNormalizedAreaData(
  buckets: TopicAttributionAggregationBucket[],
): {
  times: string[];
  series: {
    name: string;
    type: "line";
    stack: string;
    areaStyle: Record<string, unknown>;
    data: number[];
  }[];
} {
  const times = [...new Set(buckets.map((b) => b.time_bucket))].sort();
  const productTypes = [
    ...new Set(buckets.map((b) => b.dimensions.product_type ?? "Unknown")),
  ];
  const ptTimeMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const pt = b.dimensions.product_type ?? "Unknown";
    ptTimeMap[pt] ??= {};
    ptTimeMap[pt][b.time_bucket] =
      (ptTimeMap[pt][b.time_bucket] ?? 0) + parseFloat(b.total_amount);
  }
  const timeTotals: Record<string, number> = {};
  for (const t of times) {
    timeTotals[t] = productTypes.reduce(
      (s, pt) => s + (ptTimeMap[pt]?.[t] ?? 0),
      0,
    );
  }
  return {
    times,
    series: productTypes.map((pt) => ({
      name: pt,
      type: "line" as const,
      stack: "total",
      areaStyle: {},
      data: times.map((t) => {
        const total = timeTotals[t];
        return total > 0 ? ((ptTimeMap[pt]?.[t] ?? 0) / total) * 100 : 0;
      }),
    })),
  };
}

export function ProductTypeMixChart({ data, height = 300 }: ProductTypeMixChartProps): React.JSX.Element {
  const { times, series } = useMemo(() => buildNormalizedAreaData(data), [data]);

  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      valueFormatter: (v) => `${Number(v).toFixed(1)}%`,
    },
    legend: { bottom: 0 },
    xAxis: {
      type: "category",
      data: times,
      axisLabel: { rotate: 45, hideOverlap: true },
    },
    yAxis: {
      type: "value",
      max: 100,
      axisLabel: { formatter: (v: number) => `${v}%` },
    },
    series,
  };

  return <ReactECharts option={option} style={{ height }} />;
}
