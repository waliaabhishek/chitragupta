import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";

interface CostCompositionChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

function buildStackedBarData(buckets: TopicAttributionAggregationBucket[]): {
  productTypes: string[];
  times: string[];
  ptTimeMap: Record<string, Record<string, number>>;
} {
  const productTypes = [
    ...new Set(buckets.map((b) => b.dimensions.product_type ?? "Unknown")),
  ];
  const times = [...new Set(buckets.map((b) => b.time_bucket))].sort();
  const ptTimeMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const pt = b.dimensions.product_type ?? "Unknown";
    ptTimeMap[pt] ??= {};
    ptTimeMap[pt][b.time_bucket] =
      (ptTimeMap[pt][b.time_bucket] ?? 0) + parseFloat(b.total_amount);
  }
  return { productTypes, times, ptTimeMap };
}

export function CostCompositionChart({ data, height = 300 }: CostCompositionChartProps): React.JSX.Element {
  const { productTypes, times, ptTimeMap } = useMemo(
    () => buildStackedBarData(data),
    [data],
  );

  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (v: unknown) => formatCurrency(v as number),
    },
    legend: { bottom: 0 },
    xAxis: {
      type: "category",
      data: times,
      axisLabel: { rotate: 45, hideOverlap: true },
    },
    yAxis: {
      type: "value",
      axisLabel: { formatter: (v: number) => formatCurrency(v) },
    },
    series: productTypes.map((pt) => ({
      name: pt,
      type: "bar",
      stack: "total",
      data: times.map((t) => ptTimeMap[pt]?.[t] ?? 0),
    })),
  };

  return <ReactECharts option={option} style={{ height }} />;
}
