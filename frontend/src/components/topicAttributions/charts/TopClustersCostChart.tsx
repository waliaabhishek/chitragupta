import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";
import { buildTopClustersCostData } from "./clusterUtils";

interface TopClustersCostChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

export function TopClustersCostChart({
  data,
  height = 300,
}: TopClustersCostChartProps): React.JSX.Element {
  const { clusters, series } = useMemo(
    () => buildTopClustersCostData(data),
    [data],
  );

  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: unknown) => {
        const list = params as { seriesName: string; value: number; marker: string }[];
        const nonZero = list.filter((p) => p.value > 0);
        if (!nonZero.length) return "";
        const header = (list[0] as { axisValueLabel?: string }).axisValueLabel ?? "";
        return [
          `<b>${header}</b>`,
          ...nonZero.map((p) => `${p.marker} ${p.seriesName}: ${formatCurrency(p.value)}`),
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
      data: clusters,
      axisLabel: { width: 120, overflow: "truncate" },
    },
    series,
  };

  return <ReactECharts option={option} notMerge style={{ height }} />;
}
