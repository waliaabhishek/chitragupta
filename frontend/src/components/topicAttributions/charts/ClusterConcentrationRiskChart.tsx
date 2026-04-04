import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import {
  buildConcentrationRiskData,
  riskColor,
  formatRiskTooltip,
} from "./clusterUtils";

interface ClusterConcentrationRiskChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

export function ClusterConcentrationRiskChart({
  data,
  height = 300,
}: ClusterConcentrationRiskChartProps): React.JSX.Element {
  const { risks, clusters, percentages, colors } = useMemo(() => {
    const risks = buildConcentrationRiskData(data);
    const clusters = risks.map((r) => r.clusterId);
    const percentages = risks.map((r) =>
      parseFloat((r.ratio * 100).toFixed(1)),
    );
    const colors = risks.map((r) => riskColor(r.ratio));
    return { risks, clusters, percentages, colors };
  }, [data]);

  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: unknown) =>
        formatRiskTooltip(
          risks,
          (params as Array<{ dataIndex: number }>)[0].dataIndex,
        ),
    },
    grid: { left: "25%", right: "5%", containLabel: false },
    xAxis: {
      type: "value",
      max: 100,
      axisLabel: { formatter: (v: number) => `${v}%` },
    },
    yAxis: {
      type: "category",
      data: clusters,
      axisLabel: { width: 120, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: percentages.map((value, i) => ({
          value,
          itemStyle: { color: colors[i] },
        })),
      },
    ],
  };

  return <ReactECharts option={option} style={{ height }} />;
}
