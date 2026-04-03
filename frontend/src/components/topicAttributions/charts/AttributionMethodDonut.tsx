import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { aggregateByDimension } from "../../../utils/aggregation";

interface AttributionMethodDonutProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

export function AttributionMethodDonut({
  data,
  height = 300,
}: AttributionMethodDonutProps): React.JSX.Element {
  const slices = useMemo(
    () => aggregateByDimension(data, "attribution_method"),
    [data],
  );

  const option: EChartsOption = {
    tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
    legend: { orient: "horizontal", bottom: 0 },
    series: [
      {
        type: "pie",
        radius: ["35%", "65%"],
        data: slices.map((d) => ({ name: d.key, value: d.amount })),
        label: { show: false },
        emphasis: { label: { show: true, formatter: "{b}: {d}%" } },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height }} />;
}
