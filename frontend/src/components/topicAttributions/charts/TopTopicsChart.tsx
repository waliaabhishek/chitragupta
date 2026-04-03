import type React from "react";
import { useMemo, useState } from "react";
import { Radio } from "antd";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import {
  aggregateByDimension,
  topNWithOther,
  formatCurrency,
} from "../../../utils/aggregation";

interface TopTopicsChartProps {
  data: TopicAttributionAggregationBucket[];
  height?: number;
}

export function TopTopicsChart({
  data,
  height = 300,
}: TopTopicsChartProps): React.JSX.Element {
  const [chartType, setChartType] = useState<"Treemap" | "Bar">("Treemap");

  const items = useMemo(
    () => topNWithOther(aggregateByDimension(data, "topic_name"), 15),
    [data],
  );

  const treemapOption: EChartsOption = {
    tooltip: {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      formatter: (p: any) =>
        `${p.name as string}: ${formatCurrency(p.value as number)}`,
    },
    series: [
      {
        type: "treemap",
        data: items.map((d) => ({ name: d.key, value: d.amount })),
        label: { show: true, formatter: "{b}" },
        breadcrumb: { show: false },
        roam: false,
      },
    ],
  };

  const barOption: EChartsOption = {
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "value",
      axisLabel: { formatter: (v: number) => formatCurrency(v) },
    },
    yAxis: {
      type: "category",
      data: items.map((d) => d.key).reverse(),
      axisLabel: { width: 150, overflow: "truncate" },
    },
    series: [{ type: "bar", data: items.map((d) => d.amount).reverse() }],
  };

  const option = chartType === "Treemap" ? treemapOption : barOption;

  return (
    <div>
      <Radio.Group
        value={chartType}
        onChange={(e) => setChartType(e.target.value as "Treemap" | "Bar")}
        style={{ marginBottom: 8 }}
      >
        <Radio.Button value="Treemap">Treemap</Radio.Button>
        <Radio.Button value="Bar">Bar</Radio.Button>
      </Radio.Group>
      <ReactECharts option={option} style={{ height }} />
    </div>
  );
}
