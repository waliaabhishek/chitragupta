import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { AggregationBucket } from "../../types/api";
import { aggregateByDimension, topNWithOther } from "../../utils/aggregation";

interface DimensionPieChartProps {
  data: AggregationBucket[];
  dimension: string;
  topN?: number;
  loading?: boolean;
  height?: number;
}

export function DimensionPieChart({
  data,
  dimension,
  topN = 10,
  loading,
  height = 300,
}: DimensionPieChartProps): React.JSX.Element {
  const option: EChartsOption = useMemo(() => {
    const slices = topNWithOther(aggregateByDimension(data, dimension), topN);

    if (slices.length === 0) {
      return {
        graphic: [
          {
            type: "text",
            left: "center",
            top: "middle",
            style: { text: "No data", fontSize: 14, fill: "#999" },
          },
        ],
      };
    }

    return {
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
  }, [data, dimension, topN]);

  return <ReactECharts option={option} style={{ height }} showLoading={loading} />;
}
