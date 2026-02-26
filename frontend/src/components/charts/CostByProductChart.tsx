import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { AggregationBucket } from "../../types/api";
import { aggregateByDimension } from "../../utils/aggregation";

interface CostByProductChartProps {
  data: AggregationBucket[];
  chartType?: "pie" | "treemap";
  loading?: boolean;
  height?: number;
}

export function CostByProductChart({
  data,
  chartType = "pie",
  loading,
  height = 400,
}: CostByProductChartProps): JSX.Element {
  const option: EChartsOption = useMemo(() => {
    const byProduct = aggregateByDimension(data, "product_type");

    if (byProduct.length === 0) {
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

    if (chartType === "pie") {
      return {
        tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
        series: [
          {
            type: "pie",
            radius: ["40%", "70%"],
            data: byProduct.map((d) => ({ name: d.key, value: d.amount })),
            label: { show: true, formatter: "{b}: {d}%" },
          },
        ],
      };
    }

    return {
      tooltip: { formatter: "{b}: {c}" },
      series: [
        {
          type: "treemap",
          data: byProduct.map((d) => ({ name: d.key, value: d.amount })),
        },
      ],
    };
  }, [data, chartType]);

  return <ReactECharts option={option} style={{ height }} showLoading={loading} />;
}
