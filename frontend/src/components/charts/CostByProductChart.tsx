import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { AggregationBucket } from "../../types/api";
import { aggregateByDimension } from "../../utils/aggregation";
import { DimensionPieChart } from "./DimensionPieChart";

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
  height = 300,
}: CostByProductChartProps): React.JSX.Element {
  const treemapOption: EChartsOption = useMemo(() => {
    if (chartType === "pie") return {};
    const byProduct = aggregateByDimension(data, "product_type");
    if (byProduct.length === 0) {
      return {
        graphic: [
          { type: "text", left: "center", top: "middle",
            style: { text: "No data", fontSize: 14, fill: "#999" } },
        ],
      };
    }
    return {
      tooltip: { formatter: "{b}: {c}" },
      series: [
        { type: "treemap", data: byProduct.map((d) => ({ name: d.key, value: d.amount })) },
      ],
    };
  }, [data, chartType]);

  if (chartType === "pie") {
    return (
      <DimensionPieChart
        data={data}
        dimension="product_type"
        topN={10}
        loading={loading}
        height={height}
      />
    );
  }

  return <ReactECharts option={treemapOption} style={{ height }} showLoading={loading} />;
}
