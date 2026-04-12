import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { AggregationBucket } from "../../types/api";
import { aggregateByDimension, formatCurrency } from "../../utils/aggregation";

interface CostByIdentityChartProps {
  data: AggregationBucket[];
  topN?: number;
  loading?: boolean;
  height?: number;
}

export function CostByIdentityChart({
  data,
  topN = 10,
  loading,
  height = 400,
}: CostByIdentityChartProps): React.JSX.Element {
  const option: EChartsOption = useMemo(() => {
    const byIdentity = aggregateByDimension(data, "identity_id");
    const sorted = [...byIdentity]
      .sort((a, b) => b.amount - a.amount)
      .slice(0, topN);

    if (sorted.length === 0) {
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

    // Reverse so highest value appears at top of horizontal bar chart
    const reversed = [...sorted].reverse();

    return {
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "value",
        axisLabel: { formatter: formatCurrency },
      },
      yAxis: {
        type: "category",
        data: reversed.map((d) => d.key),
        axisLabel: { width: 120, overflow: "truncate" },
      },
      series: [
        {
          type: "bar",
          data: reversed.map((d) => d.amount),
        },
      ],
    };
  }, [data, topN]);

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ height }}
      showLoading={loading}
    />
  );
}
