import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { AggregationBucket } from "../../types/api";
import { aggregateByTime, formatCurrency } from "../../utils/aggregation";

interface CostTrendChartProps {
  data: AggregationBucket[];
  timeBucket: "hour" | "day" | "week" | "month";
  loading?: boolean;
  height?: number;
}

export function CostTrendChart({
  data,
  timeBucket,
  loading,
  height = 300,
}: CostTrendChartProps): React.JSX.Element {
  const option: EChartsOption = useMemo(() => {
    const byTime = aggregateByTime(data);

    if (byTime.length === 0) {
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
      tooltip: { trigger: "axis" },
      xAxis: {
        type: "category",
        data: byTime.map((d) => d.time),
        axisLabel: {
          rotate: timeBucket === "day" ? 45 : 0,
          hideOverlap: true,
        },
      },
      yAxis: {
        type: "value",
        axisLabel: { formatter: formatCurrency },
      },
      series: [
        {
          type: "line",
          data: byTime.map((d) => d.amount),
          areaStyle: { opacity: 0.3 },
          smooth: true,
        },
      ],
    };
  }, [data, timeBucket]);

  return (
    <ReactECharts option={option} style={{ height }} showLoading={loading} />
  );
}
