import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

interface DataAvailabilityTimelineProps {
  dates: string[];
  startDate: string;
  endDate: string;
  loading?: boolean;
  height?: number;
}

export function DataAvailabilityTimeline({
  dates,
  startDate,
  endDate,
  loading,
  height = 80,
}: DataAvailabilityTimelineProps): JSX.Element {
  const option: EChartsOption = useMemo(() => {
    const visible = dates.filter((d) => d >= startDate && d <= endDate);

    if (visible.length === 0) {
      return {
        graphic: [
          {
            type: "text",
            left: "center",
            top: "middle",
            style: { text: "No data available", fontSize: 14, fill: "#999" },
          },
        ],
      };
    }

    const seriesData = visible.map((d) => [new Date(d).getTime(), 1]);

    return {
      tooltip: {
        trigger: "item",
        formatter: (params: unknown) => {
          const p = params as { value: [number, number] };
          return `Data available: ${new Date(p.value[0]).toISOString().slice(0, 10)}`;
        },
      },
      grid: { top: 10, bottom: 30, left: 60, right: 20 },
      xAxis: {
        type: "time",
        min: startDate,
        max: endDate,
        axisLabel: { hideOverlap: true },
      },
      yAxis: {
        show: false,
        min: 0,
        max: 2,
      },
      series: [
        {
          type: "scatter",
          data: seriesData,
          symbolSize: 8,
          itemStyle: { color: "#52c41a" },
        },
      ],
    };
  }, [dates, startDate, endDate]);

  return <ReactECharts option={option} style={{ height }} showLoading={loading} />;
}
