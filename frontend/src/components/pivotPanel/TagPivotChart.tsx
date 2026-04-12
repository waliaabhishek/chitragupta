import type React from "react";
import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";
import type { BucketLike } from "../../utils/aggregation";
import { formatCurrency } from "../../utils/aggregation";

const UNTAGGED_COLOR = "#d9d9d9";

interface TagPivotChartProps {
  buckets: BucketLike[];
  tagDimension: string; // e.g. "tag:owner"
  height?: number;
  onBarClick?: (tagValue: string) => void;
}

function buildPivotData(
  buckets: BucketLike[],
  tagDimension: string,
): {
  owners: string[];
  productTypes: string[];
  ptOwnerMap: Record<string, Record<string, number>>;
} {
  const ownerSet = new Set<string>();
  const ptSet = new Set<string>();
  const ptOwnerMap: Record<string, Record<string, number>> = {};

  for (const b of buckets) {
    const owner = b.dimensions[tagDimension] ?? "UNTAGGED";
    const pt = b.dimensions["product_type"] ?? "Unknown";
    ownerSet.add(owner);
    ptSet.add(pt);
    ptOwnerMap[pt] ??= {};
    ptOwnerMap[pt][owner] =
      (ptOwnerMap[pt][owner] ?? 0) + parseFloat(b.total_amount);
  }

  const owners = [...ownerSet].sort((a, b) => {
    if (a === "UNTAGGED") return 1;
    if (b === "UNTAGGED") return -1;
    return a.localeCompare(b);
  });

  return { owners, productTypes: [...ptSet], ptOwnerMap };
}

export function TagPivotChart({
  buckets,
  tagDimension,
  height = 350,
  onBarClick,
}: TagPivotChartProps): React.JSX.Element {
  const { owners, productTypes, ptOwnerMap } = useMemo(
    () => buildPivotData(buckets, tagDimension),
    [buckets, tagDimension],
  );

  const option: EChartsOption = useMemo(
    () => ({
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        valueFormatter: (v: unknown) => formatCurrency(v as number),
      },
      legend: { bottom: 0 },
      xAxis: {
        type: "category",
        data: owners,
        axisLabel: { rotate: 45, hideOverlap: true },
      },
      yAxis: {
        type: "value",
        axisLabel: { formatter: (v: number) => formatCurrency(v) },
      },
      series: productTypes.map((pt) => ({
        name: pt,
        type: "bar",
        stack: "total",
        data: owners.map((o) => ({
          value: ptOwnerMap[pt]?.[o] ?? 0,
          itemStyle: o === "UNTAGGED" ? { color: UNTAGGED_COLOR } : undefined,
        })),
      })),
    }),
    [owners, productTypes, ptOwnerMap],
  );

  const onEvents = onBarClick
    ? { click: (params: { name: string }) => onBarClick(params.name) }
    : undefined;

  return (
    <ReactECharts
      option={option}
      notMerge
      style={{ height }}
      onEvents={onEvents}
    />
  );
}
