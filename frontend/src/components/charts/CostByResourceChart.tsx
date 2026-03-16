import type React from "react";
import type { AggregationBucket } from "../../types/api";
import { DimensionPieChart } from "./DimensionPieChart";

interface CostByResourceChartProps {
  data: AggregationBucket[];
  loading?: boolean;
  height?: number;
}

export function CostByResourceChart({
  data,
  loading,
  height = 300,
}: CostByResourceChartProps): React.JSX.Element {
  return (
    <DimensionPieChart
      data={data}
      dimension="resource_id"
      topN={10}
      loading={loading}
      height={height}
    />
  );
}
