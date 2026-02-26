import { useMemo } from "react";
import { Progress, Table } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { AggregationBucket } from "../../types/api";
import { aggregateByDimension, formatCurrency } from "../../utils/aggregation";

interface CostByResourceChartProps {
  data: AggregationBucket[];
  topN?: number;
  loading?: boolean;
}

interface ResourceRow {
  key: number;
  resource_id: string;
  amount: number;
  percentage: number;
}

export function CostByResourceChart({
  data,
  topN = 10,
  loading,
}: CostByResourceChartProps): JSX.Element {
  const tableData = useMemo<ResourceRow[]>(() => {
    const byResource = aggregateByDimension(data, "resource_id");
    const sorted = [...byResource].sort((a, b) => b.amount - a.amount).slice(0, topN);
    const maxAmount = sorted[0]?.amount ?? 1;

    return sorted.map((d, i) => ({
      key: i,
      resource_id: d.key,
      amount: d.amount,
      percentage: (d.amount / maxAmount) * 100,
    }));
  }, [data, topN]);

  const columns: ColumnsType<ResourceRow> = [
    {
      title: "Resource",
      dataIndex: "resource_id",
      ellipsis: true,
    },
    {
      title: "Cost",
      dataIndex: "amount",
      render: (amt: number) => formatCurrency(amt),
      width: 120,
    },
    {
      title: "",
      dataIndex: "percentage",
      width: 200,
      render: (pct: number) => <Progress percent={Math.round(pct)} showInfo={false} size="small" />,
    },
  ];

  return (
    <Table<ResourceRow>
      dataSource={tableData}
      columns={columns}
      loading={loading}
      pagination={false}
      size="small"
      locale={{ emptyText: "No data" }}
    />
  );
}
