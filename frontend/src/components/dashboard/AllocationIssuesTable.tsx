import type React from "react";
import { useState } from "react";
import { Skeleton, Table, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { AllocationIssueResponse } from "../../types/api";
import type { ChargebackFilters } from "../../types/filters";
import { useAllocationIssues } from "../../hooks/useAllocationIssues";

const { Text } = Typography;
const PAGE_SIZE = 50;

export type AllocationIssueItem = AllocationIssueResponse;

interface AllocationIssuesTableProps {
  tenantName: string;
  filters: ChargebackFilters;
}

const columns: ColumnsType<AllocationIssueItem> = [
  { title: "Ecosystem", dataIndex: "ecosystem", key: "ecosystem" },
  {
    title: "Resource",
    dataIndex: "resource_id",
    key: "resource_id",
    render: (v: string | null) => v ?? <Text type="secondary">—</Text>,
  },
  { title: "Product Type", dataIndex: "product_type", key: "product_type" },
  { title: "Identity", dataIndex: "identity_id", key: "identity_id" },
  { title: "Allocation Detail", dataIndex: "allocation_detail", key: "allocation_detail" },
  { title: "Usage Cost", dataIndex: "usage_cost", key: "usage_cost", align: "right" },
  { title: "Shared Cost", dataIndex: "shared_cost", key: "shared_cost", align: "right" },
  {
    title: "Total Cost",
    dataIndex: "total_cost",
    key: "total_cost",
    align: "right",
    defaultSortOrder: "descend",
    sorter: (a, b) => parseFloat(a.total_cost) - parseFloat(b.total_cost),
  },
];

export function AllocationIssuesTable({ tenantName, filters }: AllocationIssuesTableProps): React.JSX.Element {
  const [page, setPage] = useState(1);

  const { data, isLoading, error } = useAllocationIssues({
    tenantName,
    filters,
    page,
    pageSize: PAGE_SIZE,
  });

  if (error) {
    return <Text type="danger">Failed to load allocation issues: {error}</Text>;
  }

  if (isLoading) {
    return (
      <>
        <Skeleton active />
        <Skeleton active />
        <Skeleton active />
      </>
    );
  }

  return (
    <Table<AllocationIssueItem>
      dataSource={data?.items ?? []}
      columns={columns}
      rowKey={(r) => `${r.ecosystem}|${r.resource_id ?? ""}|${r.identity_id}|${r.allocation_detail}`}
      pagination={{
        total: data?.total ?? 0,
        pageSize: PAGE_SIZE,
        current: page,
        onChange: (p) => setPage(p),
        showTotal: (t) => `${t} issues`,
      }}
      locale={{ emptyText: "No allocation issues found" }}
      size="small"
    />
  );
}
