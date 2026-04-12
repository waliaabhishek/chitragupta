import type React from "react";
import { useCallback, useMemo, useState } from "react";
import { Segmented, Table } from "antd";
import { ChartCard } from "../charts/ChartCard";
import { TagKeySelect } from "./TagKeySelect";
import { PivotFilterBar } from "./PivotFilterBar";
import { TagPivotChart } from "./TagPivotChart";
import type { BucketLike } from "../../utils/aggregation";
import { formatCurrency } from "../../utils/aggregation";

type ViewMode = "chart" | "table";

interface TagPivotPanelProps {
  title: string;
  tenantName: string;
  buckets: BucketLike[];
  isLoading: boolean;
  error: string | null;
  onRefetch: () => void;
  selectedTagKey: string;
  onTagKeyChange: (key: string) => void;
  activeTagFilters: string[]; // active filter values for selectedTagKey
  onFilterAdd: (value: string) => void;
  onFilterRemove: (value: string) => void;
}

const VIEW_OPTIONS = [
  { label: "Chart", value: "chart" },
  { label: "Table", value: "table" },
];

export function TagPivotPanel({
  title,
  tenantName,
  buckets,
  isLoading,
  error,
  onRefetch,
  selectedTagKey,
  onTagKeyChange,
  activeTagFilters,
  onFilterAdd,
  onFilterRemove,
}: TagPivotPanelProps): React.JSX.Element {
  const [viewMode, setViewMode] = useState<ViewMode>("chart");
  const tagDimension = `tag:${selectedTagKey}`;

  const handleBarClick = useCallback(
    (tagValue: string) => {
      if (tagValue !== "UNTAGGED" && !activeTagFilters.includes(tagValue)) {
        onFilterAdd(tagValue);
      }
    },
    [activeTagFilters, onFilterAdd],
  );

  const tableData = useMemo(
    () =>
      buckets.map((b, i) => ({
        key: i,
        owner: b.dimensions[tagDimension] ?? "UNTAGGED",
        product_type: b.dimensions["product_type"] ?? "Unknown",
        total_amount: parseFloat(b.total_amount),
      })),
    [buckets, tagDimension],
  );

  const tableColumns = useMemo(
    () => [
      { title: selectedTagKey, dataIndex: "owner", key: "owner" },
      { title: "Product Type", dataIndex: "product_type", key: "product_type" },
      {
        title: "Amount",
        dataIndex: "total_amount",
        key: "total_amount",
        render: (v: number) => formatCurrency(v),
        sorter: (a: { total_amount: number }, b: { total_amount: number }) =>
          a.total_amount - b.total_amount,
      },
    ],
    [selectedTagKey],
  );

  const extra = (
    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
      <TagKeySelect
        tenantName={tenantName}
        value={selectedTagKey}
        onChange={onTagKeyChange}
      />
      <Segmented
        size="small"
        options={VIEW_OPTIONS}
        value={viewMode}
        onChange={(v) => setViewMode(v as ViewMode)}
      />
    </div>
  );

  return (
    <ChartCard
      title={title}
      loading={isLoading}
      error={error}
      onRetry={onRefetch}
      extra={extra}
    >
      <PivotFilterBar
        tenantName={tenantName}
        tagKey={selectedTagKey}
        activeFilters={activeTagFilters}
        onFilterAdd={onFilterAdd}
        onRemove={onFilterRemove}
      />
      {viewMode === "chart" ? (
        <TagPivotChart
          buckets={buckets}
          tagDimension={tagDimension}
          onBarClick={handleBarClick}
        />
      ) : (
        <Table
          size="small"
          dataSource={tableData}
          columns={tableColumns}
          pagination={{ pageSize: 20 }}
        />
      )}
    </ChartCard>
  );
}
