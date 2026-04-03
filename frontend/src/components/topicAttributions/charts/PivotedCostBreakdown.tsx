import type React from "react";
import { useMemo } from "react";
import type { ColDef, ValueFormatterParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { gridTheme, defaultColDef } from "../../../utils/gridDefaults";
import { currencyFormatter } from "../../../utils/gridFormatters";

// Static mapping: backend product_type → column group label
const PT_COLUMN_MAP = {
  write: "KAFKA_NETWORK_WRITE",
  read: "KAFKA_NETWORK_READ",
  storage: "KAFKA_STORAGE",
} as const;

interface PivotRow {
  topic_name: string;
  write: number;
  write_pct: number;
  read: number;
  read_pct: number;
  storage: number;
  storage_pct: number;
  other: number;
  other_pct: number;
  total: number;
}

function buildPivotRows(
  buckets: TopicAttributionAggregationBucket[],
): PivotRow[] {
  const topicMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const topic = b.dimensions.topic_name ?? "Unknown";
    const pt = b.dimensions.product_type ?? "Unknown";
    topicMap[topic] ??= {};
    topicMap[topic][pt] =
      (topicMap[topic][pt] ?? 0) + parseFloat(b.total_amount);
  }
  const knownPts = new Set(Object.values(PT_COLUMN_MAP));
  return Object.entries(topicMap)
    .map(([topic, ptMap]) => {
      const write = ptMap[PT_COLUMN_MAP.write] ?? 0;
      const read = ptMap[PT_COLUMN_MAP.read] ?? 0;
      const storage = ptMap[PT_COLUMN_MAP.storage] ?? 0;
      const other = Object.entries(ptMap)
        .filter(
          ([pt]) =>
            !knownPts.has(
              pt as (typeof PT_COLUMN_MAP)[keyof typeof PT_COLUMN_MAP],
            ),
        )
        .reduce((s, [, v]) => s + v, 0);
      const total = write + read + storage + other;
      const pct = (v: number): number => (total > 0 ? (v / total) * 100 : 0);
      return {
        topic_name: topic,
        write,
        write_pct: pct(write),
        read,
        read_pct: pct(read),
        storage,
        storage_pct: pct(storage),
        other,
        other_pct: pct(other),
        total,
      };
    })
    .sort((a, b) => b.total - a.total);
}

const PCT_FMT = (p: ValueFormatterParams): string =>
  `${(p.value as number).toFixed(1)}%`;

const colDefs: ColDef[] = [
  {
    field: "topic_name",
    headerName: "Topic",
    flex: 1,
    pinned: "left" as const,
  },
  {
    field: "total",
    headerName: "Total Cost",
    valueFormatter: currencyFormatter,
    width: 120,
    sort: "desc" as const,
  },
  {
    field: "write",
    headerName: "Write Cost",
    valueFormatter: currencyFormatter,
    width: 120,
  },
  {
    field: "write_pct",
    headerName: "Write %",
    valueFormatter: PCT_FMT,
    width: 90,
  },
  {
    field: "read",
    headerName: "Read Cost",
    valueFormatter: currencyFormatter,
    width: 120,
  },
  {
    field: "read_pct",
    headerName: "Read %",
    valueFormatter: PCT_FMT,
    width: 90,
  },
  {
    field: "storage",
    headerName: "Storage Cost",
    valueFormatter: currencyFormatter,
    width: 130,
  },
  {
    field: "storage_pct",
    headerName: "Storage %",
    valueFormatter: PCT_FMT,
    width: 90,
  },
  {
    field: "other",
    headerName: "Other Cost",
    valueFormatter: currencyFormatter,
    width: 120,
  },
  {
    field: "other_pct",
    headerName: "Other %",
    valueFormatter: PCT_FMT,
    width: 90,
  },
];

interface PivotedCostBreakdownProps {
  data: TopicAttributionAggregationBucket[];
}

export function PivotedCostBreakdown({
  data,
}: PivotedCostBreakdownProps): React.JSX.Element {
  const rowData = useMemo(() => buildPivotRows(data), [data]);
  return (
    <div style={{ height: 400 }}>
      <AgGridReact
        theme={gridTheme}
        columnDefs={colDefs}
        defaultColDef={defaultColDef}
        rowModelType="clientSide"
        rowData={rowData}
      />
    </div>
  );
}
