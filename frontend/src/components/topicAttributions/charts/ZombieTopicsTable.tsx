import type React from "react";
import { useMemo } from "react";
import type { ColDef } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Empty } from "antd";
import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { gridTheme, defaultColDef } from "../../../utils/gridDefaults";
import { currencyFormatter } from "../../../utils/gridFormatters";

const ZOMBIE_THRESHOLD = 0.01;

interface ZombieRow {
  topic_name: string;
  kafka_storage: number;
  kafka_network_read: number;
  kafka_network_write: number;
  total_network: number;
}

function computeZombieRows(
  buckets: TopicAttributionAggregationBucket[],
): ZombieRow[] {
  const topicMap: Record<string, Record<string, number>> = {};
  for (const b of buckets) {
    const topic = b.dimensions.topic_name ?? "Unknown";
    const pt = b.dimensions.product_type ?? "";
    topicMap[topic] ??= {};
    topicMap[topic][pt] =
      (topicMap[topic][pt] ?? 0) + parseFloat(b.total_amount);
  }
  return Object.entries(topicMap)
    .map(([topic, ptMap]) => ({
      topic_name: topic,
      kafka_storage: ptMap["KAFKA_STORAGE"] ?? 0,
      kafka_network_read: ptMap["KAFKA_NETWORK_READ"] ?? 0,
      kafka_network_write: ptMap["KAFKA_NETWORK_WRITE"] ?? 0,
      total_network:
        (ptMap["KAFKA_NETWORK_READ"] ?? 0) +
        (ptMap["KAFKA_NETWORK_WRITE"] ?? 0),
    }))
    .filter((r) => r.kafka_storage > 0 && r.total_network < ZOMBIE_THRESHOLD)
    .sort((a, b) => b.kafka_storage - a.kafka_storage);
}

const columnDefs: ColDef[] = [
  { field: "topic_name", headerName: "Topic", flex: 1 },
  {
    field: "kafka_storage",
    headerName: "Storage Cost",
    valueFormatter: currencyFormatter,
    width: 140,
  },
  {
    field: "kafka_network_read",
    headerName: "Network Read",
    valueFormatter: currencyFormatter,
    width: 140,
  },
  {
    field: "kafka_network_write",
    headerName: "Network Write",
    valueFormatter: currencyFormatter,
    width: 140,
  },
  {
    field: "total_network",
    headerName: "Total Network",
    valueFormatter: currencyFormatter,
    width: 140,
  },
];

interface ZombieTopicsTableProps {
  data: TopicAttributionAggregationBucket[];
}

export function ZombieTopicsTable({
  data,
}: ZombieTopicsTableProps): React.JSX.Element {
  const rows = useMemo(() => computeZombieRows(data), [data]);

  if (rows.length === 0) {
    return <Empty />;
  }

  return (
    <div style={{ height: 300 }}>
      <AgGridReact
        theme={gridTheme}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        rowModelType="clientSide"
        rowData={rows}
      />
    </div>
  );
}
