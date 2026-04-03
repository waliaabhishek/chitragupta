import type React from "react";
import type {
  ColDef,
  ICellRendererParams,
  IDatasource,
  IGetRowsParams,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import {
  type MutableRefObject,
  type Ref,
  useMemo,
  useEffect,
  useRef,
  useImperativeHandle,
} from "react";
import { fetchTopicAttributions } from "../../api/topicAttributions";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";
import { clusterUrl, topicUrl } from "../../config/confluentCloudUrls";
import { dateFormatter, currencyFormatter } from "../../utils/gridFormatters";
import type { TopicAttributionResponse } from "../../types/api";

interface TopicAttributionGridProps {
  tenantName: string;
  filters: Record<string, string>;
  ref?: Ref<AgGridReact>;
}

const columnDefs: ColDef[] = [
  {
    field: "timestamp",
    headerName: "Date",
    valueFormatter: dateFormatter,
    width: 120,
  },
  {
    field: "cluster_resource_id",
    headerName: "Cluster",
    flex: 1,
    cellRenderer: ConfluentLinkRenderer,
    cellRendererParams: (p: ICellRendererParams<TopicAttributionResponse>) => ({
      url:
        p.data?.env_id && p.data?.cluster_resource_id
          ? clusterUrl(p.data.env_id, p.data.cluster_resource_id)
          : null,
    }),
  },
  {
    field: "topic_name",
    headerName: "Topic",
    flex: 1,
    cellRenderer: ConfluentLinkRenderer,
    cellRendererParams: (p: ICellRendererParams<TopicAttributionResponse>) => ({
      url:
        p.data?.env_id && p.data?.cluster_resource_id && p.data?.topic_name
          ? topicUrl(
              p.data.env_id,
              p.data.cluster_resource_id,
              p.data.topic_name,
            )
          : null,
    }),
  },
  { field: "product_type", headerName: "Product Type", width: 180 },
  { field: "attribution_method", headerName: "Method", width: 160 },
  {
    field: "amount",
    headerName: "Amount",
    valueFormatter: currencyFormatter,
    width: 110,
  },
];

function createDatasource(
  tenantName: string,
  filters: Record<string, string>,
  controllerRef: MutableRefObject<AbortController>,
): IDatasource {
  return {
    getRows: (params: IGetRowsParams) => {
      const page = Math.floor(params.startRow / 100) + 1;
      fetchTopicAttributions(
        tenantName,
        { page, page_size: 100, ...filters },
        controllerRef.current.signal,
      )
        .then((data) => params.successCallback(data.items, data.total))
        .catch(() => params.failCallback());
    },
  };
}

export function TopicAttributionGrid({
  tenantName,
  filters,
  ref,
}: TopicAttributionGridProps): React.JSX.Element {
  const internalRef = useRef<AgGridReact>(null);
  const abortControllerRef = useRef(new AbortController());

  useImperativeHandle(ref, () => internalRef.current!, []);

  useEffect(() => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
  }, [tenantName, filters]);

  const datasource = useMemo(
    () => createDatasource(tenantName, filters, abortControllerRef),
    [tenantName, filters],
  );

  useEffect(() => {
    internalRef.current?.api?.purgeInfiniteCache();
  }, [datasource]);

  return (
    <div style={{ flex: 1, minHeight: 400 }}>
      <AgGridReact
        ref={internalRef}
        theme={gridTheme}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        rowModelType="infinite"
        datasource={datasource}
        cacheBlockSize={100}
        maxBlocksInCache={10}
      />
    </div>
  );
}
