import type React from "react";
import type { ColDef, IDatasource, IGetRowsParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import {
  type MutableRefObject,
  useMemo,
  useCallback,
  useEffect,
  useRef,
} from "react";
import { API_URL } from "../../config";
import type { PaginatedResponse, ResourceResponse } from "../../types/api";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";

interface ResourceGridProps {
  tenantName: string;
  queryParams: Record<string, string>;
  onRowClick: (row: ResourceResponse) => void;
}

const columnDefs: ColDef[] = [
  { field: "resource_id", headerName: "Resource ID", flex: 2, minWidth: 200 },
  { field: "resource_type", headerName: "Type", width: 160 },
  { field: "display_name", headerName: "Display Name", flex: 2, minWidth: 160 },
  { field: "status", headerName: "Status", width: 120 },
];

function createDatasource(
  tenantName: string,
  queryParams: Record<string, string>,
  controllerRef: MutableRefObject<AbortController>,
): IDatasource {
  return {
    getRows: (params: IGetRowsParams) => {
      const page = Math.floor(params.startRow / 100) + 1;
      const url = new URL(
        `${window.location.origin}${API_URL}/tenants/${tenantName}/resources`,
      );
      url.searchParams.set("page", String(page));
      url.searchParams.set("page_size", "100");
      for (const [k, v] of Object.entries(queryParams)) {
        url.searchParams.set(k, v);
      }
      fetch(url.toString(), { signal: controllerRef.current.signal })
        .then((resp) => {
          if (!resp.ok) {
            params.failCallback();
            return;
          }
          return resp.json() as Promise<PaginatedResponse<ResourceResponse>>;
        })
        .then((data) => {
          if (data) params.successCallback(data.items, data.total);
        })
        .catch(() => {
          params.failCallback();
        });
    },
  };
}

export function ResourceGrid({
  tenantName,
  queryParams,
  onRowClick,
}: ResourceGridProps): React.JSX.Element {
  const gridRef = useRef<AgGridReact>(null);
  const abortControllerRef = useRef(new AbortController());

  useEffect(() => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
  }, [tenantName, queryParams]);

  const datasource = useMemo(
    () => createDatasource(tenantName, queryParams, abortControllerRef),
    [tenantName, queryParams],
  );

  useEffect(() => {
    gridRef.current?.api?.purgeInfiniteCache();
  }, [datasource]);

  const handleRowClicked = useCallback(
    (e: { data: unknown }) => {
      const row = e.data as ResourceResponse | undefined;
      if (row?.resource_id) onRowClick(row);
    },
    [onRowClick],
  );

  return (
    <div style={{ flex: 1, minHeight: 400 }}>
      <AgGridReact
        ref={gridRef}
        theme={gridTheme}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        rowModelType="infinite"
        datasource={datasource}
        cacheBlockSize={100}
        maxBlocksInCache={10}
        onRowClicked={handleRowClicked}
      />
    </div>
  );
}
