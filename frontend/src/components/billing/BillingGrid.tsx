import type React from "react";
import type { ColDef, IDatasource, IGetRowsParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import {
  type MutableRefObject,
  type Ref,
  useMemo,
  useEffect,
  useRef,
  useImperativeHandle,
} from "react";
import { API_URL } from "../../config";
import type { BillingLineResponse, PaginatedResponse } from "../../types/api";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";
import { dateFormatter, currencyFormatter } from "../../utils/gridFormatters";

interface BillingGridProps {
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
  { field: "product_category", headerName: "Category", width: 140 },
  { field: "product_type", headerName: "Product", width: 180 },
  { field: "resource_id", headerName: "Resource", flex: 1 },
  {
    field: "quantity",
    headerName: "Quantity",
    width: 100,
    type: "numericColumn",
  },
  {
    field: "unit_price",
    headerName: "Unit Price",
    valueFormatter: currencyFormatter,
    width: 110,
  },
  {
    field: "total_cost",
    headerName: "Total Cost",
    valueFormatter: currencyFormatter,
    width: 120,
  },
  { field: "currency", headerName: "Currency", width: 90 },
  { field: "granularity", headerName: "Granularity", width: 110 },
];

function createDatasource(
  tenantName: string,
  filters: Record<string, string>,
  controllerRef: MutableRefObject<AbortController>,
): IDatasource {
  return {
    getRows: (params: IGetRowsParams) => {
      const page = Math.floor(params.startRow / 100) + 1;
      const url = new URL(
        `${window.location.origin}${API_URL}/tenants/${tenantName}/billing`,
      );
      url.searchParams.set("page", String(page));
      url.searchParams.set("page_size", "100");
      for (const [k, v] of Object.entries(filters)) {
        url.searchParams.set(k, v);
      }

      fetch(url.toString(), { signal: controllerRef.current.signal })
        .then((resp) => {
          if (!resp.ok) {
            params.failCallback();
            return;
          }
          return resp.json() as Promise<PaginatedResponse<BillingLineResponse>>;
        })
        .then((data) => {
          if (data) {
            params.successCallback(data.items, data.total);
          }
        })
        .catch(() => {
          params.failCallback();
        });
    },
  };
}

export function BillingGrid({
  tenantName,
  filters,
  ref,
}: BillingGridProps): React.JSX.Element {
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
