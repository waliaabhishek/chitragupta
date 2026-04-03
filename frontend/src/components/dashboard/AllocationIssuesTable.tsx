import type React from "react";
import type { ColDef, IDatasource, IGetRowsParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { type MutableRefObject, useMemo, useEffect, useRef } from "react";
import { API_URL } from "../../config";
import type {
  AllocationIssueResponse,
  PaginatedResponse,
} from "../../types/api";
import type { ChargebackFilters } from "../../types/filters";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";
import { currencyFormatter } from "../../utils/gridFormatters";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";

export type AllocationIssueItem = AllocationIssueResponse;

interface AllocationIssuesTableProps {
  tenantName: string;
  filters: ChargebackFilters;
}

const columnDefs: ColDef[] = [
  { field: "ecosystem", headerName: "Ecosystem", width: 140 },
  {
    field: "resource_id",
    headerName: "Resource",
    flex: 1,
    minWidth: 160,
    cellRenderer: ConfluentLinkRenderer,
  },
  { field: "product_type", headerName: "Product Type", width: 160 },
  {
    field: "identity_id",
    headerName: "Identity",
    flex: 1,
    minWidth: 160,
    cellRenderer: ConfluentLinkRenderer,
  },
  {
    field: "allocation_detail",
    headerName: "Allocation Detail",
    flex: 1,
    minWidth: 160,
  },
  {
    field: "usage_cost",
    headerName: "Usage Cost",
    width: 120,
    type: "numericColumn",
    valueFormatter: currencyFormatter,
  },
  {
    field: "shared_cost",
    headerName: "Shared Cost",
    width: 120,
    type: "numericColumn",
    valueFormatter: currencyFormatter,
  },
  {
    field: "total_cost",
    headerName: "Total Cost",
    width: 120,
    type: "numericColumn",
    valueFormatter: currencyFormatter,
  },
];

function createDatasource(
  tenantName: string,
  filters: ChargebackFilters,
  controllerRef: MutableRefObject<AbortController>,
): IDatasource {
  return {
    getRows: (params: IGetRowsParams) => {
      const page = Math.floor(params.startRow / 100) + 1;
      const url = new URL(
        `${window.location.origin}${API_URL}/tenants/${tenantName}/chargebacks/allocation-issues`,
      );
      url.searchParams.set("page", String(page));
      url.searchParams.set("page_size", "100");
      const {
        start_date,
        end_date,
        identity_id,
        product_type,
        resource_id,
        timezone,
      } = filters;
      if (start_date) url.searchParams.set("start_date", start_date);
      if (end_date) url.searchParams.set("end_date", end_date);
      if (identity_id) url.searchParams.set("identity_id", identity_id);
      if (product_type) url.searchParams.set("product_type", product_type);
      if (resource_id) url.searchParams.set("resource_id", resource_id);
      if (timezone) url.searchParams.set("timezone", timezone);

      fetch(url.toString(), { signal: controllerRef.current.signal })
        .then((resp) => {
          if (!resp.ok) {
            params.failCallback();
            return;
          }
          return resp.json() as Promise<
            PaginatedResponse<AllocationIssueResponse>
          >;
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

export function AllocationIssuesTable({
  tenantName,
  filters,
}: AllocationIssuesTableProps): React.JSX.Element {
  const gridRef = useRef<AgGridReact>(null);
  const abortControllerRef = useRef(new AbortController());

  useEffect(() => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
  }, [tenantName, filters]);

  const datasource = useMemo(
    () => createDatasource(tenantName, filters, abortControllerRef),
    [tenantName, filters],
  );

  useEffect(() => {
    gridRef.current?.api?.purgeInfiniteCache();
  }, [datasource]);

  return (
    <div style={{ height: 500 }}>
      <AgGridReact
        ref={gridRef}
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
