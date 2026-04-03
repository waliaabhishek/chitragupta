import type React from "react";
import type { ColDef, IDatasource, IGetRowsParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Tag } from "antd";
import {
  type MutableRefObject,
  type Ref,
  useMemo,
  useCallback,
  useEffect,
  useRef,
  useImperativeHandle,
} from "react";
import { API_URL } from "../../config";
import type { ChargebackResponse, PaginatedResponse } from "../../types/api";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";
import { ConfluentLinkRenderer } from "../common/ConfluentLinkRenderer";
import { dateFormatter, currencyFormatter } from "../../utils/gridFormatters";

interface ChargebackGridProps {
  tenantName: string;
  filters: Record<string, string>;
  onRowClick: (row: ChargebackResponse) => void;
  ref?: Ref<AgGridReact>;
}

function TagsCellRenderer(props: {
  value: Record<string, string>;
}): React.JSX.Element {
  const entries = Object.entries(props.value ?? {});
  const maxVisible = 2;
  const visible = entries.slice(0, maxVisible);
  const overflow = entries.length - maxVisible;

  return (
    <span>
      {visible.map(([k, v]) => (
        <Tag key={k} style={{ margin: "0 2px" }}>
          {k}: {v}
        </Tag>
      ))}
      {overflow > 0 && <Tag>+{overflow}</Tag>}
    </span>
  );
}

const columnDefs: ColDef[] = [
  {
    field: "timestamp",
    headerName: "Date",
    valueFormatter: dateFormatter,
    width: 120,
  },
  {
    field: "identity_id",
    headerName: "Identity",
    flex: 1,
    cellRenderer: ConfluentLinkRenderer,
  },
  { field: "product_type", headerName: "Product", width: 180 },
  {
    field: "resource_id",
    headerName: "Resource",
    flex: 1,
    cellRenderer: ConfluentLinkRenderer,
  },
  {
    field: "amount",
    headerName: "Amount",
    valueFormatter: currencyFormatter,
    width: 110,
  },
  {
    field: "tags",
    headerName: "Tags",
    cellRenderer: TagsCellRenderer,
    flex: 1,
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
      const url = new URL(
        `${window.location.origin}${API_URL}/tenants/${tenantName}/chargebacks`,
      );
      url.searchParams.set("page", String(page));
      url.searchParams.set("page_size", "100");
      for (const [k, v] of Object.entries(filters)) {
        url.searchParams.set(k, v);
      }

      // Read signal at call time so the useEffect's fresh controller is used.
      fetch(url.toString(), { signal: controllerRef.current.signal })
        .then((resp) => {
          if (!resp.ok) {
            params.failCallback();
            return;
          }
          return resp.json() as Promise<PaginatedResponse<ChargebackResponse>>;
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

export function ChargebackGrid({
  tenantName,
  filters,
  onRowClick,
  ref,
}: ChargebackGridProps): React.JSX.Element {
  const internalRef = useRef<AgGridReact>(null);
  const abortControllerRef = useRef(new AbortController());

  // Expose the grid instance via the forwarded ref.
  useImperativeHandle(ref, () => internalRef.current!, []);

  // Side effect: abort previous and create a fresh controller when tenant/filters change.
  // useEffect (not useMemo) owns this so it doesn't fire spuriously in Strict Mode.
  useEffect(() => {
    abortControllerRef.current.abort();
    abortControllerRef.current = new AbortController();
  }, [tenantName, filters]);

  // Pure computation: build datasource with ref; getRows reads signal at call time.
  const datasource = useMemo(
    () => createDatasource(tenantName, filters, abortControllerRef),
    [tenantName, filters],
  );

  // Purge AG Grid's infinite cache whenever the datasource changes.
  // AG Grid does not auto-refresh when the datasource prop is replaced.
  useEffect(() => {
    internalRef.current?.api?.purgeInfiniteCache();
  }, [datasource]);

  const handleRowClicked = useCallback(
    (e: { data: unknown }) => {
      const row = e.data as ChargebackResponse | undefined;
      if (row?.dimension_id != null) {
        onRowClick(row);
      }
    },
    [onRowClick],
  );

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
        onRowClicked={handleRowClicked}
      />
    </div>
  );
}
