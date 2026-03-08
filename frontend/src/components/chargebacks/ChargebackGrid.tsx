import "ag-grid-community/styles/ag-grid.css";
import "ag-grid-community/styles/ag-theme-alpine.css";
import type { ColDef, IDatasource, IGetRowsParams, SelectionChangedEvent } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Tag } from "antd";
import { forwardRef, useMemo, useCallback, useEffect, useRef, useImperativeHandle } from "react";
import { API_URL } from "../../config";
import type { ChargebackResponse, PaginatedResponse } from "../../types/api";

interface ChargebackGridProps {
  tenantName: string;
  filters: Record<string, string>;
  onRowClick: (dimensionId: number) => void;
  onSelectionChange?: (ids: number[]) => void;
  onSelectAll?: (total: number) => void;
}

function dateFormatter(params: { value: string }): string {
  if (!params.value) return "";
  return new Date(params.value).toLocaleDateString();
}

function currencyFormatter(params: { value: string }): string {
  if (params.value == null) return "";
  return `$${Number(params.value).toFixed(2)}`;
}

function TagsCellRenderer(props: { value: string[] }): JSX.Element {
  const tags = props.value ?? [];
  const maxVisible = 2;
  const visible = tags.slice(0, maxVisible);
  const overflow = tags.length - maxVisible;

  return (
    <span>
      {visible.map((tag, i) => (
        // eslint-disable-next-line react/no-array-index-key
        <Tag key={i} style={{ margin: "0 2px" }}>
          {tag}
        </Tag>
      ))}
      {overflow > 0 && <Tag>+{overflow}</Tag>}
    </span>
  );
}

const columnDefs: ColDef[] = [
  {
    headerCheckboxSelection: true,
    checkboxSelection: true,
    width: 50,
    pinned: "left",
  },
  {
    field: "timestamp",
    headerName: "Date",
    valueFormatter: dateFormatter,
    width: 120,
  },
  { field: "identity_id", headerName: "Identity", flex: 1 },
  { field: "product_type", headerName: "Product", width: 180 },
  { field: "resource_id", headerName: "Resource", flex: 1 },
  {
    field: "amount",
    headerName: "Amount",
    valueFormatter: currencyFormatter,
    width: 110,
  },
  { field: "tags", headerName: "Tags", cellRenderer: TagsCellRenderer, flex: 1 },
];

function createDatasource(
  tenantName: string,
  filters: Record<string, string>,
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

      fetch(url.toString())
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

export const ChargebackGrid = forwardRef<AgGridReact, ChargebackGridProps>(
  ({ tenantName, filters, onRowClick, onSelectionChange, onSelectAll }, ref) => {
    const internalRef = useRef<AgGridReact>(null);

    // Expose the grid instance via the forwarded ref.
    useImperativeHandle(ref, () => internalRef.current!, []);

    const datasource = useMemo(
      () => createDatasource(tenantName, filters),
      [tenantName, filters],
    );

    // Purge AG Grid's infinite cache whenever the datasource changes.
    // AG Grid does not auto-refresh when the datasource prop is replaced.
    useEffect(() => {
      internalRef.current?.api?.purgeInfiniteCache();
    }, [datasource]);

    const handleSelectionChanged = useCallback(
      (event: SelectionChangedEvent) => {
        if (!onSelectionChange) return;
        const selectedRows = event.api.getSelectedRows() as ChargebackResponse[];
        const ids = selectedRows
          .filter((r) => r.dimension_id != null)
          .map((r) => r.dimension_id as number);
        onSelectionChange(ids);
      },
      [onSelectionChange],
    );

    const handleHeaderCheckboxChange = useCallback(
      async (event: SelectionChangedEvent) => {
        // Check if all visible rows are selected (header checkbox)
        if (!onSelectAll) return;
        const selectedCount = event.api.getSelectedRows().length;
        if (selectedCount === 0) return;

        // Fetch total count matching current filters
        const url = new URL(
          `${window.location.origin}${API_URL}/tenants/${tenantName}/chargebacks`,
        );
        url.searchParams.set("page", "1");
        url.searchParams.set("page_size", "1");
        for (const [k, v] of Object.entries(filters)) {
          url.searchParams.set(k, v);
        }
        try {
          const resp = await fetch(url.toString());
          if (resp.ok) {
            const data = (await resp.json()) as { total: number };
            if (selectedCount > 0 && data.total > selectedCount) {
              onSelectAll(data.total);
            }
          }
        } catch {
          // ignore
        }
      },
      [tenantName, filters, onSelectAll],
    );

    return (
      <div className="ag-theme-alpine" style={{ flex: 1, minHeight: 400 }}>
        <AgGridReact
          ref={internalRef}
          columnDefs={columnDefs}
          rowModelType="infinite"
          datasource={datasource}
          cacheBlockSize={100}
          maxBlocksInCache={10}
          rowSelection="multiple"
          suppressRowClickSelection
          onSelectionChanged={(e) => {
            void handleHeaderCheckboxChange(e);
            handleSelectionChanged(e);
          }}
          onRowClicked={(e) => {
            const row = e.data as ChargebackResponse | undefined;
            if (row?.dimension_id != null) {
              onRowClick(row.dimension_id);
            }
          }}
          style={{ height: "100%", width: "100%" }}
        />
      </div>
    );
  },
);

ChargebackGrid.displayName = "ChargebackGrid";
