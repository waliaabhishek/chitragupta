import type React from "react";
import type {
  ColDef,
  IDatasource,
  IGetRowsParams,
  CellValueChangedEvent,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Popconfirm, Button, notification } from "antd";
import { type MutableRefObject, useMemo, useCallback, useEffect, useRef } from "react";
import { API_URL } from "../../config";
import type { EntityTagResponse, PaginatedResponse } from "../../types/api";
import { GRID_THEME_CLASS, defaultColDef } from "../../utils/gridDefaults";

interface TagsGridProps {
  tenantName: string;
  queryParams: Record<string, string>;
  isReadOnly: boolean;
}

function DeleteCellRenderer(props: {
  data: EntityTagResponse | undefined;
  tenantName: string;
  onDeleted: () => void;
}): React.JSX.Element | null {
  const { data, tenantName, onDeleted } = props;
  if (!data) return null;

  const handleDelete = async () => {
    try {
      const resp = await fetch(
        `${API_URL}/tenants/${tenantName}/entities/${data.entity_type}/${data.entity_id}/tags/${data.tag_key}`,
        { method: "DELETE" },
      );
      if (!resp.ok) throw new Error("Failed to delete tag");
      onDeleted();
    } catch (err) {
      notification.error({
        message: "Failed to delete tag",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
  };

  return (
    <Popconfirm
      title="Delete this tag?"
      onConfirm={() => void handleDelete()}
      okText="Delete"
      cancelText="Cancel"
    >
      <Button type="link" danger size="small">
        Delete
      </Button>
    </Popconfirm>
  );
}

function createDatasource(
  tenantName: string,
  queryParams: Record<string, string>,
  controllerRef: MutableRefObject<AbortController>,
): IDatasource {
  return {
    getRows: (params: IGetRowsParams) => {
      const page = Math.floor(params.startRow / 100) + 1;
      const url = new URL(`${window.location.origin}${API_URL}/tenants/${tenantName}/tags`);
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
          return resp.json() as Promise<PaginatedResponse<EntityTagResponse>>;
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

export function TagsGrid({ tenantName, queryParams, isReadOnly }: TagsGridProps): React.JSX.Element {
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

  const handleDeleted = useCallback(() => {
    gridRef.current?.api?.purgeInfiniteCache();
  }, []);

  const handleCellValueChanged = useCallback(
    async (event: CellValueChangedEvent) => {
      const row = event.data as EntityTagResponse;
      try {
        const resp = await fetch(
          `${API_URL}/tenants/${tenantName}/entities/${row.entity_type}/${row.entity_id}/tags/${row.tag_key}`,
          {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ tag_value: event.newValue as string }),
          },
        );
        if (!resp.ok) throw new Error("Failed to update tag");
      } catch (err) {
        notification.error({
          message: "Failed to update tag",
          description: err instanceof Error ? err.message : "Unknown error",
        });
        event.node.setDataValue(event.column.getColId(), event.oldValue);
      }
    },
    [tenantName],
  );

  const columnDefs: ColDef[] = useMemo(
    () => [
      { field: "entity_type", headerName: "Entity Type", width: 130 },
      { field: "entity_id", headerName: "Entity ID", flex: 2, minWidth: 200 },
      { field: "tag_key", headerName: "Key", width: 160 },
      {
        field: "tag_value",
        headerName: "Value",
        flex: 1,
        minWidth: 160,
        editable: !isReadOnly,
        cellEditor: "agTextCellEditor",
        singleClickEdit: true,
      },
      { field: "created_by", headerName: "Created By", width: 130 },
      ...(!isReadOnly
        ? [
            {
              headerName: "Actions",
              width: 100,
              cellRenderer: (props: { data: EntityTagResponse | undefined }) =>
                DeleteCellRenderer({ ...props, tenantName, onDeleted: handleDeleted }),
            },
          ]
        : []),
    ],
    [isReadOnly, tenantName, handleDeleted],
  );

  return (
    <div className={GRID_THEME_CLASS} style={{ flex: 1, minHeight: 400 }}>
      <AgGridReact
        ref={gridRef}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        rowModelType="infinite"
        datasource={datasource}
        cacheBlockSize={100}
        maxBlocksInCache={10}
        onCellValueChanged={(e) => {
          void handleCellValueChanged(e);
        }}
      />
    </div>
  );
}
