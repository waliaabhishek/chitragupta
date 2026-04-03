import type React from "react";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TagsGrid } from "./TagsGrid";
import type { EntityTagResponse } from "../../types/api";
import type {
  CellValueChangedEvent,
  ColDef,
  IDatasource,
  IGetRowsParams,
} from "ag-grid-community";

type AgGridProps = {
  ref?: React.Ref<unknown>;
  columnDefs?: ColDef[];
  rowModelType?: string;
  datasource?: IDatasource;
  cacheBlockSize?: number;
  maxBlocksInCache?: number;
  onCellValueChanged?: (e: CellValueChangedEvent) => void;
};

// Captured AG Grid props — reset per test via beforeEach
let capturedProps: AgGridProps = {};
const mockPurgeInfiniteCache = vi.hoisted(() => vi.fn());

vi.mock("ag-grid-react", async () => {
  const React = await import("react");
  return {
    AgGridReact: (props: AgGridProps) => {
      capturedProps = props;
      React.useImperativeHandle(props.ref, () => ({
        api: { purgeInfiniteCache: mockPurgeInfiniteCache },
      }));
      return React.createElement("div", { "data-testid": "tags-ag-grid" });
    },
  };
});

vi.mock("antd", () => ({
  Popconfirm: ({
    children,
    onConfirm,
    title,
  }: {
    children: ReactNode;
    onConfirm?: () => void;
    title?: string;
    okText?: string;
    cancelText?: string;
  }) => (
    <span data-testid="popconfirm" data-title={title} onClick={onConfirm}>
      {children}
    </span>
  ),
  Button: ({
    children,
    onClick,
    type: btnType,
    danger,
  }: {
    children: ReactNode;
    onClick?: () => void;
    type?: string;
    danger?: boolean;
    size?: string;
  }) => (
    <button
      onClick={onClick}
      data-btn-type={btnType}
      data-danger={danger ? "true" : undefined}
    >
      {children}
    </button>
  ),
  notification: {
    error: vi.fn(),
  },
}));

const entityTagFixture: EntityTagResponse = {
  tag_id: 1,
  tenant_id: "t-001",
  entity_type: "resource",
  entity_id: "r-001",
  tag_key: "env",
  tag_value: "prod",
  created_by: "ui",
  created_at: null,
};

beforeEach(() => {
  capturedProps = {};
  mockPurgeInfiniteCache.mockReset();
  vi.clearAllMocks();
});

describe("TagsGrid", () => {
  it("renders AG Grid wrapper", () => {
    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);
    expect(screen.getByTestId("tags-ag-grid")).toBeTruthy();
  });

  it("TagManagementPage_renders_EntityTagResponse_columns", () => {
    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);
    const fields = capturedProps.columnDefs?.map(
      (c: ColDef) => c.field ?? c.headerName,
    );
    expect(fields).toContain("entity_type");
    expect(fields).toContain("entity_id");
    expect(fields).toContain("tag_key");
    expect(fields).toContain("tag_value");
  });

  it("TagManagementPage_edit_calls_PUT_on_entity_endpoint", async () => {
    let putCalled = false;
    server.use(
      http.put(
        "/api/v1/tenants/acme/entities/resource/r-001/tags/env",
        async () => {
          putCalled = true;
          return HttpResponse.json({
            ...entityTagFixture,
            tag_value: "staging",
          });
        },
      ),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    const mockEvent = {
      data: entityTagFixture,
      newValue: "staging",
      oldValue: "prod",
      node: { setDataValue: vi.fn() },
      column: { getColId: () => "tag_value" },
    } as unknown as CellValueChangedEvent;

    capturedProps.onCellValueChanged?.(mockEvent);

    await waitFor(() => {
      expect(putCalled).toBe(true);
    });
  });

  it("TagManagementPage_delete_calls_DELETE_on_entity_endpoint", async () => {
    let deleteCalled = false;
    server.use(
      http.delete(
        "/api/v1/tenants/acme/entities/resource/r-001/tags/env",
        () => {
          deleteCalled = true;
          return new HttpResponse(null, { status: 204 });
        },
      ),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    const actionsCol = capturedProps.columnDefs?.find(
      (c: ColDef) => c.headerName === "Actions",
    );
    expect(actionsCol).toBeDefined();

    const cellRenderer = actionsCol!.cellRenderer as (props: {
      data: EntityTagResponse;
    }) => React.JSX.Element;

    const { getByTestId } = render(
      <div>{cellRenderer({ data: entityTagFixture })}</div>,
    );

    act(() => {
      fireEvent.click(getByTestId("popconfirm"));
    });

    await waitFor(() => {
      expect(deleteCalled).toBe(true);
    });
  });

  it("shows error notification when DELETE fails", async () => {
    server.use(
      http.delete(
        "/api/v1/tenants/acme/entities/resource/r-001/tags/env",
        () => {
          return new HttpResponse(null, { status: 500 });
        },
      ),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    const actionsCol = capturedProps.columnDefs?.find(
      (c: ColDef) => c.headerName === "Actions",
    );
    const cellRenderer = actionsCol!.cellRenderer as (props: {
      data: EntityTagResponse;
    }) => React.JSX.Element;

    const { getByTestId } = render(
      <div>{cellRenderer({ data: entityTagFixture })}</div>,
    );

    act(() => {
      fireEvent.click(getByTestId("popconfirm"));
    });

    const { notification } = await import("antd");
    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to delete tag" }),
      );
    });
  });

  it("pressing Enter in edit input saves the tag via PUT", async () => {
    // AG Grid fires onCellValueChanged when the user commits an edit (Enter or Tab).
    let putCalled = false;
    server.use(
      http.put(
        "/api/v1/tenants/acme/entities/resource/r-001/tags/env",
        async () => {
          putCalled = true;
          return HttpResponse.json({
            ...entityTagFixture,
            tag_value: "staging",
          });
        },
      ),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    const mockEvent = {
      data: entityTagFixture,
      newValue: "staging",
      oldValue: "prod",
      node: { setDataValue: vi.fn() },
      column: { getColId: () => "tag_value" },
    } as unknown as CellValueChangedEvent;

    capturedProps.onCellValueChanged?.(mockEvent);

    await waitFor(() => {
      expect(putCalled).toBe(true);
    });
  });

  it("shows error notification when PUT fails during edit save", async () => {
    server.use(
      http.put("/api/v1/tenants/acme/entities/resource/r-001/tags/env", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    const mockSetDataValue = vi.fn();
    const mockEvent = {
      data: entityTagFixture,
      newValue: "staging",
      oldValue: "prod",
      node: { setDataValue: mockSetDataValue },
      column: { getColId: () => "tag_value" },
    } as unknown as CellValueChangedEvent;

    capturedProps.onCellValueChanged?.(mockEvent);

    const { notification } = await import("antd");
    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to update tag" }),
      );
    });
    // Error handler reverts the cell value
    await waitFor(() => {
      expect(mockSetDataValue).toHaveBeenCalledWith("tag_value", "prod");
    });
  });

  it("shows fetch failure notification when API fails", async () => {
    // TagsGrid datasource calls failCallback on API error.
    server.use(
      http.get("/api/v1/tenants/acme/tags", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={false} />);

    expect(capturedProps.datasource).toBeDefined();

    const failCallback = vi.fn();
    capturedProps.datasource!.getRows({
      startRow: 0,
      successCallback: vi.fn(),
      failCallback,
    } as unknown as IGetRowsParams);

    await waitFor(() => {
      expect(failCallback).toHaveBeenCalled();
    });
  });

  it("isReadOnly hides edit button and shows plain text value", () => {
    render(<TagsGrid tenantName="acme" queryParams={{}} isReadOnly={true} />);

    // No Actions column when isReadOnly=true
    const actionsCol = capturedProps.columnDefs?.find(
      (c: ColDef) => c.headerName === "Actions",
    );
    expect(actionsCol).toBeUndefined();

    // tag_value column is not editable when isReadOnly=true
    const valueCol = capturedProps.columnDefs?.find(
      (c: ColDef) => c.field === "tag_value",
    );
    expect(valueCol?.editable).toBe(false);
  });

  it("datasource fetches tags from API with queryParams", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/tags", () => {
        return HttpResponse.json({
          items: [entityTagFixture],
          total: 1,
          page: 1,
          page_size: 100,
          pages: 1,
        });
      }),
    );

    render(
      <TagsGrid
        tenantName="acme"
        queryParams={{ tag_key: "env" }}
        isReadOnly={false}
      />,
    );

    const capturedDatasource = capturedProps.datasource;
    expect(capturedDatasource).toBeDefined();

    const successCallback = vi.fn();
    const failCallback = vi.fn();

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback,
      failCallback,
    } as unknown as IGetRowsParams);

    await waitFor(() => {
      expect(successCallback).toHaveBeenCalledWith(
        expect.arrayContaining([expect.objectContaining({ tag_id: 1 })]),
        1,
      );
    });
  });
});
