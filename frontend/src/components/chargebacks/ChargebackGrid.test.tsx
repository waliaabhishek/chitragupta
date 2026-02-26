import { render, screen } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { ChargebackGrid } from "./ChargebackGrid";

type AgGridProps = {
  columnDefs?: ColDef[];
  onRowClicked?: (e: { data: unknown }) => void;
  datasource?: { getRows: (params: unknown) => void };
  rowModelType?: string;
  cacheBlockSize?: number;
  maxBlocksInCache?: number;
  style?: object;
};

// Mock AG Grid — renders columnDefs' cellRenderers so we can test them.
vi.mock("ag-grid-react", () => ({
  AgGridReact: vi.fn(
    ({ columnDefs, onRowClicked, datasource }: AgGridProps) => {
      const tagsCol = columnDefs?.find((c) => c.field === "tags");
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const TagsCellRenderer = tagsCol?.cellRenderer as any;
      return (
        <div>
          <div
            data-testid="ag-grid"
            data-has-datasource={datasource ? "true" : "false"}
            onClick={() =>
              onRowClicked?.({
                data: {
                  dimension_id: 42,
                  identity_id: "user@example.com",
                },
              })
            }
          >
            AG Grid
          </div>
          {TagsCellRenderer && (
            <div data-testid="tags-cell">
              <TagsCellRenderer value={["alpha", "beta", "gamma"]} />
            </div>
          )}
        </div>
      );
    },
  ),
}));

// Mock AG Grid CSS imports (not available in jsdom).
vi.mock("ag-grid-community/styles/ag-grid.css", () => ({}));
vi.mock("ag-grid-community/styles/ag-theme-alpine.css", () => ({}));

// Mock antd Tag
vi.mock("antd", () => ({
  Tag: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
}));

describe("ChargebackGrid", () => {
  it("renders AG Grid wrapper", () => {
    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
      />,
    );
    expect(screen.getByTestId("ag-grid")).toBeTruthy();
  });

  it("passes datasource to AG Grid", () => {
    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{ start_date: "2026-01-01" }}
        onRowClick={vi.fn()}
      />,
    );
    expect(screen.getByTestId("ag-grid").getAttribute("data-has-datasource")).toBe(
      "true",
    );
  });

  it("calls onRowClick when row is clicked", () => {
    const onRowClick = vi.fn();
    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={onRowClick}
      />,
    );
    screen.getByTestId("ag-grid").click();
    expect(onRowClick).toHaveBeenCalledWith(42);
  });

  it("renders tags cell with max 2 visible and overflow count", () => {
    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
      />,
    );
    const tagsCell = screen.getByTestId("tags-cell");
    // TagsCellRenderer receives ["alpha", "beta", "gamma"] — shows 2, overflow +1
    expect(tagsCell.textContent).toContain("alpha");
    expect(tagsCell.textContent).toContain("beta");
    expect(tagsCell.textContent).toContain("+1");
  });

  it("datasource fetches data from API", async () => {
    let capturedDatasource: { getRows: (p: { startRow: number; successCallback: (rows: unknown[], total: number) => void; failCallback: () => void }) => void } | undefined;

    const { AgGridReact } = await import("ag-grid-react");
    vi.mocked(AgGridReact).mockImplementationOnce(({ datasource }: { datasource?: typeof capturedDatasource }) => {
      capturedDatasource = datasource;
      return <div data-testid="ag-grid" />;
    });

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks", () => {
        return HttpResponse.json({
          items: [{ dimension_id: 1, identity_id: "user-1", amount: "10.00" }],
          total: 1,
          page: 1,
          page_size: 100,
          pages: 1,
        });
      }),
    );

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
      />,
    );

    expect(capturedDatasource).toBeDefined();

    const successCallback = vi.fn();
    const failCallback = vi.fn();

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback,
      failCallback,
    });

    await vi.waitFor(() => {
      expect(successCallback).toHaveBeenCalledWith(
        expect.arrayContaining([expect.objectContaining({ dimension_id: 1 })]),
        1,
      );
    });
  });

  it("datasource calls failCallback on API error", async () => {
    let capturedDatasource: { getRows: (p: { startRow: number; successCallback: (rows: unknown[], total: number) => void; failCallback: () => void }) => void } | undefined;

    const { AgGridReact } = await import("ag-grid-react");
    vi.mocked(AgGridReact).mockImplementationOnce(({ datasource }: { datasource?: typeof capturedDatasource }) => {
      capturedDatasource = datasource;
      return <div data-testid="ag-grid" />;
    });

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
      />,
    );

    const successCallback = vi.fn();
    const failCallback = vi.fn();

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback,
      failCallback,
    });

    await vi.waitFor(() => {
      expect(failCallback).toHaveBeenCalled();
    });
  });
});
