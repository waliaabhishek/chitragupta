import { render, screen } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { ChargebackGrid } from "./ChargebackGrid";

type AgGridProps = {
  columnDefs?: ColDef[];
  onRowClicked?: (e: { data: unknown }) => void;
  onSelectionChanged?: (e: unknown) => void;
  datasource?: { getRows: (params: unknown) => void };
  rowModelType?: string;
  cacheBlockSize?: number;
  maxBlocksInCache?: number;
  style?: object;
  rowSelection?: { mode: string }; // Added: v35 uses object-based rowSelection
  suppressRowClickSelection?: boolean;
};

// Hoisted spy — must be declared before vi.mock so the factory can close over it.
const mockPurgeInfiniteCache = vi.hoisted(() => vi.fn());

// Per-test render override: set before render(), consumed once, then cleared.
// Tests that need to capture props use this instead of vi.mocked().mockImplementationOnce().
type RenderOverrideFn = (props: AgGridProps & { ref?: React.Ref<unknown> }) => React.JSX.Element;
let renderOverride: RenderOverrideFn | undefined;

// Mock AG Grid so ChargebackGrid's internalRef is properly populated and
// api.purgeInfiniteCache() can be called through it. React 19: ref as prop.
vi.mock("ag-grid-react", async () => {
  const React = await import("react");
  return {
    AgGridReact: (props: AgGridProps & { ref?: React.Ref<unknown> }) => {
      const { columnDefs, onRowClicked, datasource } = props;

      // Expose api.purgeInfiniteCache through the ref prop.
      React.useImperativeHandle(props.ref, () => ({
        api: { purgeInfiniteCache: mockPurgeInfiniteCache },
      }));

      // Allow per-test render override (consumed once).
      if (renderOverride) {
        const impl = renderOverride;
        renderOverride = undefined;
        return impl(props);
      }

      // Default render — renders columnDefs cellRenderers so we can test them.
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
      ) as React.JSX.Element;
    },
  };
});

// Mock antd Tag
vi.mock("antd", () => ({
  Tag: ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
}));

describe("ChargebackGrid", () => {
  beforeEach(() => {
    mockPurgeInfiniteCache.mockReset();
    renderOverride = undefined;
  });

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
    expect(onRowClick).toHaveBeenCalledWith(
      expect.objectContaining({ dimension_id: 42 }),
    );
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

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

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

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

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


  it("calls purgeInfiniteCache on the grid when filters change", () => {
    const { rerender } = render(
      <ChargebackGrid tenantName="acme" filters={{}} onRowClick={vi.fn()} />,
    );

    // Clear calls from initial mount before testing filter-change behavior.
    mockPurgeInfiniteCache.mockClear();

    rerender(
      <ChargebackGrid
        tenantName="acme"
        filters={{ start_date: "2026-01-01" }}
        onRowClick={vi.fn()}
      />,
    );

    expect(mockPurgeInfiniteCache).toHaveBeenCalledTimes(1);
  });

  it("does not pass deprecated suppressRowClickSelection prop", () => {
    let capturedProps: AgGridProps | undefined;

    renderOverride = (props: AgGridProps) => {
      capturedProps = props;
      return <div data-testid="ag-grid" />;
    };

    render(
      <ChargebackGrid tenantName="acme" filters={{}} onRowClick={vi.fn()} />,
    );

    expect(capturedProps?.suppressRowClickSelection).toBeUndefined();
  });

  it("TagsCellRenderer_renders_dict_with_key_value_chips_and_overflow", () => {
    // TASK-160.02: tags column now receives Record<string, string> instead of string[].
    // This test will FAIL until TagsCellRenderer is updated to handle dict input.
    let capturedColDefs: ColDef[] | undefined;

    renderOverride = ({ columnDefs }: AgGridProps) => {
      capturedColDefs = columnDefs;
      return <div data-testid="ag-grid" />;
    };

    render(
      <ChargebackGrid tenantName="acme" filters={{}} onRowClick={vi.fn()} />,
    );

    expect(capturedColDefs).toBeDefined();
    const tagsCol = capturedColDefs?.find((c) => c.field === "tags");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const TagsCellRenderer = tagsCol?.cellRenderer as any;
    expect(TagsCellRenderer).toBeDefined();

    // Render with dict value: 3 entries, max 2 visible, +1 overflow
    const { container } = render(
      <div data-testid="tags-cell-dict">
        <TagsCellRenderer value={{ env: "prod", team: "platform", a: "b" }} />
      </div>,
    );

    const tagsCell = container.querySelector("[data-testid='tags-cell-dict']")!;
    expect(tagsCell.textContent).toContain("env: prod");
    expect(tagsCell.textContent).toContain("team: platform");
    expect(tagsCell.textContent).toContain("+1");
  });

  it("datasource applies filters as query params when getRows is called", async () => {
    let capturedDatasource: { getRows: (p: { startRow: number; successCallback: (rows: unknown[], total: number) => void; failCallback: () => void }) => void } | undefined;
    let capturedUrl = "";

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          items: [],
          total: 0,
          page: 1,
          page_size: 100,
          pages: 0,
        });
      }),
    );

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{ start_date: "2026-01-01", identity_id: "user-1" }}
        onRowClick={vi.fn()}
      />,
    );

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback: vi.fn(),
      failCallback: vi.fn(),
    });

    await vi.waitFor(() => {
      expect(capturedUrl).toContain("start_date=2026-01-01");
      expect(capturedUrl).toContain("identity_id=user-1");
    });
  });

  it("TagsCellRenderer_renders_empty_dict_without_chips", () => {
    // TASK-160.02: empty dict should render no tag chips.
    let capturedColDefs: ColDef[] | undefined;

    renderOverride = ({ columnDefs }: AgGridProps) => {
      capturedColDefs = columnDefs;
      return <div data-testid="ag-grid" />;
    };

    render(
      <ChargebackGrid tenantName="acme" filters={{}} onRowClick={vi.fn()} />,
    );

    const tagsCol = capturedColDefs?.find((c) => c.field === "tags");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const TagsCellRenderer = tagsCol?.cellRenderer as any;

    const { container } = render(
      <div data-testid="tags-cell-empty">
        <TagsCellRenderer value={{}} />
      </div>,
    );

    const tagsCell = container.querySelector("[data-testid='tags-cell-empty']")!;
    // No tag chips rendered for empty dict
    expect(tagsCell.textContent).toBe("");
  });

});

