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
};

// Hoisted spy — must be declared before vi.mock so the factory can close over it.
const mockPurgeInfiniteCache = vi.hoisted(() => vi.fn());

// Per-test render override: set before render(), consumed once, then cleared.
// Tests that need to capture props use this instead of vi.mocked().mockImplementationOnce().
type RenderOverrideFn = (props: AgGridProps, ref: React.ForwardedRef<unknown>) => JSX.Element;
let renderOverride: RenderOverrideFn | undefined;

// Mock AG Grid as a forwardRef component so ChargebackGrid's internalRef is
// properly populated and api.purgeInfiniteCache() can be called through it.
vi.mock("ag-grid-react", async () => {
  const React = await import("react");
  return {
    AgGridReact: React.forwardRef(
      (props: AgGridProps, ref: React.ForwardedRef<unknown>) => {
        const { columnDefs, onRowClicked, datasource } = props;

        // Expose api.purgeInfiniteCache through the forwarded ref.
        React.useImperativeHandle(ref, () => ({
          api: { purgeInfiniteCache: mockPurgeInfiniteCache },
        }));

        // Allow per-test render override (consumed once).
        if (renderOverride) {
          const impl = renderOverride;
          renderOverride = undefined;
          return impl(props, ref);
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
        ) as JSX.Element;
      },
    ),
  };
});

// Mock AG Grid CSS imports (not available in jsdom).
vi.mock("ag-grid-community/styles/ag-grid.css", () => ({}));
vi.mock("ag-grid-community/styles/ag-theme-alpine.css", () => ({}));

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

  it("calls onSelectionChange when selection changes", async () => {
    let capturedOnSelectionChanged: ((e: unknown) => void) | undefined;

    renderOverride = ({ onSelectionChanged }: AgGridProps) => {
      capturedOnSelectionChanged = onSelectionChanged;
      return <div data-testid="ag-grid" />;
    };

    const onSelectionChange = vi.fn();

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
        onSelectionChange={onSelectionChange}
      />,
    );

    expect(capturedOnSelectionChanged).toBeDefined();

    // Simulate selection change event
    const mockEvent = {
      api: {
        getSelectedRows: () => [
          { dimension_id: 1, identity_id: "user1" },
          { dimension_id: 2, identity_id: "user2" },
        ],
      },
    };

    await capturedOnSelectionChanged!(mockEvent);

    expect(onSelectionChange).toHaveBeenCalledWith([1, 2]);
  });

  it("calls onSelectAll when header checkbox triggers selection", async () => {
    let capturedOnSelectionChanged: ((e: unknown) => void) | undefined;

    renderOverride = ({ onSelectionChanged }: AgGridProps) => {
      capturedOnSelectionChanged = onSelectionChanged;
      return <div data-testid="ag-grid" />;
    };

    server.use(
      http.get("/api/v1/tenants/acme/chargebacks", () => {
        return HttpResponse.json({
          items: [],
          total: 500,
          page: 1,
          page_size: 1,
          pages: 500,
        });
      }),
    );

    const onSelectAll = vi.fn();

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
        onSelectAll={onSelectAll}
      />,
    );

    expect(capturedOnSelectionChanged).toBeDefined();

    // Simulate header checkbox selection (some rows selected)
    const mockEvent = {
      api: {
        getSelectedRows: () => [
          { dimension_id: 1 },
          { dimension_id: 2 },
          { dimension_id: 3 },
        ],
      },
    };

    await capturedOnSelectionChanged!(mockEvent);

    await vi.waitFor(() => {
      expect(onSelectAll).toHaveBeenCalledWith(500);
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

  it("filters null dimension_ids from selection", async () => {
    let capturedOnSelectionChanged: ((e: unknown) => void) | undefined;

    renderOverride = ({ onSelectionChanged }: AgGridProps) => {
      capturedOnSelectionChanged = onSelectionChanged;
      return <div data-testid="ag-grid" />;
    };

    const onSelectionChange = vi.fn();

    render(
      <ChargebackGrid
        tenantName="acme"
        filters={{}}
        onRowClick={vi.fn()}
        onSelectionChange={onSelectionChange}
      />,
    );

    // Mix of valid and null dimension_ids
    const mockEvent = {
      api: {
        getSelectedRows: () => [
          { dimension_id: 1, identity_id: "user1" },
          { dimension_id: null, identity_id: "user2" },
          { dimension_id: 3, identity_id: "user3" },
        ],
      },
    };

    await capturedOnSelectionChanged!(mockEvent);

    // Should only include valid dimension_ids
    expect(onSelectionChange).toHaveBeenCalledWith([1, 3]);
  });
});
