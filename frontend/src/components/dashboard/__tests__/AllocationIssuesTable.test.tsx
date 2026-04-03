import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AllocationIssuesTable } from "../AllocationIssuesTable";
import type { ChargebackFilters } from "../../../types/filters";

// ---------------------------------------------------------------------------
// Capture AG Grid props
// ---------------------------------------------------------------------------

type ColDef = {
  field?: string;
  headerName?: string;
  valueFormatter?: (p: { value: unknown }) => string;
};

const gridCapture = {
  columnDefs: null as ColDef[] | null,
  datasource: null as { getRows: (params: unknown) => void } | null,
};

vi.mock("ag-grid-react", () => ({
  AgGridReact: (props: {
    columnDefs?: ColDef[];
    datasource?: { getRows: (params: unknown) => void };
    rowModelType?: string;
    cacheBlockSize?: number;
    maxBlocksInCache?: number;
    theme?: unknown;
    defaultColDef?: unknown;
  }) => {
    gridCapture.columnDefs = props.columnDefs ?? null;
    gridCapture.datasource = props.datasource ?? null;
    return (
      <div
        data-testid="ag-grid"
        data-rowmodel={props.rowModelType}
        data-cacheblocksize={props.cacheBlockSize}
      >
        <table>
          <thead>
            <tr>
              {props.columnDefs?.map((col) => (
                <th key={col.field ?? col.headerName}>{col.headerName}</th>
              ))}
            </tr>
          </thead>
        </table>
      </div>
    );
  },
}));

vi.mock("ag-grid-community", () => ({
  themeAlpine: {
    withParams: () => ({
      withParams: () => ({}),
    }),
  },
}));

vi.mock("../../../utils/gridDefaults", () => ({
  gridTheme: {},
  defaultColDef: { sortable: true, resizable: true },
}));

vi.mock("../../../utils/gridFormatters", () => ({
  currencyFormatter: (p: { value: unknown }) => String(p.value ?? ""),
}));

const MOCK_FILTERS: ChargebackFilters = {
  start_date: null,
  end_date: null,
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
  timezone: null,
  tag_key: null,
  tag_value: null,
};

describe("AllocationIssuesTable (AG Grid)", () => {
  it("renders all 8 expected column headers", () => {
    render(
      <AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />,
    );

    const expectedColumns = [
      "Ecosystem",
      "Resource",
      "Product Type",
      "Identity",
      "Allocation Detail",
      "Usage Cost",
      "Shared Cost",
      "Total Cost",
    ];

    for (const col of expectedColumns) {
      expect(screen.getByText(col)).toBeInTheDocument();
    }
    expect(gridCapture.columnDefs).toHaveLength(8);
  });

  it("uses infinite row model with cacheBlockSize=100", () => {
    render(
      <AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />,
    );

    const grid = screen.getByTestId("ag-grid");
    expect(grid.getAttribute("data-rowmodel")).toBe("infinite");
    expect(grid.getAttribute("data-cacheblocksize")).toBe("100");
  });

  it("resource_id column uses ConfluentLinkRenderer cell renderer", () => {
    render(
      <AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />,
    );

    const resourceCol = gridCapture.columnDefs?.find(
      (c) => c.field === "resource_id",
    );
    expect(resourceCol).toBeDefined();
    expect(resourceCol?.valueFormatter).toBeUndefined();
    expect(
      (resourceCol as Record<string, unknown>)?.cellRenderer,
    ).toBeDefined();
  });

  it("datasource getRows calls the correct API URL with filters", () => {
    const filtersWithValues: ChargebackFilters = {
      ...MOCK_FILTERS,
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      identity_id: "sa-001",
      product_type: null,
      resource_id: null,
      timezone: "UTC",
    };

    render(
      <AllocationIssuesTable tenantName="acme" filters={filtersWithValues} />,
    );

    expect(gridCapture.datasource).not.toBeNull();

    // Mock fetch to verify the URL
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          items: [],
          total: 0,
          page: 1,
          page_size: 100,
          pages: 0,
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );

    const successCallback = vi.fn();
    gridCapture.datasource?.getRows({
      startRow: 0,
      endRow: 100,
      successCallback,
      failCallback: vi.fn(),
    });

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const calledUrl = fetchSpy.mock.calls[0][0] as string;
    expect(calledUrl).toContain("/tenants/acme/chargebacks/allocation-issues");
    expect(calledUrl).toContain("page=1");
    expect(calledUrl).toContain("page_size=100");
    expect(calledUrl).toContain("start_date=2026-01-01");
    expect(calledUrl).toContain("end_date=2026-01-31");
    expect(calledUrl).toContain("identity_id=sa-001");
    expect(calledUrl).toContain("timezone=UTC");
    expect(calledUrl).not.toContain("product_type");

    fetchSpy.mockRestore();
  });
});
