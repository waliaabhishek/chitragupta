import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { AllocationIssuesTable } from "../AllocationIssuesTable";
import type { AllocationIssueItem } from "../AllocationIssuesTable";
import type { ChargebackFilters } from "../../../types/filters";

// Capture column definitions and dataSource passed to Table
let capturedColumns: Array<{ title?: string; dataIndex?: string; defaultSortOrder?: string }> = [];
let capturedLocale: { emptyText?: ReactNode } | undefined;
let capturedDataSource: AllocationIssueItem[] = [];
let capturedPagination: { total?: number; pageSize?: number } | undefined;

vi.mock("../../../hooks/useAllocationIssues", () => ({
  useAllocationIssues: vi.fn(),
}));

vi.mock("antd", () => ({
  Table: ({
    columns,
    dataSource,
    locale,
    pagination,
  }: {
    columns?: Array<{ title?: string; dataIndex?: string; defaultSortOrder?: string }>;
    dataSource?: AllocationIssueItem[];
    locale?: { emptyText?: ReactNode };
    pagination?: { total?: number; pageSize?: number };
  }) => {
    capturedColumns = columns ?? [];
    capturedLocale = locale;
    capturedDataSource = dataSource ?? [];
    capturedPagination = pagination ?? undefined;
    return (
      <div data-testid="table">
        {columns?.map((col) => (
          <div key={col.dataIndex ?? col.title} data-testid="column-header">
            {col.title}
          </div>
        ))}
        {dataSource?.length === 0 && locale?.emptyText ? (
          <div data-testid="empty-state">{locale.emptyText}</div>
        ) : null}
      </div>
    );
  },
  Typography: {
    Text: ({ children, type }: { children: ReactNode; type?: string }) => (
      <span data-type={type}>{children}</span>
    ),
  },
  Empty: ({ description }: { description?: string }) => (
    <div data-testid="ant-empty">{description}</div>
  ),
  Skeleton: ({ active }: { active?: boolean }) => (
    <div data-testid="skeleton" data-active={active} />
  ),
}));

import { useAllocationIssues } from "../../../hooks/useAllocationIssues";

const MOCK_FILTERS: ChargebackFilters = {
  start_date: null,
  end_date: null,
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
};

const MOCK_ITEMS: AllocationIssueItem[] = [
  {
    ecosystem: "ccloud",
    resource_id: "lkc-abc123",
    product_type: "kafka",
    identity_id: "sa-001",
    allocation_detail: "no_identities_located",
    row_count: 3,
    usage_cost: "120.00",
    shared_cost: "0.00",
    total_cost: "120.00",
  },
  {
    ecosystem: "ccloud",
    resource_id: "lkc-xyz789",
    product_type: "connector",
    identity_id: "sa-002",
    allocation_detail: "no_metrics_located",
    row_count: 1,
    usage_cost: "0.00",
    shared_cost: "50.00",
    total_cost: "50.00",
  },
];

function mockHook(overrides: Partial<ReturnType<typeof useAllocationIssues>> = {}) {
  (useAllocationIssues as ReturnType<typeof vi.fn>).mockReturnValue({
    data: { items: MOCK_ITEMS, total: 2, page: 1, page_size: 50, pages: 1 },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    ...overrides,
  });
}

describe("AllocationIssuesTable", () => {
  it("renders all 8 expected columns", () => {
    mockHook();
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

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

    const headers = screen.getAllByTestId("column-header").map((el) => el.textContent);
    for (const col of expectedColumns) {
      expect(headers).toContain(col);
    }
    expect(headers).toHaveLength(8);
  });

  it("total_cost column has defaultSortOrder: descend", () => {
    mockHook();
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    const totalCostCol = capturedColumns.find(
      (c) => c.title === "Total Cost" || c.dataIndex === "total_cost",
    );
    expect(totalCostCol).toBeDefined();
    expect(totalCostCol?.defaultSortOrder).toBe("descend");
  });

  it("shows 'No allocation issues found' empty state when items is empty", () => {
    mockHook({
      data: { items: [], total: 0, page: 1, page_size: 50, pages: 0 },
    });
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    const emptyState = screen.getByTestId("empty-state");
    expect(emptyState.textContent).toContain("No allocation issues found");
  });

  it("shows skeleton when isLoading=true", () => {
    mockHook({ isLoading: true, data: null });
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    const skeletons = screen.getAllByTestId("skeleton");
    expect(skeletons.length).toBeGreaterThan(0);
  });

  it("passes items as dataSource to Table", () => {
    mockHook();
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    expect(capturedDataSource).toHaveLength(2);
    expect(capturedDataSource[0].identity_id).toBe("sa-001");
    expect(capturedDataSource[1].identity_id).toBe("sa-002");
  });

  it("pagination reflects total from hook data", () => {
    mockHook({
      data: { items: MOCK_ITEMS, total: 42, page: 1, page_size: 50, pages: 1 },
    });
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    expect(capturedPagination?.total).toBe(42);
    expect(capturedPagination?.pageSize).toBe(50);
  });

  it("shows error message when hook returns error", () => {
    mockHook({ error: "Network failure", data: null, isLoading: false });
    render(<AllocationIssuesTable tenantName="test-tenant" filters={MOCK_FILTERS} />);

    expect(screen.getByText(/Failed to load allocation issues: Network failure/)).toBeDefined();
  });
});
