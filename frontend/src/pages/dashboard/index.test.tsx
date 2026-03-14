import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CostDashboardPage } from "./index";

// Mock echarts-for-react globally for all chart components
vi.mock("echarts-for-react", () => ({
  default: vi.fn(() => <div data-testid="echarts" />),
}));

// Mock chart components to avoid deep ECharts rendering
vi.mock("../../components/charts/ProductChartTypeToggle", () => ({
  ProductChartTypeToggle: vi.fn(() => <div data-testid="product-chart-type-toggle" />),
}));
vi.mock("../../components/charts/CostTrendChart", () => ({
  CostTrendChart: vi.fn(() => <div data-testid="cost-trend-chart" />),
}));
vi.mock("../../components/charts/CostByIdentityChart", () => ({
  CostByIdentityChart: vi.fn(() => <div data-testid="cost-by-identity-chart" />),
}));
vi.mock("../../components/charts/CostByProductChart", () => ({
  CostByProductChart: vi.fn(() => <div data-testid="cost-by-product-chart" />),
}));
vi.mock("../../components/charts/CostByResourceChart", () => ({
  CostByResourceChart: vi.fn(() => <div data-testid="cost-by-resource-chart" />),
}));
vi.mock("../../components/charts/DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(() => <div data-testid="dimension-pie-chart" />),
}));
vi.mock("../../components/charts/DataAvailabilityTimeline", () => ({
  DataAvailabilityTimeline: vi.fn(() => <div data-testid="data-availability-timeline" />),
}));

// Mock FilterPanel — expose onRefresh so tests can trigger it
vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(({ onReset, onRefresh }: { onReset: () => void; onRefresh?: () => void }) => (
    <div data-testid="filter-panel">
      <button onClick={onReset}>Reset</button>
      {onRefresh !== undefined && (
        <button data-testid="filter-refresh" onClick={onRefresh}>
          Refresh Data
        </button>
      )}
    </div>
  )),
}));

// Mock ChartCard — just render children
vi.mock("../../components/charts/ChartCard", () => ({
  ChartCard: vi.fn(
    ({ title, children, loading }: { title: string; children: ReactNode; loading?: boolean }) =>
      loading ? (
        <div data-testid="chart-card-loading">{title}</div>
      ) : (
        <div data-testid="chart-card">
          <span>{title}</span>
          {children}
        </div>
      ),
  ),
}));

// Mock useAggregation so we can spy on calls
vi.mock("../../hooks/useAggregation", () => ({
  useAggregation: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useDataAvailability", () => ({
  useDataAvailability: vi.fn(() => ({
    dates: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../hooks/useInventorySummary", () => ({
  useInventorySummary: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../components/dashboard/InventoryCounters", () => ({
  InventoryCounters: vi.fn(() => <div data-testid="inventory-counters" />),
}));

vi.mock("../../components/dashboard/AllocationIssuesTable", () => ({
  AllocationIssuesTable: vi.fn(() => <div data-testid="allocation-issues-table" />),
}));

// Mock antd
vi.mock("antd", () => ({
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number }) => <h3>{children}</h3>,
    Text: ({ children }: { children: ReactNode; type?: string }) => <span>{children}</span>,
  },
  Row: ({ children }: { children: ReactNode; gutter?: number | number[] }) => <div>{children}</div>,
  Col: ({ children }: { children: ReactNode; span?: number; xs?: number; md?: number }) => (
    <div>{children}</div>
  ),
  Card: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Skeleton: () => <div data-testid="skeleton" />,
  Statistic: ({ title, value }: { title: string; value: string | number }) => (
    <div>
      <span>{title}</span>
      <span>{value}</span>
    </div>
  ),
  Radio: {
    Group: ({
      children,
      value,
      onChange,
    }: {
      children: ReactNode;
      value: string;
      onChange: (e: { target: { value: string } }) => void;
    }) => (
      <div data-testid="time-bucket-selector" data-value={value}>
        {children}
        <button
          onClick={() => onChange({ target: { value: "week" } })}
          data-testid="select-week"
        >
          Week
        </button>
      </div>
    ),
    Button: ({ children, value }: { children: ReactNode; value: string }) => (
      <button data-value={value}>{children}</button>
    ),
  },
}));

const mockTenant = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
};

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: null,
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    appStatus: "ready" as const,
    readiness: null,
    isReadOnly: false,
  })),
}));

vi.mock("../../hooks/useChargebackFilters", () => ({
  useChargebackFilters: vi.fn(() => ({
    filters: { start_date: null, end_date: null },
    setFilter: vi.fn(),
    setFilters: vi.fn(),
    resetFilters: vi.fn(),
    toQueryParams: vi.fn(() => ({})),
  })),
}));

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return (
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      {children}
    </MemoryRouter>
  );
}

describe("CostDashboardPage", () => {
  beforeEach(async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: null,
      tenants: [],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("shows placeholder when no tenant selected", () => {
    render(<CostDashboardPage />, { wrapper });
    expect(screen.getByText("Cost Dashboard")).toBeInTheDocument();
    expect(screen.getByText("Select a tenant to view cost analytics.")).toBeInTheDocument();
    expect(screen.queryByTestId("filter-panel")).toBeNull();
    expect(screen.queryByTestId("time-bucket-selector")).toBeNull();
    expect(screen.queryByText("Cost Trend Over Time")).toBeNull();
  });

  it("renders all six chart cards when tenant is selected", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
      expect(screen.getByTestId("time-bucket-selector")).toBeInTheDocument();
    });

    expect(screen.getByTestId("inventory-counters")).toBeInTheDocument();
    expect(screen.getByText("Data Availability")).toBeInTheDocument();
    expect(screen.getByTestId("data-availability-timeline")).toBeInTheDocument();
    expect(screen.getByText("Cost Trend Over Time")).toBeInTheDocument();
    expect(screen.getByText("Cost by Identity")).toBeInTheDocument();
    expect(screen.getByText("Cost by Environment")).toBeInTheDocument();
    expect(screen.getByText("Cost by Resource")).toBeInTheDocument();
    expect(screen.getByText("Cost by Product Type")).toBeInTheDocument();
    expect(screen.getByText("Cost by Product Sub-Type")).toBeInTheDocument();
  });

  it("makes 5 useAggregation calls with environment_id and product_sub_type groupBy", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    expect(mockUseAggregation).toHaveBeenCalledTimes(5);

    const groupByValues = mockUseAggregation.mock.calls.map(
      (call) => call[0].groupBy,
    );
    expect(groupByValues.flat()).toContain("environment_id");
    expect(groupByValues.flat()).toContain("product_sub_type");
  });

  it("forwards explicit date filters to useAggregation", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    const { useChargebackFilters } = await import("../../hooks/useChargebackFilters");
    vi.mocked(useChargebackFilters).mockReturnValue({
      filters: {
        start_date: "2026-01-01",
        end_date: "2026-01-31",
        identity_id: null,
        product_type: null,
        resource_id: null,
        cost_type: null,
      },
      setFilter: vi.fn(),
      setFilters: vi.fn(),
      resetFilters: vi.fn(),
      toQueryParams: vi.fn(() => ({})),
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    const mockUseAggregation = vi.mocked(useAggregation);

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    const calls = mockUseAggregation.mock.calls;
    expect(calls.length).toBeGreaterThan(0);
    expect(calls[0][0].startDate).toBe("2026-01-01");
    expect(calls[0][0].endDate).toBe("2026-01-31");
  });

  it("changes time bucket when selector is clicked", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("time-bucket-selector")).toBeInTheDocument();
    });

    expect(screen.getByTestId("time-bucket-selector").getAttribute("data-value")).toBe("day");
    await userEvent.click(screen.getByTestId("select-week"));
    expect(screen.getByTestId("time-bucket-selector").getAttribute("data-value")).toBe("week");
  });

  it("passes onRefresh to FilterPanel and remounts DashboardContent on click", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenant,
      tenants: [mockTenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      appStatus: "ready" as const,
      readiness: null,
      isReadOnly: false,
    });

    const { useAggregation } = await import("../../hooks/useAggregation");
    vi.mocked(useAggregation).mockClear();

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      expect(screen.getByTestId("filter-panel")).toBeInTheDocument();
    });

    // FilterPanel receives onRefresh — the Refresh Data button should render
    expect(screen.getByTestId("filter-refresh")).toBeInTheDocument();

    const callsAfterMount = vi.mocked(useAggregation).mock.calls.length;
    expect(callsAfterMount).toBeGreaterThan(0);

    // Clicking Refresh Data increments refreshKey → DashboardContent remounts
    await userEvent.click(screen.getByTestId("filter-refresh"));

    // DashboardContent remount triggers useAggregation calls again
    expect(vi.mocked(useAggregation).mock.calls.length).toBeGreaterThan(callsAfterMount);
  });
});
