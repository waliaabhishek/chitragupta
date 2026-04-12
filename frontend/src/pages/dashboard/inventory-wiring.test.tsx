import type React from "react";
/**
 * Integration test: verifies that DashboardContent wires useInventorySummary
 * to InventoryCounters correctly — real hook + real component + MSW.
 *
 * Deliberately does NOT mock useInventorySummary or InventoryCounters so the
 * full data flow is exercised: fetch → hook state → component render.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";
import { server } from "../../test/mocks/server";
import { CostDashboardPage } from "./index";

vi.mock("echarts-for-react", () => ({
  default: vi.fn(() => <div data-testid="echarts" />),
}));

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
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
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

vi.mock("../../providers/TenantContext", () => ({
  // GAP-100 Category B: appStatus/readiness removed — they move to useReadiness().
  useTenant: vi.fn(() => ({
    currentTenant: {
      tenant_name: "acme",
      tenant_id: "t-001",
      ecosystem: "ccloud",
      dates_pending: 0,
      dates_calculated: 10,
      last_calculated_date: null,
    },
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
  useReadiness: vi.fn(() => ({
    appStatus: "ready" as const,
    readiness: null,
  })),
}));

vi.mock("../../components/charts/ChartCard", () => ({
  ChartCard: vi.fn(({ children }: { children: ReactNode }) => (
    <div data-testid="chart-card">{children}</div>
  )),
}));

vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(() => <div data-testid="filter-panel" />),
}));

vi.mock("../../components/charts/CostTrendChart", () => ({
  CostTrendChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByIdentityChart", () => ({
  CostByIdentityChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByProductChart", () => ({
  CostByProductChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/CostByResourceChart", () => ({
  CostByResourceChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/DimensionPieChart", () => ({
  DimensionPieChart: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/DataAvailabilityTimeline", () => ({
  DataAvailabilityTimeline: vi.fn(() => <div />),
}));
vi.mock("../../components/charts/ProductChartTypeToggle", () => ({
  ProductChartTypeToggle: vi.fn(() => <div />),
}));

vi.mock("../../components/dashboard/AllocationIssuesTable", () => ({
  AllocationIssuesTable: vi.fn(() => (
    <div data-testid="allocation-issues-table" />
  )),
}));

vi.mock("../../components/pivotPanel/TagPivotPanel", () => ({
  TagPivotPanel: vi.fn(() => null),
}));

// Partial antd mock — includes all components used by InventoryCounters
vi.mock("antd", () => ({
  Card: ({ children }: { children: ReactNode }) => (
    <div data-testid="card">{children}</div>
  ),
  Col: ({ children }: { children?: ReactNode }) => (
    <div data-testid="col">{children}</div>
  ),
  Row: ({ children }: { children: ReactNode }) => (
    <div data-testid="row">{children}</div>
  ),
  Skeleton: ({ active }: { active?: boolean }) => (
    <div data-testid="skeleton" data-active={active} />
  ),
  Statistic: ({ title, value }: { title: string; value: string | number }) => (
    <div data-testid="statistic">
      <div data-testid="statistic-title">{title}</div>
      <div data-testid="statistic-value">{value}</div>
    </div>
  ),
  Typography: {
    Title: ({ children }: { children: ReactNode }) => <h3>{children}</h3>,
    Text: ({ children }: { children: ReactNode }) => (
      <span data-testid="typography-text">{children}</span>
    ),
  },
  Collapse: ({
    items,
  }: {
    items: Array<{ key: string; label: string; children: ReactNode }>;
  }) => (
    <div data-testid="collapse">
      {items.map((item) => (
        <div key={item.key}>
          <span data-testid="collapse-label">{item.label}</span>
          <div data-testid="collapse-content">{item.children}</div>
        </div>
      ))}
    </div>
  ),
  Empty: ({ description }: { description?: string }) => (
    <div data-testid="empty">{description}</div>
  ),
  Radio: {
    Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    Button: ({ children }: { children: ReactNode }) => (
      <button>{children}</button>
    ),
  },
}));

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  const testQueryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  });
  return (
    <QueryClientProvider client={testQueryClient}>
      <MemoryRouter>{children}</MemoryRouter>
    </QueryClientProvider>
  );
}

describe("InventoryCounters wiring integration", () => {
  it("renders inventory count cards from MSW response via real hook + real component", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/inventory/summary", () =>
        HttpResponse.json({
          resource_counts: {
            kafka_cluster: { total: 5, active: 4, deleted: 1 },
            connector: { total: 3, active: 3, deleted: 0 },
          },
          identity_counts: {
            service_account: { total: 12, active: 10, deleted: 2 },
            user: { total: 3, active: 3, deleted: 0 },
          },
        }),
      ),
    );

    render(<CostDashboardPage />, { wrapper });

    await waitFor(() => {
      const titles = screen
        .getAllByTestId("statistic-title")
        .map((el) => el.textContent);
      expect(titles).toContain("Kafka Cluster");
    });

    const titles = screen
      .getAllByTestId("statistic-title")
      .map((el) => el.textContent);
    expect(titles).toContain("Kafka Cluster");
    expect(titles).toContain("Connector");
    expect(titles).toContain("Service Account");
    expect(titles).toContain("User");

    const values = screen
      .getAllByTestId("statistic-value")
      .map((el) => el.textContent);
    expect(values).toContain("5"); // kafka_cluster total
    expect(values).toContain("3"); // connector total
    expect(values).toContain("12"); // service_account total
  });
});
