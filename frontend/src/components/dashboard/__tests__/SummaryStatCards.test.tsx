import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import type { AggregationResponse } from "../../../types/api";
import { SummaryStatCards } from "../SummaryStatCards";
import { CostDashboardPage } from "../../../pages/dashboard/index";

vi.mock("echarts-for-react", () => ({
  default: () => <div data-testid="mock-chart" />,
}));

vi.mock("antd", () => ({
  Card: ({ children }: { children: ReactNode }) => (
    <div data-testid="card">{children}</div>
  ),
  Col: ({ children }: { children: ReactNode }) => (
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
    Text: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  },
  Radio: {
    Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    Button: ({ children }: { children: ReactNode }) => <button>{children}</button>,
  },
}));

vi.mock("../../../utils/aggregation", () => ({
  formatCurrency: (amount: number) =>
    `$${amount.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`,
}));

vi.mock("../../../components/charts/ChartCard", () => ({
  ChartCard: ({ title }: { title: string }) => (
    <div data-testid="chart-card">
      <div data-testid="chart-card-title">{title}</div>
    </div>
  ),
}));

vi.mock("../../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: () => <div data-testid="filter-panel" />,
}));

vi.mock("../../../hooks/useAggregation", () => ({
  useAggregation: vi.fn(() => ({
    data: {
      buckets: [],
      total_amount: "1500.00",
      usage_amount: "1200.00",
      shared_amount: "300.00",
      total_rows: 0,
    },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../../hooks/useDataAvailability", () => ({
  useDataAvailability: vi.fn(() => ({
    dates: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../../hooks/useInventorySummary", () => ({
  useInventorySummary: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  })),
}));

vi.mock("../../../components/dashboard/InventoryCounters", () => ({
  InventoryCounters: () => <div data-testid="inventory-counters" />,
}));

vi.mock("../../../components/charts/DataAvailabilityTimeline", () => ({
  DataAvailabilityTimeline: () => <div data-testid="data-availability-timeline" />,
}));

vi.mock("../../../providers/TenantContext", () => ({
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
    appStatus: "ready" as const,
    readiness: null,
    isReadOnly: false,
  })),
}));

const mockData: AggregationResponse = {
  buckets: [],
  total_amount: "1234.56",
  usage_amount: "900.00",
  shared_amount: "334.56",
  total_rows: 0,
};

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return (
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      {children}
    </MemoryRouter>
  );
}

describe("SummaryStatCards", () => {
  it("renders Total Cost, Usage Cost, Shared Cost headings with correct formatted values", () => {
    render(<SummaryStatCards data={mockData} isLoading={false} />);

    const titles = screen.getAllByTestId("statistic-title").map((el) => el.textContent);
    expect(titles).toContain("Total Cost");
    expect(titles).toContain("Usage Cost");
    expect(titles).toContain("Shared Cost");

    const values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toContain("$1,234.56");
    expect(values).toContain("$900.00");
    expect(values).toContain("$334.56");
  });

  it("shows Skeleton elements and no Statistic when isLoading=true and data=null", () => {
    render(<SummaryStatCards data={null} isLoading={true} />);

    const skeletons = screen.getAllByTestId("skeleton");
    expect(skeletons.length).toBeGreaterThan(0);
    expect(screen.queryByTestId("statistic")).toBeNull();
  });

  it("shows $0.00 for all three cards when data=null and not loading", () => {
    render(<SummaryStatCards data={null} isLoading={false} />);

    const values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toHaveLength(3);
    expect(values.every((v) => v === "$0.00")).toBe(true);
  });

  it("shows em dash for all three cards when error is set", () => {
    render(<SummaryStatCards data={null} isLoading={false} error="some error" />);

    const values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toHaveLength(3);
    expect(values.every((v) => v === "—")).toBe(true);
  });
});

describe("DashboardContent integration", () => {
  it("renders SummaryStatCards headings before chart cards in CostDashboardPage", () => {
    render(<CostDashboardPage />, { wrapper });

    const statTitles = screen.getAllByTestId("statistic-title");
    expect(statTitles.map((el) => el.textContent)).toContain("Total Cost");

    const chartCards = screen.getAllByTestId("chart-card");
    expect(chartCards.length).toBeGreaterThan(0);

    // Stat card titles must appear before the first chart card in the DOM
    expect(
      statTitles[0].compareDocumentPosition(chartCards[0]) & Node.DOCUMENT_POSITION_FOLLOWING,
    ).toBeTruthy();
  });
});

describe("SummaryStatCards filter reactivity", () => {
  it("updates displayed values when data changes (simulating filter change)", () => {
    const { rerender } = render(<SummaryStatCards data={mockData} isLoading={false} />);

    let values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toContain("$1,234.56");
    expect(values).toContain("$900.00");
    expect(values).toContain("$334.56");

    const updatedData: AggregationResponse = {
      buckets: [],
      total_amount: "500.00",
      usage_amount: "400.00",
      shared_amount: "100.00",
      total_rows: 0,
    };

    rerender(<SummaryStatCards data={updatedData} isLoading={false} />);

    values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toContain("$500.00");
    expect(values).toContain("$400.00");
    expect(values).toContain("$100.00");
  });
});
