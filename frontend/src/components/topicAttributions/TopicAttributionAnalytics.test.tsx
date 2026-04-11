import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { TopicAttributionAnalytics } from "./TopicAttributionAnalytics";
import type { TopicAttributionFilters } from "../../types/filters";

vi.mock("antd", () => ({
  Col: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Row: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Radio: Object.assign(
    ({ children }: { children: ReactNode }) => <label>{children}</label>,
    {
      Button: ({ children }: { children: ReactNode }) => (
        <button>{children}</button>
      ),
      Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
    },
  ),
}));

vi.mock("../charts/ChartCard", () => ({
  ChartCard: ({
    title,
    subtitle,
    children,
  }: {
    title: string;
    subtitle?: string;
    children: ReactNode;
  }) => (
    <div data-testid="chart-card">
      <div data-testid="chart-card-title">{title}</div>
      {subtitle && <div data-testid="chart-card-subtitle">{subtitle}</div>}
      {children}
    </div>
  ),
}));

vi.mock("./charts/TopTopicsChart", () => ({ TopTopicsChart: () => null }));
vi.mock("./charts/CostCompositionChart", () => ({
  CostCompositionChart: () => null,
}));
vi.mock("./charts/CostVelocityChart", () => ({
  CostVelocityChart: () => null,
}));
vi.mock("./charts/AttributionMethodDonut", () => ({
  AttributionMethodDonut: () => null,
}));
vi.mock("./charts/ZombieTopicsTable", () => ({
  ZombieTopicsTable: () => null,
}));
vi.mock("./charts/EnvironmentCostChart", () => ({
  EnvironmentCostChart: () => null,
}));
vi.mock("./charts/TopClustersCostChart", () => ({
  TopClustersCostChart: () => null,
}));
vi.mock("./charts/ClusterConcentrationRiskChart", () => ({
  ClusterConcentrationRiskChart: () => null,
}));
vi.mock("./charts/ProductTypeMixChart", () => ({
  ProductTypeMixChart: () => null,
}));
vi.mock("./charts/PivotedCostBreakdown", () => ({
  PivotedCostBreakdown: () => null,
}));

vi.mock("../../hooks/useTopicAttributionAggregation", () => ({
  useTopicAttributionAggregation: () => ({
    data: { buckets: [] },
    isLoading: false,
    error: null,
    refetch: vi.fn(),
  }),
}));

const defaultFilters: TopicAttributionFilters = {
  start_date: "2024-01-01",
  end_date: "2024-01-31",
  cluster_resource_id: null,
  topic_name: null,
  product_type: null,
  attribution_method: null,
  timezone: null,
  tag_key: null,
  tag_value: null,
};

describe("TopicAttributionAnalytics", () => {
  it("Cost Velocity ChartCard is wired with correct subtitle", () => {
    render(
      <TopicAttributionAnalytics tenantName="test" filters={defaultFilters} />,
    );
    const subtitles = screen.getAllByTestId("chart-card-subtitle");
    expect(
      subtitles.some(
        (el) =>
          el.textContent ===
          "Top 10 topics by largest period-over-period cost change",
      ),
    ).toBe(true);
  });
});
