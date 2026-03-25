import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { InventorySummaryResponse } from "../../../types/api";
import { InventoryCounters } from "../InventoryCounters";

vi.mock("antd", () => ({
  Card: ({ children }: { children: ReactNode }) => (
    <div data-testid="card">{children}</div>
  ),
  Col: ({
    children,
    xs,
    sm,
    md,
  }: {
    children?: ReactNode;
    xs?: number;
    sm?: number;
    md?: number;
  }) => (
    <div data-testid="col" data-xs={xs} data-sm={sm} data-md={md}>
      {children}
    </div>
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
    Text: ({
      children,
      type,
    }: {
      children: ReactNode;
      type?: string;
    }) => <span data-testid="typography-text" data-type={type}>{children}</span>,
  },
  Collapse: ({
    items,
    defaultActiveKey,
  }: {
    items: Array<{ key: string; label: string; children: ReactNode }>;
    defaultActiveKey?: string[];
  }) => (
    <div data-testid="collapse" data-default-active-key={JSON.stringify(defaultActiveKey ?? [])}>
      {items.map((item) => (
        <div key={item.key} data-testid="collapse-panel">
          <button
            data-testid="collapse-header"
            onClick={() => {
              const panel = document.querySelector(
                `[data-panel-key="${item.key}"]`,
              );
              if (panel) {
                (panel as HTMLElement).style.display =
                  (panel as HTMLElement).style.display === "none" ? "block" : "none";
              }
            }}
          >
            {item.label}
          </button>
          <div data-testid="collapse-content" data-panel-key={item.key}>
            {item.children}
          </div>
        </div>
      ))}
    </div>
  ),
  Empty: ({ description }: { description?: string }) => (
    <div data-testid="empty" data-description={description}>
      {description}
    </div>
  ),
}));

const MOCK_DATA: InventorySummaryResponse = {
  resource_counts: {
    kafka_cluster: { total: 5, active: 4, deleted: 1 },
    connector: { total: 3, active: 3, deleted: 0 },
  },
  identity_counts: {
    service_account: { total: 12, active: 10, deleted: 2 },
    user: { total: 3, active: 3, deleted: 0 },
  },
};

describe("InventoryCounters", () => {
  it("renders correct count cards from mock data", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    const titles = screen.getAllByTestId("statistic-title").map((el) => el.textContent);
    expect(titles).toContain("Kafka Cluster");
    expect(titles).toContain("Connector");
    expect(titles).toContain("Service Account");
    expect(titles).toContain("User");

    const values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values).toContain("5");
    expect(values).toContain("3");
    expect(values).toContain("12");
  });

  it("renders active/deleted secondary text", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    expect(screen.getByText("Active: 4 / Deleted: 1")).toBeDefined();
  });

  it("converts snake_case keys to Title Case labels", () => {
    const data: InventorySummaryResponse = {
      resource_counts: {
        kafka_cluster: { total: 1, active: 1, deleted: 0 },
        identity_pool: { total: 2, active: 2, deleted: 0 },
      },
      identity_counts: {
        service_account: { total: 5, active: 5, deleted: 0 },
      },
    };
    render(<InventoryCounters data={data} isLoading={false} error={null} />);

    const titles = screen.getAllByTestId("statistic-title").map((el) => el.textContent);
    expect(titles).toContain("Kafka Cluster");
    expect(titles).toContain("Identity Pool");
    expect(titles).toContain("Service Account");
  });

  it("renders a Collapse component (collapsed by default — no defaultActiveKey)", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    const collapse = screen.getByTestId("collapse");
    expect(collapse).toBeDefined();
    // defaultActiveKey must be absent/empty — panel starts closed
    const activeKey = JSON.parse(collapse.getAttribute("data-default-active-key") ?? "[]");
    expect(activeKey).toHaveLength(0);
  });

  it("renders collapse header labelled Inventory", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    const header = screen.getByTestId("collapse-header");
    expect(header.textContent).toBe("Inventory");
  });

  it("shows 3 skeleton placeholders per CounterRow section when isLoading=true", () => {
    render(<InventoryCounters data={null} isLoading={true} error={null} />);

    const skeletons = screen.getAllByTestId("skeleton");
    // 2 sections (resources + identities), 3 skeletons each = 6 total
    expect(skeletons.length).toBe(6);
    expect(screen.queryByTestId("statistic")).toBeNull();
  });

  it("renders Empty with 'No inventory data' when counts are empty and no error", () => {
    const emptyData: InventorySummaryResponse = {
      resource_counts: {},
      identity_counts: {},
    };
    render(<InventoryCounters data={emptyData} isLoading={false} error={null} />);

    const empties = screen.getAllByTestId("empty");
    expect(empties.length).toBe(2); // one per section
    empties.forEach((el) => {
      expect(el.getAttribute("data-description")).toBe("No inventory data");
    });
  });

  it("renders em dash values on error when data is populated", () => {
    render(
      <InventoryCounters data={MOCK_DATA} isLoading={false} error="fetch failed" />,
    );

    const values = screen.getAllByTestId("statistic-value").map((el) => el.textContent);
    expect(values.every((v) => v === "—")).toBe(true);
    expect(values.length).toBeGreaterThan(0);

    // secondary active/deleted text must NOT be present when error
    expect(screen.queryByText(/Active: \d+ \/ Deleted: \d+/)).toBeNull();
  });

  it("renders em dash text placeholder (not Empty) when error occurs on first load (data=null)", () => {
    render(
      <InventoryCounters data={null} isLoading={false} error="network error" />,
    );

    // Empty component must NOT render — error overrides empty-state guard
    expect(screen.queryByTestId("empty")).toBeNull();

    // "—" text placeholder must be present
    const texts = screen.getAllByText("—");
    expect(texts.length).toBeGreaterThan(0);
  });

  it("card Col elements use xs=12 for mobile (2 per row)", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    const cols = screen.getAllByTestId("col");
    const xsCols = cols.filter((el) => el.getAttribute("data-xs") === "12");
    expect(xsCols.length).toBeGreaterThan(0);
  });

  it("card Col elements use md=6 for desktop (4 per row)", () => {
    render(<InventoryCounters data={MOCK_DATA} isLoading={false} error={null} />);

    const cols = screen.getAllByTestId("col");
    const mdCols = cols.filter((el) => el.getAttribute("data-md") === "6");
    expect(mdCols.length).toBeGreaterThan(0);
  });
});
