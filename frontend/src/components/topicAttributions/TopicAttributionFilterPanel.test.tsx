import { render } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TopicAttributionFilterPanel } from "./TopicAttributionFilterPanel";
import type { TopicAttributionFilters } from "../../types/filters";

vi.mock("antd", () => ({
  Form: Object.assign(
    ({ children }: { children: ReactNode }) => <form>{children}</form>,
    {
      Item: ({ children }: { children: ReactNode }) => <div>{children}</div>,
      useForm: () => [{}],
    },
  ),
  Select: ({ placeholder }: { placeholder?: string }) => (
    <select data-placeholder={placeholder} />
  ),
  Input: (props: { placeholder?: string; value?: string }) => (
    <input placeholder={props.placeholder} value={props.value} readOnly />
  ),
  DatePicker: Object.assign(() => <input type="date" />, {
    RangePicker: () => <input type="date" data-testid="range-picker" />,
  }),
  Button: ({ children }: { children: ReactNode }) => (
    <button>{children}</button>
  ),
  Space: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Tooltip: ({ children, title }: { children: ReactNode; title?: string }) => (
    <div data-tooltip={title}>{children}</div>
  ),
}));

vi.mock("@ant-design/icons", () => ({
  FilterOutlined: () => <span />,
  ReloadOutlined: () => <span />,
}));

vi.mock("@tanstack/react-query", () => ({
  useQuery: vi.fn(() => ({ data: undefined })),
}));

vi.mock("../../api/topicAttributions", () => ({
  fetchTopicAttributionDates: vi.fn(),
}));

const defaultFilters: TopicAttributionFilters = {
  start_date: "2026-01-01",
  end_date: "2026-01-31",
  cluster_resource_id: null,
  topic_name: null,
  product_type: null,
  attribution_method: null,
  timezone: "UTC",
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe("TopicAttributionFilterPanel — Attribution Method tooltip", () => {
  it("shows 'Applies to table view only' tooltip when activeTab is analytics", () => {
    render(
      <TopicAttributionFilterPanel
        tenantName="acme"
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        activeTab="analytics"
      />,
    );
    expect(
      document.querySelector('[data-tooltip="Applies to table view only"]'),
    ).not.toBeNull();
  });

  it("does not show tooltip when activeTab is table", () => {
    render(
      <TopicAttributionFilterPanel
        tenantName="acme"
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        activeTab="table"
      />,
    );
    expect(
      document.querySelector('[data-tooltip="Applies to table view only"]'),
    ).toBeNull();
  });

  it("does not show tooltip when activeTab is not provided", () => {
    render(
      <TopicAttributionFilterPanel
        tenantName="acme"
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
      />,
    );
    expect(
      document.querySelector('[data-tooltip="Applies to table view only"]'),
    ).toBeNull();
  });
});
