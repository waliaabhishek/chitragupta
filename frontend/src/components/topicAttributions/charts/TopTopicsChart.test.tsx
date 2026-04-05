import { fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { TopTopicsChart } from "./TopTopicsChart";
import type { TopicAttributionAggregationBucket } from "../../../types/api";

let lastOption: Record<string, unknown> = {};

// Mock echarts-for-react to avoid canvas issues in jsdom
vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: Record<string, unknown> }) => {
    lastOption = option;
    return (
      <div
        data-testid="echarts"
        data-series-type={
          Array.isArray(option.series) && option.series.length > 0
            ? (option.series[0] as { type: string }).type
            : ""
        }
      />
    );
  },
}));

// Mock antd Radio.Group for toggle
vi.mock("antd", () => ({
  Radio: Object.assign(
    ({ children }: { children: React.ReactNode }) => <span>{children}</span>,
    {
      Group: ({
        value,
        onChange,
        children,
      }: {
        value: string;
        onChange: (e: { target: { value: string } }) => void;
        children: React.ReactNode;
      }) => (
        <div data-testid="radio-group" data-value={value}>
          <button
            data-testid="radio-treemap"
            onClick={() => onChange({ target: { value: "Treemap" } })}
          >
            Treemap
          </button>
          <button
            data-testid="radio-bar"
            onClick={() => onChange({ target: { value: "Bar" } })}
          >
            Bar
          </button>
          {children}
        </div>
      ),
      Button: ({ children }: { children: React.ReactNode }) => (
        <span>{children}</span>
      ),
    },
  ),
}));

function makeBucket(
  topicName: string,
  amount: string,
  timeBucket = "2026-01-01",
): TopicAttributionAggregationBucket {
  return {
    dimensions: { topic_name: topicName },
    time_bucket: timeBucket,
    total_amount: amount,
    row_count: 1,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  lastOption = {};
});

describe("TopTopicsChart", () => {
  it("renders with treemap as the default chart type", () => {
    const data = [
      makeBucket("topic-a", "100.00"),
      makeBucket("topic-b", "50.00"),
    ];
    render(<TopTopicsChart data={data} />);

    const chart = screen.getByTestId("echarts");
    expect(chart).toBeTruthy();
    expect(chart.getAttribute("data-series-type")).toBe("treemap");
  });

  it("renders empty without crashing when data is empty", () => {
    render(<TopTopicsChart data={[]} />);
    expect(screen.getByTestId("echarts")).toBeTruthy();
  });

  it("shows toggle control for chart type", () => {
    const data = [makeBucket("topic-a", "100.00")];
    render(<TopTopicsChart data={data} />);
    expect(screen.getByTestId("radio-group")).toBeTruthy();
  });

  it("switches to bar chart when Bar radio is clicked", () => {
    const data = [makeBucket("topic-a", "100.00")];
    render(<TopTopicsChart data={data} />);

    fireEvent.click(screen.getByTestId("radio-bar"));

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("bar");
  });

  it("switches back to treemap when Treemap radio is clicked after bar", () => {
    const data = [makeBucket("topic-a", "100.00")];
    render(<TopTopicsChart data={data} />);

    fireEvent.click(screen.getByTestId("radio-bar"));
    fireEvent.click(screen.getByTestId("radio-treemap"));

    const chart = screen.getByTestId("echarts");
    expect(chart.getAttribute("data-series-type")).toBe("treemap");
  });
});

describe("TASK-203: tooltip valueFormatter", () => {
  it("TopTopicsChart bar view — valueFormatter present", () => {
    const data = [makeBucket("topic-a", "100.00")];
    render(<TopTopicsChart data={data} />);
    fireEvent.click(screen.getByTestId("radio-bar"));
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    expect(typeof tooltip?.valueFormatter).toBe("function");
  });

  it("TopTopicsChart bar view — valueFormatter output", () => {
    const data = [makeBucket("topic-a", "100.00")];
    render(<TopTopicsChart data={data} />);
    fireEvent.click(screen.getByTestId("radio-bar"));
    const tooltip = lastOption.tooltip as Record<string, unknown>;
    const valueFormatter = tooltip?.valueFormatter as (v: number) => string;
    expect(valueFormatter(1996.9649999999929)).toBe("$1,996.96");
  });
});
