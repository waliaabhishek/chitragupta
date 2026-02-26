import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ChartCard } from "./ChartCard";

// Mock antd components used inside ChartCard
vi.mock("antd", () => ({
  Card: ({
    title,
    children,
    extra,
  }: {
    title: string;
    children: React.ReactNode;
    extra?: React.ReactNode;
    style?: object;
  }) => (
    <div data-testid="card">
      <div data-testid="card-title">{title}</div>
      {extra && <div data-testid="card-extra">{extra}</div>}
      <div data-testid="card-body">{children}</div>
    </div>
  ),
  Spin: () => <div data-testid="spin" />,
  Result: ({
    title,
    subTitle,
    extra,
  }: {
    status?: string;
    title?: string;
    subTitle?: string;
    extra?: React.ReactNode;
  }) => (
    <div data-testid="result">
      {title && <div data-testid="result-title">{title}</div>}
      {subTitle && <div data-testid="result-subtitle">{subTitle}</div>}
      {extra && <div data-testid="result-extra">{extra}</div>}
    </div>
  ),
  Button: ({
    children,
    onClick,
  }: {
    children: React.ReactNode;
    onClick?: () => void;
    type?: string;
  }) => (
    <button data-testid="retry-button" onClick={onClick}>
      {children}
    </button>
  ),
}));

describe("ChartCard", () => {
  it("renders title and children", () => {
    render(<ChartCard title="Test Chart"><div>chart content</div></ChartCard>);
    expect(screen.getByTestId("card-title").textContent).toBe("Test Chart");
    expect(screen.getByText("chart content")).toBeInTheDocument();
  });

  it("shows Spin when loading", () => {
    render(<ChartCard title="Loading Chart" loading><div>hidden</div></ChartCard>);
    expect(screen.getByTestId("spin")).toBeInTheDocument();
    expect(screen.queryByText("hidden")).toBeNull();
  });

  it("renders extra content", () => {
    render(
      <ChartCard title="With Extra" extra={<button>Action</button>}>
        <div>content</div>
      </ChartCard>,
    );
    expect(screen.getByTestId("card-extra")).toBeInTheDocument();
    expect(screen.getByText("Action")).toBeInTheDocument();
  });

  it("shows Result with error message when error is set", () => {
    render(
      <ChartCard title="Error Chart" error="Network timeout">
        <div>hidden content</div>
      </ChartCard>,
    );
    expect(screen.getByTestId("result")).toBeInTheDocument();
    expect(screen.getByTestId("result-subtitle").textContent).toBe("Network timeout");
    expect(screen.queryByText("hidden content")).toBeNull();
  });

  it("shows Retry button when error and onRetry are provided", () => {
    const onRetry = vi.fn();
    render(
      <ChartCard title="Error Chart" error="Failed to load" onRetry={onRetry}>
        <div>content</div>
      </ChartCard>,
    );
    expect(screen.getByTestId("retry-button")).toBeInTheDocument();
  });

  it("does not show Retry button when error is set but onRetry is not", () => {
    render(
      <ChartCard title="Error Chart" error="Failed to load">
        <div>content</div>
      </ChartCard>,
    );
    expect(screen.queryByTestId("retry-button")).toBeNull();
  });

  it("calls onRetry when Retry button is clicked", async () => {
    const onRetry = vi.fn();
    render(
      <ChartCard title="Error Chart" error="Failed to load" onRetry={onRetry}>
        <div>content</div>
      </ChartCard>,
    );
    await userEvent.click(screen.getByTestId("retry-button"));
    expect(onRetry).toHaveBeenCalledOnce();
  });
});
