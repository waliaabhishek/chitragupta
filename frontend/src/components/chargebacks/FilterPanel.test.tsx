import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ChargebackFilters } from "../../types/filters";
import { FilterPanel } from "./FilterPanel";

// Mock antd to make DatePicker.RangePicker and Select testable in jsdom.
vi.mock("antd", () => ({
  Button: ({ children, onClick }: { children: React.ReactNode; onClick?: () => void }) => (
    <button type="button" onClick={onClick}>{children}</button>
  ),
  DatePicker: {
    RangePicker: ({
      onChange,
    }: {
      value?: unknown;
      onChange?: (
        dates: [{ format: (f: string) => string } | null, { format: (f: string) => string } | null] | null,
      ) => void;
      allowClear?: boolean;
    }) => (
      <div>
        <button
          type="button"
          data-testid="date-range-set"
          onClick={() =>
            onChange?.([
              { format: (_: string) => "2026-02-01" },
              { format: (_: string) => "2026-02-28" },
            ])
          }
        >
          Set Dates
        </button>
        <button
          type="button"
          data-testid="date-range-clear"
          onClick={() => onChange?.(null)}
        >
          Clear Dates
        </button>
      </div>
    ),
  },
  Form: Object.assign(
    ({ children }: { children: React.ReactNode; layout?: string; style?: object }) => (
      <form>{children}</form>
    ),
    {
      Item: ({ children }: { children: React.ReactNode; label?: string }) => (
        <div>{children}</div>
      ),
    },
  ),
  Input: ({
    placeholder,
    value,
    onChange,
    style,
  }: {
    placeholder?: string;
    value?: string;
    onChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
    allowClear?: boolean;
    style?: object;
  }) => (
    <input
      placeholder={placeholder}
      value={value}
      onChange={onChange}
      style={style}
    />
  ),
  Select: ({
    value,
    onChange,
    options,
    placeholder,
    style,
  }: {
    value?: string;
    onChange?: (val: string | undefined) => void;
    options?: { label: string; value: string }[];
    placeholder?: string;
    allowClear?: boolean;
    style?: object;
  }) => (
    <select
      value={value ?? ""}
      onChange={(e) => onChange?.(e.target.value || undefined)}
      style={style}
    >
      <option value="">{placeholder ?? "Select"}</option>
      {options?.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  ),
  Space: ({ children }: { children: React.ReactNode; wrap?: boolean }) => (
    <span>{children}</span>
  ),
}));

const defaultFilters: ChargebackFilters = {
  start_date: "2026-01-01",
  end_date: "2026-01-31",
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
};

describe("FilterPanel", () => {
  it("renders all filter inputs", () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
      />,
    );

    expect(screen.getByPlaceholderText("Identity ID")).toBeTruthy();
    expect(screen.getByPlaceholderText("Product type")).toBeTruthy();
    expect(screen.getByPlaceholderText("Resource ID")).toBeTruthy();
    expect(screen.getByText("Reset")).toBeTruthy();
  });

  it("calls onChange when identity input changes", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Identity ID");
    fireEvent.change(input, { target: { value: "user-1" } });

    expect(onChange).toHaveBeenCalledWith("identity_id", "user-1");
  });

  it("calls onChange with null when identity input is cleared", () => {
    const onChange = vi.fn();
    const filtersWithIdentity: ChargebackFilters = {
      ...defaultFilters,
      identity_id: "user-1",
    };
    render(
      <FilterPanel
        filters={filtersWithIdentity}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    const input = screen.getByPlaceholderText("Identity ID");
    fireEvent.change(input, { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("identity_id", null);
  });

  it("calls onReset when Reset button clicked", () => {
    const onReset = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={onReset}
      />,
    );

    fireEvent.click(screen.getByText("Reset"));
    expect(onReset).toHaveBeenCalledOnce();
  });

  it("calls onChange when product type changes", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("Product type"), {
      target: { value: "kafka" },
    });
    expect(onChange).toHaveBeenCalledWith("product_type", "kafka");
  });

  it("calls onChange for both dates when date range is set", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTestId("date-range-set"));

    expect(onChange).toHaveBeenCalledWith("start_date", "2026-02-01");
    expect(onChange).toHaveBeenCalledWith("end_date", "2026-02-28");
  });

  it("calls onChange with null for both dates when date range is cleared", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTestId("date-range-clear"));

    expect(onChange).toHaveBeenCalledWith("start_date", null);
    expect(onChange).toHaveBeenCalledWith("end_date", null);
  });

  it("calls onChange when cost type select changes", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    const select = screen.getByRole("combobox");
    fireEvent.change(select, { target: { value: "usage" } });

    expect(onChange).toHaveBeenCalledWith("cost_type", "usage");
  });

  it("calls onChange with null when cost type select is cleared", () => {
    const onChange = vi.fn();
    const filtersWithCostType: ChargebackFilters = {
      ...defaultFilters,
      cost_type: "usage",
    };
    render(
      <FilterPanel
        filters={filtersWithCostType}
        onChange={onChange}
        onReset={vi.fn()}
      />,
    );

    const select = screen.getByRole("combobox");
    fireEvent.change(select, { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("cost_type", null);
  });
});
