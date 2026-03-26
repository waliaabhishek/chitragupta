import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BillingFilters } from "../../types/filters";
import { BillingFilterPanel } from "./BillingFilterPanel";

// Mock antd to make Select and Input testable in jsdom.
vi.mock("antd", async () => {
  const { useState } = await import("react");

  type FilterFn = (input: string, option?: { label?: unknown }) => boolean;

  interface MockSelectProps {
    value?: string;
    onChange?: (val: string | undefined) => void;
    options?: { label: string; value: string }[];
    placeholder?: string;
    allowClear?: boolean;
    style?: object;
    loading?: boolean;
    showSearch?: boolean;
    filterOption?: FilterFn | boolean;
  }

  const MockSelect = ({
    value,
    onChange,
    options,
    placeholder,
    style,
    loading,
    showSearch,
    filterOption,
  }: MockSelectProps) => {
    const [search, setSearch] = useState("");

    const displayedOptions =
      showSearch && typeof filterOption === "function" && search
        ? options?.filter((o) => (filterOption as FilterFn)(search, o))
        : options;

    return (
      <>
        {showSearch && (
          <input
            data-testid={`select-search-${placeholder}`}
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        )}
        <select
          value={value ?? ""}
          onChange={(e) => onChange?.(e.target.value || undefined)}
          style={style}
          data-loading={loading ? "true" : undefined}
        >
          <option value="">{placeholder ?? "Select"}</option>
          {displayedOptions?.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      </>
    );
  };

  return {
    Button: ({
      children,
      onClick,
    }: {
      children: React.ReactNode;
      onClick?: () => void;
    }) => (
      <button type="button" onClick={onClick}>
        {children}
      </button>
    ),
    DatePicker: {
      RangePicker: ({
        onChange,
      }: {
        value?: unknown;
        onChange?: (
          dates: [
            { format: (f: string) => string } | null,
            { format: (f: string) => string } | null,
          ] | null,
        ) => void;
        allowClear?: boolean;
      }) => (
        <div>
          <button
            type="button"
            data-testid="date-range-set"
            onClick={() =>
              onChange?.([
                { format: () => "2026-02-01" },
                { format: () => "2026-02-28" },
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
      ({
        children,
      }: {
        children: React.ReactNode;
        layout?: string;
        style?: object;
      }) => <form>{children}</form>,
      {
        Item: ({
          children,
        }: {
          children: React.ReactNode;
          label?: string;
        }) => <div>{children}</div>,
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
    Select: MockSelect,
  };
});

// Suppress fetch calls from the resource-loading useEffect
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [], total: 0, page: 1, page_size: 1000, pages: 0 }),
    }),
  );
});

const defaultFilters: BillingFilters = {
  start_date: "2026-01-01",
  end_date: "2026-01-31",
  product_type: null,
  resource_id: null,
  timezone: "UTC",
};

describe("BillingFilterPanel", () => {
  it("renders timezone Select", () => {
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Resource and Timezone selects are comboboxes (index 0 and 1)
    const allSelects = screen.getAllByRole("combobox");
    expect(allSelects).toHaveLength(2);
  });

  it("calls onChange when timezone select changes", () => {
    const onChange = vi.fn();
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Timezone is the second combobox (index 1): resource, timezone
    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[1], { target: { value: "America/Chicago" } });

    expect(onChange).toHaveBeenCalledWith("timezone", "America/Chicago");
  });

  it("calls onChange with null when timezone select is cleared", () => {
    const onChange = vi.fn();
    render(
      <BillingFilterPanel
        filters={{ ...defaultFilters, timezone: "America/Chicago" }}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[1], { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("timezone", null);
  });

  it("renders Reset button and calls onReset", () => {
    const onReset = vi.fn();
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={onReset}
        tenantName="t1"
      />,
    );

    fireEvent.click(screen.getByText("Reset"));
    expect(onReset).toHaveBeenCalledOnce();
  });

  it("renders Refresh Data button when onRefresh is provided", () => {
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        onRefresh={vi.fn()}
        tenantName="t1"
      />,
    );

    expect(screen.getByText("Refresh Data")).toBeInTheDocument();
  });

  it("does not render Refresh Data button when onRefresh is omitted", () => {
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    expect(screen.queryByText("Refresh Data")).toBeNull();
  });

  it("calls onBatchChange with both dates when date range is set", () => {
    const onChange = vi.fn();
    const onBatchChange = vi.fn();
    render(
      <BillingFilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onBatchChange={onBatchChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    fireEvent.click(screen.getByTestId("date-range-set"));

    expect(onBatchChange).toHaveBeenCalledWith({
      start_date: "2026-02-01",
      end_date: "2026-02-28",
    });
    expect(onChange).not.toHaveBeenCalled();
  });
});
