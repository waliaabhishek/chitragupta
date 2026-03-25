import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { UseFilterOptionsResult } from "../../hooks/useFilterOptions";
import { useFilterOptions } from "../../hooks/useFilterOptions";
import type { ChargebackFilters } from "../../types/filters";
import { filterByLabel } from "../../utils/filterHelpers";
import { FilterPanel } from "./FilterPanel";

// Mock the useFilterOptions hook so FilterPanel doesn't make real HTTP calls.
vi.mock("../../hooks/useFilterOptions", () => ({
  useFilterOptions: vi.fn(
    (): UseFilterOptionsResult => ({
      identityOptions: [
        { label: "Alice (u-1)", value: "u-1" },
        { label: "Bob (u-2)", value: "u-2" },
      ],
      resourceOptions: [{ label: "Cluster 1 (r-1)", value: "r-1" }],
      productTypeOptions: [{ label: "KAFKA", value: "KAFKA" }],
      isLoading: false,
      error: null,
    }),
  ),
}));

// Mock antd to make Select and DatePicker.RangePicker testable in jsdom.
// Select is rendered as a native <select> with optional search input for showSearch.
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
    Space: ({ children }: { children: React.ReactNode; wrap?: boolean }) => (
      <span>{children}</span>
    ),
  };
});

const defaultFilters: ChargebackFilters = {
  start_date: "2026-01-01",
  end_date: "2026-01-31",
  identity_id: null,
  product_type: null,
  resource_id: null,
  cost_type: null,
};

const fixtureIdentityOptions = [
  { label: "Alice (u-1)", value: "u-1" },
  { label: "Bob (u-2)", value: "u-2" },
  { label: "Carol (u-3)", value: "u-3" },
];

describe("FilterPanel", () => {
  it("FilterPanel_renders_Select_dropdowns_not_Input", () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Identity, product type, resource should be Select (combobox), not free-text Input
    expect(screen.queryByPlaceholderText("Identity ID")).toBeNull();
    expect(screen.queryByPlaceholderText("Product type")).toBeNull();
    expect(screen.queryByPlaceholderText("Resource ID")).toBeNull();

    // All four Select dropdowns should be present
    const comboboxes = screen.getAllByRole("combobox");
    expect(comboboxes.length).toBeGreaterThanOrEqual(4);
  });

  it("FilterPanel_calls_onChange_identity_id_value_on_Select_change", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Identity is the first combobox (index 0)
    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[0], { target: { value: "u-1" } });

    expect(onChange).toHaveBeenCalledWith("identity_id", "u-1");
  });

  it("FilterPanel_calls_onChange_identity_id_null_on_Select_clear", () => {
    const onChange = vi.fn();
    const filtersWithIdentity: ChargebackFilters = {
      ...defaultFilters,
      identity_id: "u-1",
    };
    render(
      <FilterPanel
        filters={filtersWithIdentity}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[0], { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("identity_id", null);
  });

  it("FilterPanel_calls_onChange_product_type_value_on_Select_change", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Product type is the second combobox (index 1)
    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[1], { target: { value: "KAFKA" } });

    expect(onChange).toHaveBeenCalledWith("product_type", "KAFKA");
  });

  it("FilterPanel_calls_onChange_resource_id_value_on_Select_change", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Resource is the third combobox (index 2)
    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[2], { target: { value: "r-1" } });

    expect(onChange).toHaveBeenCalledWith("resource_id", "r-1");
  });

  it("calls onReset when Reset button clicked", () => {
    const onReset = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={onReset}
        tenantName="t1"
      />,
    );

    fireEvent.click(screen.getByText("Reset"));
    expect(onReset).toHaveBeenCalledOnce();
  });

  it("calls onChange for both dates when date range is set", () => {
    const onChange = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
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
        tenantName="t1"
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
        tenantName="t1"
      />,
    );

    // Cost type is the fourth combobox (index 3): identity, product_type, resource, cost_type
    const allSelects = screen.getAllByRole("combobox");
    expect(allSelects).toHaveLength(4);
    fireEvent.change(allSelects[3], { target: { value: "usage" } });

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
        tenantName="t1"
      />,
    );

    const allSelects = screen.getAllByRole("combobox");
    expect(allSelects).toHaveLength(4);
    fireEvent.change(allSelects[3], { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("cost_type", null);
  });

  it("FilterPanel_search_filters_options_in_identity_Select", () => {
    vi.mocked(useFilterOptions).mockReturnValue({
      identityOptions: fixtureIdentityOptions,
      resourceOptions: [],
      productTypeOptions: [],
      isLoading: false,
      error: null,
    });

    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Before search: all 3 options visible in identity select
    expect(screen.getByText("Alice (u-1)")).toBeTruthy();
    expect(screen.getByText("Bob (u-2)")).toBeTruthy();
    expect(screen.getByText("Carol (u-3)")).toBeTruthy();

    // Type "bob" into the identity search input
    const searchInput = screen.getByTestId("select-search-Any identity");
    fireEvent.change(searchInput, { target: { value: "bob" } });

    // After search: only "Bob" visible
    expect(screen.queryByText("Alice (u-1)")).toBeNull();
    expect(screen.getByText("Bob (u-2)")).toBeTruthy();
    expect(screen.queryByText("Carol (u-3)")).toBeNull();
  });

  it("FilterPanel_displays_loading_state_on_Select_components", () => {
    vi.mocked(useFilterOptions).mockReturnValue({
      identityOptions: [],
      resourceOptions: [],
      productTypeOptions: [],
      isLoading: true,
      error: null,
    });

    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    // Identity, product type, and resource selects should have data-loading="true"
    const loadingSelects = screen
      .getAllByRole("combobox")
      .filter((el) => el.getAttribute("data-loading") === "true");

    expect(loadingSelects.length).toBeGreaterThanOrEqual(3);
  });

  it("calls onBatchChange with both dates when date range is set and onBatchChange is provided", () => {
    const onChange = vi.fn();
    const onBatchChange = vi.fn();
    render(
      <FilterPanel
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

  it("renders with null start_date and end_date without crashing", () => {
    const nullDateFilters: ChargebackFilters = {
      ...defaultFilters,
      start_date: null,
      end_date: null,
    };
    render(
      <FilterPanel
        filters={nullDateFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );
    // Four selects still present; no crash from null date values
    expect(screen.getAllByRole("combobox")).toHaveLength(4);
  });

  it("FilterPanel_calls_onChange_product_type_null_on_Select_clear", () => {
    const onChange = vi.fn();
    const filtersWithProductType: ChargebackFilters = {
      ...defaultFilters,
      product_type: "KAFKA",
    };
    render(
      <FilterPanel
        filters={filtersWithProductType}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[1], { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("product_type", null);
  });

  it("FilterPanel_calls_onChange_resource_id_null_on_Select_clear", () => {
    const onChange = vi.fn();
    const filtersWithResource: ChargebackFilters = {
      ...defaultFilters,
      resource_id: "r-1",
    };
    render(
      <FilterPanel
        filters={filtersWithResource}
        onChange={onChange}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    const allSelects = screen.getAllByRole("combobox");
    fireEvent.change(allSelects[2], { target: { value: "" } });

    expect(onChange).toHaveBeenCalledWith("resource_id", null);
  });

  it("FilterPanel_renders_Refresh_Data_button_when_onRefresh_provided", () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        onRefresh={vi.fn()}
        tenantName="t1"
      />,
    );

    expect(screen.getByText("Refresh Data")).toBeInTheDocument();
  });

  it("FilterPanel_no_Refresh_Data_button_when_onRefresh_omitted", () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        tenantName="t1"
      />,
    );

    expect(screen.queryByText("Refresh Data")).toBeNull();
  });

  it("FilterPanel_clicking_Refresh_Data_invokes_callback", () => {
    const onRefresh = vi.fn();
    render(
      <FilterPanel
        filters={defaultFilters}
        onChange={vi.fn()}
        onReset={vi.fn()}
        onRefresh={onRefresh}
        tenantName="t1"
      />,
    );

    fireEvent.click(screen.getByText("Refresh Data"));
    expect(onRefresh).toHaveBeenCalledOnce();
  });

  describe("filterByLabel", () => {
    it("returns true when label contains the search input (case-insensitive)", () => {
      expect(filterByLabel("alice", { label: "Alice (u-1)" })).toBe(true);
      expect(filterByLabel("ALICE", { label: "Alice (u-1)" })).toBe(true);
    });

    it("returns false when label does not contain the search input", () => {
      expect(filterByLabel("bob", { label: "Alice (u-1)" })).toBe(false);
    });

    it("returns false when option is undefined", () => {
      expect(filterByLabel("anything", undefined)).toBe(false);
    });

    it("returns false when option has no label property", () => {
      expect(filterByLabel("anything", {})).toBe(false);
    });
  });
});
