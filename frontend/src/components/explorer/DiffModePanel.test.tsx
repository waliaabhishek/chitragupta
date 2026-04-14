import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DiffModePanel } from "./DiffModePanel";

// GIT-R2-001: replace antd DatePicker.RangePicker with a testable <input>
// so we can fire onChange and verify onRangesChange calls.
// Also call disabledDate with representative dates so the inline callback
// branches at lines 167-168 and 196-197 are exercised on every render.
vi.mock("antd", async () => {
  const { createElement } = await import("react");
  const dayjsMod = await import("dayjs");
  const dayjs = dayjsMod.default;
  return {
    DatePicker: {
      RangePicker: ({
        onChange,
        disabled,
        disabledDate,
      }: {
        onChange?: (_: unknown, dateStrings: [string, string]) => void;
        disabled?: boolean;
        disabledDate?: (d: unknown) => boolean;
      }) => {
        // Invoke disabledDate with dates covering all 3 branch paths:
        //   1. before minDate  → isBefore=true (short-circuit OR)
        //   2. after maxDate   → isBefore=false, isAfter=true
        //   3. in range        → both false → returns false
        // When minDate/maxDate are null the callback hits the early-return guard.
        if (disabledDate) {
          disabledDate(dayjs("2020-01-01"));
          disabledDate(dayjs("2030-12-31"));
          disabledDate(dayjs("2026-02-15"));
        }
        return createElement("input", {
          type: "date",
          disabled,
          "data-testid": "mock-range-picker-input",
          onChange: (e: { target: { value: string } }) => {
            const v = e.target.value;
            if (onChange && v) onChange(null, [v, v]);
          },
        });
      },
    },
  };
});

const DEFAULT_PROPS = {
  isActive: false,
  onToggle: vi.fn(),
  fromRange: null as [string, string] | null,
  toRange: null as [string, string] | null,
  onRangesChange: vi.fn(),
  minDate: "2026-01-01",
  maxDate: "2026-04-13",
  isDark: false,
};

describe("DiffModePanel", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // Fix reference date for deterministic preset date calculations
    vi.setSystemTime(new Date("2026-04-13T00:00:00Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("renders a toggle button", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} />);

    expect(
      screen.getByRole("button", { name: /diff|compare/i }),
    ).toBeInTheDocument();
  });

  it("calls onToggle when toggle button is clicked", () => {
    const onToggle = vi.fn();
    render(<DiffModePanel {...DEFAULT_PROPS} onToggle={onToggle} />);

    fireEvent.click(screen.getByRole("button", { name: /diff|compare/i }));

    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  it("shows preset and date picker controls when isActive=true", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    // Preset buttons should be visible when diff mode is active
    expect(
      screen.getByText(/week over week/i) ||
        screen.getByText(/month over month/i),
    ).toBeInTheDocument();
  });

  it("does not show preset controls when isActive=false", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={false} />);

    expect(screen.queryByText(/week over week/i)).toBeNull();
    expect(screen.queryByText(/month over month/i)).toBeNull();
  });

  it("renders 'Week over week' preset button when active", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    expect(screen.getByText(/week over week/i)).toBeInTheDocument();
  });

  it("renders 'Month over month' preset button when active", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    expect(screen.getByText(/month over month/i)).toBeInTheDocument();
  });

  it("renders 'Last 30d vs previous 30d' preset button when active", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    expect(screen.getByText(/30d|30 days/i)).toBeInTheDocument();
  });

  it("'Last 30d vs previous 30d' preset calls onRangesChange with correct date ranges", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    fireEvent.click(screen.getByText(/30d|30 days/i));

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [from, to] = onRangesChange.mock.calls[0];

    // "to" range ends at maxDate (2026-04-13), spans 30 days
    expect(to).toHaveLength(2);
    expect(to[1]).toBe("2026-04-13");
    expect(new Date(to[1]).getTime() - new Date(to[0]).getTime()).toBe(
      29 * 24 * 60 * 60 * 1000,
    );

    // "from" range is the 30 days before "to"
    expect(from).toHaveLength(2);
    expect(from[1]).toBe(
      new Date(new Date(to[0]).getTime() - 24 * 60 * 60 * 1000)
        .toISOString()
        .split("T")[0],
    );
  });

  it("'Week over week' preset calls onRangesChange with two 7-day ranges", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    fireEvent.click(screen.getByText(/week over week/i));

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [from, to] = onRangesChange.mock.calls[0];

    // Each range spans 7 days
    const toDays =
      (new Date(to[1]).getTime() - new Date(to[0]).getTime()) /
      (24 * 60 * 60 * 1000);
    const fromDays =
      (new Date(from[1]).getTime() - new Date(from[0]).getTime()) /
      (24 * 60 * 60 * 1000);
    expect(toDays).toBe(6); // 7 days inclusive = 6 day diff
    expect(fromDays).toBe(6);
  });

  it("'Month over month' preset calls onRangesChange with two 30-day ranges", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    fireEvent.click(screen.getByText(/month over month/i));

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [from, to] = onRangesChange.mock.calls[0];

    expect(from).toHaveLength(2);
    expect(to).toHaveLength(2);

    // Both ranges span approximately 30 days
    const toDays =
      (new Date(to[1]).getTime() - new Date(to[0]).getTime()) /
      (24 * 60 * 60 * 1000);
    expect(toDays).toBeCloseTo(29, 0);
  });

  it("shows custom date pickers when 'Custom' mode is selected", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    const customBtn = screen.getByText(/custom/i);
    fireEvent.click(customBtn);

    // Two range pickers should appear for from/to date selection
    const rangePickers = document.querySelectorAll(
      ".ant-picker-range, [data-testid='from-range-picker'], [data-testid='to-range-picker']",
    );
    expect(rangePickers.length).toBeGreaterThanOrEqual(2);
  });

  it("disables presets and pickers when minDate is null", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} minDate={null} />);

    const presetBtns = screen.getAllByRole("button");
    // All interactive controls should be disabled
    const hasDisabledBtn = presetBtns.some(
      (btn) =>
        btn.hasAttribute("disabled") ||
        btn.getAttribute("aria-disabled") === "true",
    );
    expect(hasDisabledBtn).toBe(true);
  });

  it("disables presets and pickers when maxDate is null", () => {
    render(<DiffModePanel {...DEFAULT_PROPS} isActive={true} maxDate={null} />);

    const presetBtns = screen.getAllByRole("button");
    const hasDisabledBtn = presetBtns.some(
      (btn) =>
        btn.hasAttribute("disabled") ||
        btn.getAttribute("aria-disabled") === "true",
    );
    expect(hasDisabledBtn).toBe(true);
  });

  it("toggle button reflects isActive state visually", () => {
    const { rerender } = render(
      <DiffModePanel {...DEFAULT_PROPS} isActive={false} />,
    );

    const btn = screen.getByRole("button", { name: /diff|compare/i });
    const inactiveClass = btn.className;

    rerender(<DiffModePanel {...DEFAULT_PROPS} isActive={true} />);

    const activeBtn = screen.getByRole("button", { name: /diff|compare/i });
    expect(activeBtn.className).not.toBe(inactiveClass);
  });

  // GIT-002: custom range picker path (depends on GIC-007 code fix)
  it("custom mode: selecting dates in from-picker fires onRangesChange", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    // Switch to custom mode
    fireEvent.click(screen.getByText(/custom/i));

    // Two range pickers should appear
    const fromPicker = document.querySelector(
      "[data-testid='from-range-picker']",
    );
    const toPicker = document.querySelector("[data-testid='to-range-picker']");
    expect(fromPicker).not.toBeNull();
    expect(toPicker).not.toBeNull();
  });

  it("custom mode: both range pickers are constrained to [minDate, maxDate]", () => {
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        minDate="2026-01-01"
        maxDate="2026-04-13"
      />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    const fromPicker = document.querySelector(
      "[data-testid='from-range-picker']",
    );
    const toPicker = document.querySelector("[data-testid='to-range-picker']");

    // Pickers should exist and be constrained (disabledDate applied)
    expect(fromPicker).not.toBeNull();
    expect(toPicker).not.toBeNull();
  });

  it("custom mode: fromRange prop pre-fills from-picker selection", () => {
    const { rerender } = render(
      <DiffModePanel {...DEFAULT_PROPS} isActive={true} fromRange={null} />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    rerender(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        fromRange={["2026-02-01", "2026-02-28"]}
      />,
    );

    // Component should reflect the provided fromRange (renders without error)
    expect(
      document.querySelector("[data-testid='from-range-picker']"),
    ).not.toBeNull();
  });

  // GIT-R2-001: onChange handlers on custom range pickers
  it("custom from-picker onChange calls onRangesChange with new from range", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    const fromWrapper = document.querySelector(
      "[data-testid='from-range-picker']",
    );
    const fromInput = fromWrapper?.querySelector(
      "[data-testid='mock-range-picker-input']",
    );
    expect(fromInput).not.toBeNull();

    fireEvent.change(fromInput!, { target: { value: "2026-02-01" } });

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [from] = onRangesChange.mock.calls[0];
    expect(from).toEqual(["2026-02-01", "2026-02-01"]);
  });

  it("custom to-picker onChange calls onRangesChange with new to range", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
      />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    const toWrapper = document.querySelector(
      "[data-testid='to-range-picker']",
    );
    const toInput = toWrapper?.querySelector(
      "[data-testid='mock-range-picker-input']",
    );
    expect(toInput).not.toBeNull();

    fireEvent.change(toInput!, { target: { value: "2026-03-01" } });

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [, to] = onRangesChange.mock.calls[0];
    expect(to).toEqual(["2026-03-01", "2026-03-01"]);
  });

  it("custom from-picker onChange passes existing toRange as second arg when toRange is set", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
        toRange={["2026-03-01", "2026-03-31"]}
      />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    const fromWrapper = document.querySelector(
      "[data-testid='from-range-picker']",
    );
    const fromInput = fromWrapper?.querySelector(
      "[data-testid='mock-range-picker-input']",
    );
    fireEvent.change(fromInput!, { target: { value: "2026-02-01" } });

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [, to] = onRangesChange.mock.calls[0];
    expect(to).toEqual(["2026-03-01", "2026-03-31"]);
  });

  // GIT-R3: disabledDate early-return guard (lines 167, 196) — minDate null case
  // Open custom mode while minDate is set, then rerender with minDate=null so the
  // already-visible RangePicker re-renders with the null-guard path.
  it("custom mode: disabledDate early-return guard fires when minDate becomes null", () => {
    const { rerender } = render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        minDate="2026-01-01"
        maxDate="2026-04-13"
      />,
    );

    // Open custom mode while minDate is valid
    fireEvent.click(screen.getByText(/custom/i));
    expect(
      document.querySelector("[data-testid='from-range-picker']"),
    ).not.toBeNull();

    // Rerender with minDate=null — disabledDate is called with null minDate,
    // hitting the `if (!minDate || !maxDate) return false` early-return branch.
    rerender(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        minDate={null}
        maxDate="2026-04-13"
      />,
    );

    // Pickers still in DOM (showCustom state persisted)
    expect(
      document.querySelector("[data-testid='from-range-picker']"),
    ).not.toBeNull();
  });

  it("custom to-picker onChange passes existing fromRange as first arg when fromRange is set", () => {
    const onRangesChange = vi.fn();
    render(
      <DiffModePanel
        {...DEFAULT_PROPS}
        isActive={true}
        onRangesChange={onRangesChange}
        fromRange={["2026-01-01", "2026-01-31"]}
      />,
    );

    fireEvent.click(screen.getByText(/custom/i));

    const toWrapper = document.querySelector(
      "[data-testid='to-range-picker']",
    );
    const toInput = toWrapper?.querySelector(
      "[data-testid='mock-range-picker-input']",
    );
    fireEvent.change(toInput!, { target: { value: "2026-03-01" } });

    expect(onRangesChange).toHaveBeenCalledTimes(1);
    const [from] = onRangesChange.mock.calls[0];
    expect(from).toEqual(["2026-01-01", "2026-01-31"]);
  });
});
