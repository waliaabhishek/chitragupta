import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TagPivotPanel } from "./TagPivotPanel";

vi.mock("./TagPivotChart", () => ({
  TagPivotChart: vi.fn(
    ({ onBarClick }: { onBarClick?: (v: string) => void }) => (
      <div data-testid="tag-pivot-chart">
        <button type="button" onClick={() => onBarClick?.("alice")}>
          bar-alice
        </button>
        <button type="button" onClick={() => onBarClick?.("UNTAGGED")}>
          bar-untagged
        </button>
      </div>
    ),
  ),
}));

vi.mock("./TagKeySelect", () => ({
  TagKeySelect: vi.fn(
    ({
      value,
      onChange,
    }: {
      tenantName: string;
      value: string;
      onChange: (k: string) => void;
    }) => (
      <select
        data-testid="tag-key-select"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        <option value="owner">owner</option>
        <option value="team">team</option>
      </select>
    ),
  ),
}));

vi.mock("./AddFilterPopover", () => ({
  AddFilterPopover: vi.fn(() => <button type="button">+ Add filter</button>),
}));

const BASE_PROPS = {
  title: "Cost by Owner",
  tenantName: "acme",
  buckets: [],
  isLoading: false,
  error: null,
  onRefetch: vi.fn(),
  selectedTagKey: "owner",
  onTagKeyChange: vi.fn(),
  activeTagFilters: [],
  onFilterAdd: vi.fn(),
  onFilterRemove: vi.fn(),
};

describe("TagPivotPanel", () => {
  it("renders chart in chart mode and table in table mode after toggle", async () => {
    render(<TagPivotPanel {...BASE_PROPS} />);

    // Default: chart mode — TagPivotChart visible
    expect(screen.getByTestId("tag-pivot-chart")).toBeInTheDocument();
    expect(screen.queryByRole("table")).toBeNull();

    // Toggle to table mode
    await userEvent.click(screen.getByText("Table"));
    expect(screen.queryByTestId("tag-pivot-chart")).toBeNull();
    expect(screen.getByRole("table")).toBeInTheDocument();
  });

  it("calls onFilterAdd('alice') when alice bar is clicked", async () => {
    const onFilterAdd = vi.fn();
    render(<TagPivotPanel {...BASE_PROPS} onFilterAdd={onFilterAdd} />);

    await userEvent.click(screen.getByText("bar-alice"));
    expect(onFilterAdd).toHaveBeenCalledWith("alice");
  });

  it("does NOT call onFilterAdd when UNTAGGED bar is clicked", async () => {
    const onFilterAdd = vi.fn();
    render(<TagPivotPanel {...BASE_PROPS} onFilterAdd={onFilterAdd} />);

    await userEvent.click(screen.getByText("bar-untagged"));
    expect(onFilterAdd).not.toHaveBeenCalled();
  });

  it("calls onTagKeyChange when tag key select changes", async () => {
    const onTagKeyChange = vi.fn();
    render(<TagPivotPanel {...BASE_PROPS} onTagKeyChange={onTagKeyChange} />);

    await userEvent.selectOptions(screen.getByTestId("tag-key-select"), "team");
    expect(onTagKeyChange).toHaveBeenCalledWith("team");
  });
});
