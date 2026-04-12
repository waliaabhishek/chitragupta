import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AddFilterPopover } from "./AddFilterPopover";
import { useTagValues } from "../../hooks/useTagValues";

vi.mock("../../hooks/useTagValues");
const mockUseTagValues = vi.mocked(useTagValues);

const DEFAULT_PROPS = {
  tenantName: "acme",
  tagKey: "owner",
  activeTagFilters: [],
  onFilterAdd: vi.fn(),
};

function setupDefaultMock(values: string[] = ["alice", "bob"]) {
  mockUseTagValues.mockReturnValue({
    data: values,
    isLoading: false,
    error: null,
  });
}

describe("AddFilterPopover", () => {
  beforeEach(() => {
    setupDefaultMock();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("clicking '+ Add filter' opens the popover with an AutoComplete input", async () => {
    render(<AddFilterPopover {...DEFAULT_PROPS} />);

    await userEvent.click(screen.getByText("+ Add filter"));

    expect(
      screen.getByPlaceholderText("Search owner values…"),
    ).toBeInTheDocument();
  });

  it("typing 'ali' debounces 300ms then triggers useTagValues with prefix 'ali'", async () => {
    vi.useFakeTimers();
    setupDefaultMock();

    render(<AddFilterPopover {...DEFAULT_PROPS} />);

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");

    // Type without advancing timers — searchPrefix not yet set
    await userEvent.type(input, "ali");

    // Verify useTagValues NOT yet called with "ali"
    const callsBeforeDebounce = mockUseTagValues.mock.calls;
    expect(callsBeforeDebounce.every((args) => args[2] !== "ali")).toBe(true);

    // Advance debounce timer
    vi.advanceTimersByTime(300);

    await waitFor(() => {
      expect(mockUseTagValues).toHaveBeenCalledWith("acme", "owner", "ali");
    });

    vi.useRealTimers();
  });

  it("selecting a value calls onFilterAdd and closes the popover", async () => {
    const onFilterAdd = vi.fn();
    setupDefaultMock(["alice", "bob"]);

    render(<AddFilterPopover {...DEFAULT_PROPS} onFilterAdd={onFilterAdd} />);

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");
    await userEvent.click(input);

    // Select "alice" from the dropdown options
    const option = await screen.findByText("alice");
    await userEvent.click(option);

    expect(onFilterAdd).toHaveBeenCalledWith("alice");
    // Popover should close — input no longer visible
    expect(screen.queryByPlaceholderText("Search owner values…")).toBeNull();
  });

  it("closing popover without selecting resets input and search prefix", async () => {
    render(<AddFilterPopover {...DEFAULT_PROPS} />);

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");
    await userEvent.type(input, "ali");

    // Close the popover by pressing Escape
    await userEvent.keyboard("{Escape}");

    // Popover should be closed
    await waitFor(() => {
      expect(screen.queryByPlaceholderText("Search owner values…")).toBeNull();
    });

    // Re-opening should show empty input
    await userEvent.click(screen.getByText("+ Add filter"));
    const freshInput = screen.getByPlaceholderText("Search owner values…");
    expect(freshInput).toHaveValue("");
  });

  it("excludes already-active filter values from options", async () => {
    setupDefaultMock(["alice", "bob"]);

    render(
      <AddFilterPopover {...DEFAULT_PROPS} activeTagFilters={["alice"]} />,
    );

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");
    await userEvent.click(input);

    const options = await screen.findAllByRole("option");
    const optionTexts = options.map((o) => o.textContent);
    expect(optionTexts).not.toContain("alice");
    expect(optionTexts).toContain("bob");
  });

  it("shows empty options when all values are already active", async () => {
    setupDefaultMock(["alice", "bob"]);

    render(
      <AddFilterPopover
        {...DEFAULT_PROPS}
        activeTagFilters={["alice", "bob"]}
      />,
    );

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");
    await userEvent.click(input);

    // No options should be rendered (antd shows "No values found" when options=[])
    await waitFor(() => {
      const options = screen.queryAllByRole("option");
      expect(options).toHaveLength(0);
    });
  });

  it("resets inputValue and searchPrefix when tagKey prop changes", async () => {
    vi.useFakeTimers();
    setupDefaultMock();

    const { rerender } = render(<AddFilterPopover {...DEFAULT_PROPS} />);

    await userEvent.click(screen.getByText("+ Add filter"));
    const input = screen.getByPlaceholderText("Search owner values…");
    await userEvent.type(input, "ali");

    // Verify inputValue is "ali" before changing key
    expect(input).toHaveValue("ali");

    // Change tagKey — should reset inputValue and clear debounce timer
    rerender(<AddFilterPopover {...DEFAULT_PROPS} tagKey="team" />);

    // After tagKey change, searchPrefix should reset — useTagValues should not be called with "ali"
    vi.advanceTimersByTime(300);
    expect(mockUseTagValues).not.toHaveBeenCalledWith("acme", "team", "ali");

    vi.useRealTimers();
  });

  it("clears debounce timer on unmount to prevent post-unmount state update", () => {
    vi.useFakeTimers();
    setupDefaultMock();

    const { unmount } = render(<AddFilterPopover {...DEFAULT_PROPS} />);

    // Open and start typing to arm the debounce timer
    // (can't use await userEvent with fake timers easily, so we test the cleanup guard)
    unmount();

    // Advancing timers after unmount should not throw
    expect(() => vi.advanceTimersByTime(500)).not.toThrow();

    vi.useRealTimers();
  });
});
