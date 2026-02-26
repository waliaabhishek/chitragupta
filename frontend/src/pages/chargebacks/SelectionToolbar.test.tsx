import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { SelectionToolbar } from "./SelectionToolbar";

describe("SelectionToolbar", () => {
  it("shows selected count in normal mode", () => {
    render(
      <SelectionToolbar
        selectedCount={3}
        isSelectAllMode={false}
        totalCount={100}
        onClear={vi.fn()}
        onAddTags={vi.fn()}
      />,
    );
    expect(screen.getByText("3 selected")).toBeTruthy();
  });

  it("shows total in select-all mode", () => {
    render(
      <SelectionToolbar
        selectedCount={100}
        isSelectAllMode={true}
        totalCount={500}
        onClear={vi.fn()}
        onAddTags={vi.fn()}
      />,
    );
    expect(screen.getByText("All 500 matching rows selected")).toBeTruthy();
  });

  it("calls onClear when Clear Selection clicked", () => {
    const onClear = vi.fn();
    render(
      <SelectionToolbar
        selectedCount={2}
        isSelectAllMode={false}
        totalCount={50}
        onClear={onClear}
        onAddTags={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByText("Clear Selection"));
    expect(onClear).toHaveBeenCalledOnce();
  });

  it("calls onAddTags when Add Tags clicked", () => {
    const onAddTags = vi.fn();
    render(
      <SelectionToolbar
        selectedCount={2}
        isSelectAllMode={false}
        totalCount={50}
        onClear={vi.fn()}
        onAddTags={onAddTags}
      />,
    );
    fireEvent.click(screen.getByText("Add Tags"));
    expect(onAddTags).toHaveBeenCalledOnce();
  });
});
