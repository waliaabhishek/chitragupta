import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { PivotFilterBar } from "./PivotFilterBar";

vi.mock("./AddFilterPopover", () => ({
  AddFilterPopover: vi.fn(() => <button type="button">+ Add filter</button>),
}));

describe("PivotFilterBar", () => {
  it("renders '+ Add filter' button when active filters are empty", () => {
    render(
      <PivotFilterBar
        tenantName="acme"
        tagKey="owner"
        activeFilters={[]}
        onFilterAdd={vi.fn()}
        onRemove={vi.fn()}
      />,
    );

    expect(screen.getByText("+ Add filter")).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: /close/i })).toBeNull();
  });

  it("clicking chip close button calls onRemove with the filter value", async () => {
    const onRemove = vi.fn();
    render(
      <PivotFilterBar
        tenantName="acme"
        tagKey="owner"
        activeFilters={["alice"]}
        onFilterAdd={vi.fn()}
        onRemove={onRemove}
      />,
    );

    await userEvent.click(screen.getByRole("img", { name: /close/i }));

    expect(onRemove).toHaveBeenCalledWith("alice");
  });

  it("renders chip 'owner=alice' with close button and '+ Add filter' when one filter active", () => {
    render(
      <PivotFilterBar
        tenantName="acme"
        tagKey="owner"
        activeFilters={["alice"]}
        onFilterAdd={vi.fn()}
        onRemove={vi.fn()}
      />,
    );

    expect(screen.getByText("owner=alice")).toBeInTheDocument();
    expect(screen.getByText("+ Add filter")).toBeInTheDocument();
  });
});
