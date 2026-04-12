import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TagKeySelect } from "./TagKeySelect";

vi.mock("../../hooks/useTagKeys");
const { useTagKeys } = await import("../../hooks/useTagKeys");
const mockUseTagKeys = vi.mocked(useTagKeys);

describe("TagKeySelect", () => {
  it("shows disabled 'No tags configured' option when keys array is empty", async () => {
    mockUseTagKeys.mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
    });

    render(<TagKeySelect tenantName="acme" value="" onChange={vi.fn()} />);

    // Open the dropdown
    await userEvent.click(screen.getByRole("combobox"));

    expect(screen.getByText("No tags configured")).toBeInTheDocument();
  });

  it("shows owner and team options when keys are provided", async () => {
    mockUseTagKeys.mockReturnValue({
      data: ["owner", "team"],
      isLoading: false,
      error: null,
    });

    render(<TagKeySelect tenantName="acme" value="owner" onChange={vi.fn()} />);

    // Open the dropdown
    await userEvent.click(screen.getByRole("combobox"));

    // Use getAllByRole("option") to handle rc-select's duplicate ARIA elements
    // (rc-select renders the option value as text in a hidden element; with
    // label==value the two text nodes would trip getByText's uniqueness check).
    const options = screen.getAllByRole("option");
    const texts = options.map((o) => o.textContent);
    expect(texts).toContain("owner");
    expect(texts).toContain("team");
  });
});
