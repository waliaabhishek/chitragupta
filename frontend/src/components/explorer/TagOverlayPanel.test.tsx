import { createElement } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import { TagOverlayPanel } from "./TagOverlayPanel";

// Mock antd Select with a simple <select> element for testability
vi.mock("antd", async () => {
  const { createElement: ce } = await import("react");
  return {
    Select: Object.assign(
      ({
        value,
        onChange,
        disabled,
        placeholder,
        options,
      }: {
        value?: string | null;
        onChange?: (v: string | null) => void;
        disabled?: boolean;
        placeholder?: string;
        options?: { label: string; value: string }[];
      }) =>
        ce(
          "select",
          {
            "data-testid": "tag-key-select",
            value: value ?? "",
            disabled,
            "aria-placeholder": placeholder,
            onChange: (e: { target: { value: string } }) => {
              onChange?.(e.target.value || null);
            },
          },
          options?.map((o) =>
            ce("option", { key: o.value, value: o.value }, o.label),
          ),
        ),
      { Option: () => null },
    ),
    Tooltip: ({ children }: { children: React.ReactNode }) => createElement("div", {}, children),
  };
});

const DEFAULT_PROPS = {
  availableKeys: ["team", "env", "owner"],
  isLoadingKeys: false,
  activeKey: null as string | null,
  onKeyChange: vi.fn(),
  colorMap: {} as Record<string, string>,
  selectedValue: null as string | null,
  onValueClick: vi.fn(),
  isDark: false,
};

describe("TagOverlayPanel", () => {
  beforeEach(() => {
    DEFAULT_PROPS.onKeyChange = vi.fn();
    DEFAULT_PROPS.onValueClick = vi.fn();
  });

  it("renders dropdown with available keys", () => {
    render(<TagOverlayPanel {...DEFAULT_PROPS} />);
    const select = screen.getByTestId("tag-key-select");
    expect(select).toBeTruthy();
    expect(screen.getByText("team")).toBeTruthy();
    expect(screen.getByText("env")).toBeTruthy();
  });

  it("shows 'No tags available' placeholder and disables select when keys empty", () => {
    render(
      <TagOverlayPanel
        {...DEFAULT_PROPS}
        availableKeys={[]}
      />,
    );
    const select = screen.getByTestId("tag-key-select");
    expect((select as HTMLSelectElement).disabled).toBe(true);
  });

  it("renders color legend when key is selected and colorMap is populated", () => {
    render(
      <TagOverlayPanel
        {...DEFAULT_PROPS}
        activeKey="team"
        colorMap={{ platform: "#1677ff", data: "#52c41a" }}
      />,
    );
    expect(screen.getByText("platform")).toBeTruthy();
    expect(screen.getByText("data")).toBeTruthy();
  });

  it("clicking a legend value calls onValueClick with that value", () => {
    const onValueClick = vi.fn();
    render(
      <TagOverlayPanel
        {...DEFAULT_PROPS}
        activeKey="team"
        colorMap={{ platform: "#1677ff" }}
        onValueClick={onValueClick}
      />,
    );
    fireEvent.click(screen.getByText("platform"));
    expect(onValueClick).toHaveBeenCalledWith("platform");
  });

  it("clicking the already-selected value calls onValueClick(null) to deselect", () => {
    const onValueClick = vi.fn();
    render(
      <TagOverlayPanel
        {...DEFAULT_PROPS}
        activeKey="team"
        colorMap={{ platform: "#1677ff" }}
        selectedValue="platform"
        onValueClick={onValueClick}
      />,
    );
    fireEvent.click(screen.getByText("platform"));
    expect(onValueClick).toHaveBeenCalledWith(null);
  });

  it("does not render legend when no key is selected", () => {
    render(
      <TagOverlayPanel
        {...DEFAULT_PROPS}
        activeKey={null}
        colorMap={{ platform: "#1677ff" }}
      />,
    );
    expect(screen.queryByText("platform")).toBeNull();
  });
});
