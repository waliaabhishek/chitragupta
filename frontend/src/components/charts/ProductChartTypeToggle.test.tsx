import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ProductChartTypeToggle } from "./ProductChartTypeToggle";

vi.mock("antd", () => ({
  Segmented: ({
    options,
    value,
    onChange,
  }: {
    options: { label: string; value: string }[];
    value: string;
    onChange: (v: string) => void;
  }) => (
    <div data-testid="segmented" data-value={value}>
      {options.map((opt) => (
        <button
          key={opt.value}
          data-testid={`option-${opt.value}`}
          onClick={() => onChange(opt.value)}
        >
          {opt.label}
        </button>
      ))}
    </div>
  ),
}));

describe("ProductChartTypeToggle", () => {
  it("renders pie and treemap options", () => {
    render(<ProductChartTypeToggle value="pie" onChange={vi.fn()} />);
    expect(screen.getByTestId("option-pie")).toBeInTheDocument();
    expect(screen.getByTestId("option-treemap")).toBeInTheDocument();
    expect(screen.getByText("Pie")).toBeInTheDocument();
    expect(screen.getByText("Treemap")).toBeInTheDocument();
  });

  it("reflects current value as selected", () => {
    render(<ProductChartTypeToggle value="treemap" onChange={vi.fn()} />);
    expect(screen.getByTestId("segmented").getAttribute("data-value")).toBe(
      "treemap",
    );
  });

  it("calls onChange with 'pie' when pie option is clicked", async () => {
    const onChange = vi.fn();
    render(<ProductChartTypeToggle value="treemap" onChange={onChange} />);
    await userEvent.click(screen.getByTestId("option-pie"));
    expect(onChange).toHaveBeenCalledWith("pie");
  });

  it("calls onChange with 'treemap' when treemap option is clicked", async () => {
    const onChange = vi.fn();
    render(<ProductChartTypeToggle value="pie" onChange={onChange} />);
    await userEvent.click(screen.getByTestId("option-treemap"));
    expect(onChange).toHaveBeenCalledWith("treemap");
  });
});
