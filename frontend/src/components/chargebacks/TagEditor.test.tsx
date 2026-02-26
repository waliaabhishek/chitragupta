import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { TagResponse } from "../../types/api";
import { TagEditor } from "./TagEditor";

const sampleTags: TagResponse[] = [
  {
    tag_id: 1,
    dimension_id: 10,
    tag_key: "env",
    tag_value: "prod",
    created_by: "ui",
    created_at: null,
  },
  {
    tag_id: 2,
    dimension_id: 10,
    tag_key: "team",
    tag_value: "platform",
    created_by: "ui",
    created_at: null,
  },
];

describe("TagEditor", () => {
  it("renders existing tags", () => {
    render(
      <TagEditor
        tags={sampleTags}
        onAdd={vi.fn()}
        onRemove={vi.fn()}
      />,
    );

    expect(screen.getByText("env: prod")).toBeTruthy();
    expect(screen.getByText("team: platform")).toBeTruthy();
  });

  it("renders empty state with no tags", () => {
    render(
      <TagEditor tags={[]} onAdd={vi.fn()} onRemove={vi.fn()} />,
    );
    expect(screen.getByText("Tags")).toBeTruthy();
    expect(screen.getByPlaceholderText("Key")).toBeTruthy();
    expect(screen.getByPlaceholderText("Value")).toBeTruthy();
  });

  it("calls onAdd with key and value on form submit", async () => {
    const onAdd = vi.fn().mockResolvedValue(undefined);
    render(
      <TagEditor tags={[]} onAdd={onAdd} onRemove={vi.fn()} />,
    );

    fireEvent.change(screen.getByPlaceholderText("Key"), {
      target: { value: "env" },
    });
    fireEvent.change(screen.getByPlaceholderText("Value"), {
      target: { value: "staging" },
    });

    await act(async () => {
      fireEvent.click(screen.getByText("Add"));
    });

    expect(onAdd).toHaveBeenCalledWith("env", "staging");
  });

  it("does not call onAdd when form fields are empty", async () => {
    const onAdd = vi.fn();
    render(
      <TagEditor tags={[]} onAdd={onAdd} onRemove={vi.fn()} />,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("Add"));
    });

    // onAdd should not be called with empty values
    expect(onAdd).not.toHaveBeenCalled();
  });

  it("calls onRemove with tag_id when close button clicked", async () => {
    const onRemove = vi.fn().mockResolvedValue(undefined);
    render(
      <TagEditor tags={sampleTags} onAdd={vi.fn()} onRemove={onRemove} />,
    );

    // Ant Design Tag close buttons are rendered as .ant-tag-close-icon spans
    const closeButtons = document.querySelectorAll(".ant-tag-close-icon");
    expect(closeButtons.length).toBeGreaterThan(0);

    await act(async () => {
      fireEvent.click(closeButtons[0]);
    });

    expect(onRemove).toHaveBeenCalledWith(1);
  });

  it("shows loading state while adding tag", async () => {
    let resolveAdd!: () => void;
    const onAdd = vi.fn().mockReturnValue(
      new Promise<void>((resolve) => {
        resolveAdd = resolve;
      }),
    );
    render(
      <TagEditor tags={[]} onAdd={onAdd} onRemove={vi.fn()} />,
    );

    fireEvent.change(screen.getByPlaceholderText("Key"), {
      target: { value: "k" },
    });
    fireEvent.change(screen.getByPlaceholderText("Value"), {
      target: { value: "v" },
    });
    fireEvent.click(screen.getByText("Add"));

    // Button should be in loading state
    await waitFor(() => {
      const btn = screen.getByRole("button", { name: /add/i });
      expect(btn.classList.contains("ant-btn-loading")).toBe(true);
    });

    await act(async () => {
      resolveAdd();
    });
  });
});
