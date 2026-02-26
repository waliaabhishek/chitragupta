import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { TagResponse } from "../../types/api";
import { TagEditor } from "./TagEditor";

const sampleTags: TagResponse[] = [
  {
    tag_id: 1,
    dimension_id: 10,
    tag_key: "env",
    tag_value: "uuid-aaa",
    display_name: "Production",
    created_by: "ui",
    created_at: null,
  },
  {
    tag_id: 2,
    dimension_id: 10,
    tag_key: "team",
    tag_value: "uuid-bbb",
    display_name: "Platform",
    created_by: "ui",
    created_at: null,
  },
];

describe("TagEditor", () => {
  it("renders existing tags with display_name", () => {
    render(
      <TagEditor
        tags={sampleTags}
        onAdd={vi.fn()}
        onRemove={vi.fn()}
      />,
    );

    expect(screen.getByText("Production")).toBeTruthy();
    expect(screen.getByText("Platform")).toBeTruthy();
  });

  it("renders empty state with Key and Display Name fields", () => {
    render(
      <TagEditor tags={[]} onAdd={vi.fn()} onRemove={vi.fn()} />,
    );
    expect(screen.getByText("Tags")).toBeTruthy();
    expect(screen.getByPlaceholderText("Key")).toBeTruthy();
    expect(screen.getByPlaceholderText("Display Name")).toBeTruthy();
  });

  it("calls onAdd with key and displayName on form submit", async () => {
    const onAdd = vi.fn().mockResolvedValue(undefined);
    render(
      <TagEditor tags={[]} onAdd={onAdd} onRemove={vi.fn()} />,
    );

    fireEvent.change(screen.getByPlaceholderText("Key"), {
      target: { value: "env" },
    });
    fireEvent.change(screen.getByPlaceholderText("Display Name"), {
      target: { value: "Staging" },
    });

    await act(async () => {
      fireEvent.click(screen.getByText("Add"));
    });

    expect(onAdd).toHaveBeenCalledWith("env", "Staging");
  });

  it("does not call onAdd when form fields are empty", async () => {
    const onAdd = vi.fn();
    render(
      <TagEditor tags={[]} onAdd={onAdd} onRemove={vi.fn()} />,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("Add"));
    });

    expect(onAdd).not.toHaveBeenCalled();
  });

  it("calls onRemove with tag_id when close button clicked", async () => {
    const onRemove = vi.fn().mockResolvedValue(undefined);
    render(
      <TagEditor tags={sampleTags} onAdd={vi.fn()} onRemove={onRemove} />,
    );

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
    fireEvent.change(screen.getByPlaceholderText("Display Name"), {
      target: { value: "v" },
    });
    fireEvent.click(screen.getByText("Add"));

    await waitFor(() => {
      const btn = screen.getByRole("button", { name: /add/i });
      expect(btn.classList.contains("ant-btn-loading")).toBe(true);
    });

    await act(async () => {
      resolveAdd();
    });
  });
});
