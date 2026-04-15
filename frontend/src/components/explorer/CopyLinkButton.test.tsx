import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { CopyLinkButton } from "./CopyLinkButton";

describe("CopyLinkButton", () => {
  beforeEach(() => {
    // Mock navigator.clipboard.writeText
    Object.defineProperty(navigator, "clipboard", {
      value: {
        writeText: vi.fn(() => Promise.resolve()),
      },
      writable: true,
      configurable: true,
    });

    // Set a predictable location href
    Object.defineProperty(window, "location", {
      value: { href: "http://localhost/explorer?focus=lkc-abc&at=2026-03-15" },
      writable: true,
      configurable: true,
    });
  });

  it("calls navigator.clipboard.writeText with current window.location.href on click", async () => {
    render(<CopyLinkButton isDark={false} />);
    const button = screen.getByRole("button");
    fireEvent.click(button);

    await waitFor(() =>
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(
        "http://localhost/explorer?focus=lkc-abc&at=2026-03-15",
      ),
    );
  });

  it("shows 'Copied!' feedback after click", async () => {
    render(<CopyLinkButton isDark={false} />);
    const button = screen.getByRole("button");
    fireEvent.click(button);

    await waitFor(() => screen.getByText(/Copied!/i));
  });

  it("renders a button element", () => {
    render(<CopyLinkButton isDark={false} />);
    expect(screen.getByRole("button")).toBeTruthy();
  });

  it("shows 'Failed!' when clipboard.writeText rejects", async () => {
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: vi.fn(() => Promise.reject(new Error("denied"))) },
      writable: true,
      configurable: true,
    });

    render(<CopyLinkButton isDark={false} />);
    fireEvent.click(screen.getByRole("button"));

    await waitFor(() => screen.getByText(/Failed!/i));
  });

  it("renders with dark mode styling", () => {
    render(<CopyLinkButton isDark={true} />);
    const button = screen.getByRole("button");
    expect(button).toBeTruthy();
  });
});
