import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, beforeEach } from "vitest";
import { SearchBar } from "./SearchBar";

// Mock useGraphSearch — returns controlled results per test
vi.mock("../../hooks/useGraphSearch", () => ({
  useGraphSearch: vi.fn(() => ({
    results: [],
    isLoading: false,
    error: null,
  })),
}));

// Mock useDebouncedValue — returns value immediately (no 200ms delay in tests)
vi.mock("../../hooks/useDebouncedValue", () => ({
  useDebouncedValue: vi.fn((value: unknown) => value),
}));

import { useGraphSearch } from "../../hooks/useGraphSearch";

const MOCK_RESULTS = [
  {
    id: "lkc-abc",
    resource_type: "kafka_cluster",
    display_name: "Kafka Prod",
    parent_id: "env-abc",
    parent_display_name: "ACME Env",
    status: "active",
  },
];

describe("SearchBar", () => {
  beforeEach(() => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: [],
      isLoading: false,
      error: null,
    });
  });

  it("renders input element", () => {
    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    expect(screen.getByRole("textbox")).toBeTruthy();
  });

  it("fires onSelect with id/resourceType/displayName when result is clicked", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: MOCK_RESULTS,
      isLoading: false,
      error: null,
    });
    const onSelect = vi.fn();

    render(<SearchBar tenantName="acme" onSelect={onSelect} isDark={false} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "kafka" } });

    await waitFor(() => screen.getByText(/Kafka Prod/));
    fireEvent.click(screen.getByText(/Kafka Prod/));

    expect(onSelect).toHaveBeenCalledWith("lkc-abc", "kafka_cluster", "Kafka Prod");
  });

  it("shows 'No matches found' when results are empty and query is non-empty", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: [],
      isLoading: false,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "xyz-no-match" } });

    await waitFor(() => screen.getByText(/No matches found/i));
  });

  it("shows parent context 'in {parent_display_name}' in results", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: MOCK_RESULTS,
      isLoading: false,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "kafka" } });

    await waitFor(() => screen.getByText(/ACME Env/));
  });

  it("Escape key closes the dropdown", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: MOCK_RESULTS,
      isLoading: false,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "kafka" } });
    await waitFor(() => screen.getByText(/Kafka Prod/));

    fireEvent.keyDown(input, { key: "Escape" });
    await waitFor(() => expect(screen.queryByText(/Kafka Prod/)).toBeNull());
  });

  it("Cmd+K focuses the input", async () => {
    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");

    fireEvent.keyDown(document, { key: "k", metaKey: true });

    await waitFor(() => expect(document.activeElement).toBe(input));
  });

  it("Ctrl+K focuses the input", async () => {
    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");

    fireEvent.keyDown(document, { key: "k", ctrlKey: true });

    await waitFor(() => expect(document.activeElement).toBe(input));
  });

  it("does not show dropdown when query is empty", () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: MOCK_RESULTS,
      isLoading: false,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    // No change event — query stays empty
    expect(screen.queryByText(/Kafka Prod/)).toBeNull();
  });

  it("renders in dark mode without error", () => {
    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={true} />);
    expect(screen.getByRole("textbox")).toBeTruthy();
  });

  it("shows loading state when search is loading", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: [],
      isLoading: true,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={false} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "test" } });

    await waitFor(() => screen.getByText(/Loading…/i));
  });

  it("shows result without parent context when parent_display_name is absent", async () => {
    vi.mocked(useGraphSearch).mockReturnValue({
      results: [
        {
          id: "lkc-xyz",
          resource_type: "kafka_cluster",
          display_name: null,
          parent_id: null,
          parent_display_name: null,
          status: "active",
        },
      ],
      isLoading: false,
      error: null,
    });

    render(<SearchBar tenantName="acme" onSelect={vi.fn()} isDark={true} />);
    const input = screen.getByRole("textbox");
    fireEvent.change(input, { target: { value: "lkc" } });

    await waitFor(() => screen.getByText("lkc-xyz"));
  });
});
