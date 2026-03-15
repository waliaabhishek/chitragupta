// GAP-100 TDD red phase — verification item 7
// Test MUST fail until ChargebackListPage passes stable `queryParams` (not toQueryParams())
// to ChargebackGrid, so re-renders from drawer open / row selection do not purge the cache.
import { fireEvent, render, screen } from "@testing-library/react";
import { forwardRef } from "react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChargebackListPage } from "./list";

// Hoisted storage for captured filters props — must be initialised before vi.mock.
const capturedFilters = vi.hoisted(
  (): { values: (Record<string, string> | undefined)[] } => ({ values: [] }),
);

vi.mock("../../components/chargebacks/ChargebackGrid", () => {
  // Capture the `filters` prop on every render so the test can check reference stability.
  return {
    ChargebackGrid: forwardRef(
      (
        {
          filters,
          onSelectionChange,
          onSelectAll,
        }: {
          filters?: Record<string, string>;
          onSelectionChange?: (ids: number[]) => void;
          onSelectAll?: (total: number) => void;
        },
        _ref: unknown,
      ) => {
        capturedFilters.values.push(filters);
        return (
          <div data-testid="chargeback-grid">
            <button
              data-testid="trigger-selection"
              onClick={() => onSelectionChange?.([1, 2, 3])}
            >
              Trigger Selection
            </button>
            <button
              data-testid="trigger-select-all"
              onClick={() => onSelectAll?.(99)}
            >
              Trigger Select All
            </button>
          </div>
        );
      },
    ),
  };
});

vi.mock("../../components/chargebacks/FilterPanel", () => ({
  FilterPanel: vi.fn(({ onReset }: { onReset: () => void; onRefresh?: () => void }) => (
    <div data-testid="filter-panel">
      <button onClick={onReset}>Reset</button>
    </div>
  )),
}));

vi.mock("./ChargebackDetailDrawer", () => ({
  ChargebackDetailDrawer: vi.fn(
    ({ onClose }: { onClose: () => void; dimensionId: number | null }) => (
      <div data-testid="drawer">
        <button data-testid="close-drawer" onClick={onClose}>
          Close
        </button>
      </div>
    ),
  ),
}));

vi.mock("../../components/chargebacks/ExportButton", () => ({
  ExportButton: () => <button data-testid="export-button">Export CSV</button>,
}));

vi.mock("../../components/chargebacks/BulkTagModal", () => ({
  BulkTagModal: ({ onClose }: { onClose: () => void; onSuccess: () => void }) => (
    <div data-testid="bulk-modal">
      <button onClick={onClose}>Close</button>
    </div>
  ),
}));

vi.mock("./SelectionToolbar", () => ({
  SelectionToolbar: ({
    onClear,
    onAddTags,
  }: {
    selectedCount: number;
    isSelectAllMode: boolean;
    totalCount: number;
    onClear: () => void;
    onAddTags: () => void;
  }) => (
    <div data-testid="selection-toolbar">
      <button onClick={onClear}>Clear</button>
      <button onClick={onAddTags}>Add Tags</button>
    </div>
  ),
}));

vi.mock("antd", () => ({
  Typography: {
    Title: ({ children }: { children: ReactNode }) => <h3>{children}</h3>,
    Text: ({ children }: { children: ReactNode }) => <span>{children}</span>,
  },
}));

const mockTenant = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
};

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: mockTenant,
    tenants: [mockTenant],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
}));

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return (
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      {children}
    </MemoryRouter>
  );
}

describe("ChargebackListPage — queryParams reference stability (GAP-100)", () => {
  afterEach(() => {
    capturedFilters.values = [];
    vi.clearAllMocks();
  });

  it("passes a stable filters reference to ChargebackGrid when selection state changes", () => {
    // Verification item 7: ChargebackGrid must receive the same `filters` object reference
    // across re-renders that do NOT change filter field values (e.g. opening drawer, selecting rows).
    // Without the fix: toQueryParams() is called in render → new object each time → cache purge.
    render(<ChargebackListPage />, { wrapper });

    expect(screen.getByTestId("chargeback-grid")).toBeTruthy();
    expect(capturedFilters.values.length).toBeGreaterThan(0);

    const firstFiltersRef = capturedFilters.values[capturedFilters.values.length - 1];

    // Trigger a re-render by changing selection state (does not change filters).
    fireEvent.click(screen.getByTestId("trigger-selection"));

    const secondFiltersRef = capturedFilters.values[capturedFilters.values.length - 1];

    // FAILS in red state: toQueryParams() returns a new object on every render.
    expect(secondFiltersRef).toBe(firstFiltersRef);
  });
});
