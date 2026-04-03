import type { ReactNode } from "react";
import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TopicAttributionPage } from "./list";
import type { TenantStatusSummary } from "../../types/api";

// ---------------------------------------------------------------------------
// Mock TopicAttributionGrid — captures filters prop for integration assertions
// ---------------------------------------------------------------------------

vi.mock("../../components/topicAttributions/TopicAttributionGrid", () => ({
  TopicAttributionGrid: ({
    filters,
  }: {
    filters: Record<string, string>;
  }) => (
    <div data-testid="ag-grid" data-filters={JSON.stringify(filters)} />
  ),
}));

// ---------------------------------------------------------------------------
// Mock ag-grid-react (needed by other components rendered in analytics tab)
// ---------------------------------------------------------------------------

vi.mock("ag-grid-react", () => ({
  AgGridReact: () => <div data-testid="ag-grid" />,
}));

// ---------------------------------------------------------------------------
// Mock ag-grid-community
// ---------------------------------------------------------------------------

vi.mock("ag-grid-community", () => ({
  themeAlpine: {
    withParams: () => ({ withParams: () => ({}) }),
  },
}));

vi.mock("../../utils/gridDefaults", () => ({
  gridTheme: {},
  defaultColDef: { sortable: true, resizable: true },
}));

// ---------------------------------------------------------------------------
// Mock antd
// ---------------------------------------------------------------------------

vi.mock("antd", () => ({
  Typography: {
    Title: ({ children, level }: { children: ReactNode; level?: number }) => (
      <h3 data-level={String(level)}>{children}</h3>
    ),
    Text: ({ children, type }: { children: ReactNode; type?: string }) => (
      <span data-type={type}>{children}</span>
    ),
  },
  Tabs: ({
    items,
    activeKey,
    onChange,
  }: {
    items?: Array<{ key: string; label: string; children: ReactNode }>;
    activeKey?: string;
    onChange?: (key: string) => void;
  }) => (
    <div data-testid="tabs" data-active={activeKey}>
      {items?.map((item) => (
        <div key={item.key}>
          <button
            data-testid={`tab-${item.key}`}
            onClick={() => onChange?.(item.key)}
          >
            {item.label}
          </button>
          {activeKey === item.key && (
            <div data-testid={`tab-content-${item.key}`}>{item.children}</div>
          )}
        </div>
      ))}
    </div>
  ),
  Button: ({
    children,
    disabled,
    onClick,
  }: {
    children: ReactNode;
    disabled?: boolean;
    onClick?: () => void;
  }) => (
    <button disabled={disabled} onClick={onClick}>
      {children}
    </button>
  ),
  Tooltip: ({ children, title }: { children: ReactNode; title?: string }) => (
    <div data-tooltip={title}>{children}</div>
  ),
  Form: Object.assign(
    ({ children }: { children: ReactNode }) => <form>{children}</form>,
    {
      Item: ({ children }: { children: ReactNode }) => <div>{children}</div>,
      useForm: () => [{}],
    },
  ),
  Space: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Select: ({ children }: { children: ReactNode }) => (
    <select>{children}</select>
  ),
  Input: (props: { placeholder?: string }) => (
    <input placeholder={props.placeholder} />
  ),
  DatePicker: Object.assign(() => <input type="date" />, {
    RangePicker: () => <input type="date" data-testid="range-picker" />,
  }),
  Radio: Object.assign(
    ({ children }: { children: ReactNode }) => <span>{children}</span>,
    {
      Group: ({ children }: { children: ReactNode }) => <div>{children}</div>,
      Button: ({ children }: { children: ReactNode }) => (
        <button>{children}</button>
      ),
    },
  ),
  Row: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Col: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  Card: ({ children, title }: { children: ReactNode; title?: string }) => (
    <div data-testid="card">
      {title && <span>{title}</span>}
      {children}
    </div>
  ),
  Empty: () => <div data-testid="empty" />,
  notification: { error: vi.fn(), success: vi.fn() },
  Alert: ({
    type,
    message,
    description,
  }: {
    type: string;
    message: string;
    description?: ReactNode;
    showIcon?: boolean;
  }) => (
    <div data-testid="alert" data-type={type}>
      {message}
      {description}
    </div>
  ),
}));

// ---------------------------------------------------------------------------
// Mock @tanstack/react-query
// ---------------------------------------------------------------------------

vi.mock("@tanstack/react-query", () => ({
  useQuery: vi.fn(() => ({
    data: undefined,
    isLoading: false,
    isError: false,
  })),
  QueryClient: vi.fn(),
  QueryClientProvider: ({ children }: { children: ReactNode }) => (
    <>{children}</>
  ),
}));

// ---------------------------------------------------------------------------
// Mock icons
// ---------------------------------------------------------------------------

vi.mock("@ant-design/icons", () => ({
  ExportOutlined: () => <span />,
  ReloadOutlined: () => <span />,
  FilterOutlined: () => <span />,
}));

// ---------------------------------------------------------------------------
// Mock echarts
// ---------------------------------------------------------------------------

vi.mock("echarts-for-react", () => ({
  default: () => <div data-testid="echarts" />,
}));

vi.mock("echarts", () => ({}));

// ---------------------------------------------------------------------------
// Mock TenantContext
// ---------------------------------------------------------------------------

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(),
}));

import { useTenant } from "../../providers/TenantContext";

const mockUseTenant = vi.mocked(useTenant);

// ---------------------------------------------------------------------------
// Mock router hooks
// ---------------------------------------------------------------------------

vi.mock("react-router", () => ({
  useSearchParams: vi.fn(() => [new URLSearchParams(), vi.fn()]),
}));

import { useSearchParams } from "react-router";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function setupTenantContext(
  tenantName: string | null = "acme",
  isReadOnly = false,
  topicAttributionEnabled?: boolean,
): void {
  mockUseTenant.mockReturnValue({
    currentTenant: tenantName
      ? ({
          tenant_name: tenantName,
          tenant_id: "t-001",
          ecosystem: "ccloud",
          dates_pending: 0,
          dates_calculated: 10,
          last_calculated_date: null,
          topic_attribution_enabled: topicAttributionEnabled ?? false,
        } as TenantStatusSummary)
      : null,
    tenants: [],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    setCurrentTenant: vi.fn(),
    isReadOnly,
  });
}

beforeEach(() => {
  vi.clearAllMocks();
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TopicAttributionPage", () => {
  it("renders title Topic Attribution", () => {
    setupTenantContext();
    render(<TopicAttributionPage />);
    expect(screen.getByText("Topic Attribution")).toBeTruthy();
  });

  it("no-tenant guard: renders select tenant prompt without fetching data", () => {
    setupTenantContext(null);
    render(<TopicAttributionPage />);
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
    // No grid should be rendered when no tenant
    expect(screen.queryByTestId("ag-grid")).toBeNull();
  });

  it("renders main content when tenant is selected", () => {
    setupTenantContext("acme");
    render(<TopicAttributionPage />);
    // Grid or tabs should be present
    expect(screen.queryByText("Select a tenant to begin.")).toBeNull();
  });

  it("Export button is disabled when isReadOnly=true", () => {
    setupTenantContext("acme", true, true);
    render(<TopicAttributionPage />);
    // Find the export button by text or test-id and verify it is disabled
    const exportBtn = screen.getByText("Export CSV").closest("button");
    expect(exportBtn?.disabled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Integration: filter param flow
// ---------------------------------------------------------------------------

describe("TopicAttributionPage — filter param integration", () => {
  afterEach(() => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams(),
      vi.fn(),
    ]);
  });

  it("passes URL filter params through useTopicAttributionFilters to grid", () => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams(
        "cluster_resource_id=lkc-abc123&start_date=2026-01-01&end_date=2026-01-31",
      ),
      vi.fn(),
    ]);
    setupTenantContext("acme", false, true);
    render(<TopicAttributionPage />);
    const grid = screen.getByTestId("ag-grid");
    expect(grid).toBeTruthy();
    const filters = JSON.parse(
      grid.getAttribute("data-filters") ?? "{}",
    ) as Record<string, string>;
    expect(filters.cluster_resource_id).toBe("lkc-abc123");
    expect(filters.start_date).toBe("2026-01-01");
    expect(filters.end_date).toBe("2026-01-31");
  });

  it("grid is not rendered when no tenant is selected, even with filter params", () => {
    vi.mocked(useSearchParams).mockImplementation(() => [
      new URLSearchParams("cluster_resource_id=lkc-abc123"),
      vi.fn(),
    ]);
    setupTenantContext(null);
    render(<TopicAttributionPage />);
    expect(screen.queryByTestId("ag-grid")).toBeNull();
    expect(screen.getByText("Select a tenant to begin.")).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// TASK-187: topic_attribution_enabled flag — feature discovery
// ---------------------------------------------------------------------------

describe("TASK-187: topic_attribution_enabled flag", () => {
  it("topic_attribution_enabled=false → shows feature discovery alert instead of data page", () => {
    setupTenantContext("acme", false, false);
    render(<TopicAttributionPage />);

    const alert = screen.getByTestId("alert");
    expect(alert).toBeTruthy();
    expect(alert.textContent?.toLowerCase()).toContain("not configured");
    expect(screen.queryByTestId("ag-grid")).toBeNull();
  });

  it("topic_attribution_enabled=true → shows normal data page", () => {
    setupTenantContext("acme", false, true);
    render(<TopicAttributionPage />);

    expect(screen.queryByTestId("alert")).toBeNull();
    expect(screen.queryByText("Select a tenant to begin.")).toBeNull();
  });
});
