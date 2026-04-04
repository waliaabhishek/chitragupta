import type { ReactNode } from "react";
// @ts-expect-error — node builtins available at vitest runtime but not in browser tsconfig
import { readFileSync } from "node:fs";
// @ts-expect-error — node builtins available at vitest runtime but not in browser tsconfig
import { fileURLToPath } from "node:url";
// @ts-expect-error — node builtins available at vitest runtime but not in browser tsconfig
import { dirname, join } from "node:path";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PipelineStatusPage } from "./status";
import type {
  PipelineStatusResponse,
  ReadinessResponse,
  TenantReadiness,
  TenantStatusDetailResponse,
  TenantStatusSummary,
} from "../../types/api";

// ---------------------------------------------------------------------------
// Hoisted captures
// ---------------------------------------------------------------------------

// Capture AG Grid column defs for assertion
const gridCapture = vi.hoisted(() => ({
  columnDefs: null as Array<{
    field?: string;
    sort?: string;
    headerName?: string;
  }> | null,
  rowData: null as Array<Record<string, unknown>> | null,
}));

// ---------------------------------------------------------------------------
// Mock antd
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Mock ag-grid-react
// ---------------------------------------------------------------------------

vi.mock("ag-grid-react", () => ({
  AgGridReact: (props: {
    columnDefs?: Array<{
      field?: string;
      sort?: string;
      headerName?: string;
      cellRenderer?: (p: { value: unknown }) => ReactNode;
    }>;
    rowData?: Array<Record<string, unknown>>;
    theme?: unknown;
    defaultColDef?: unknown;
    getRowId?: unknown;
  }) => {
    gridCapture.columnDefs = props.columnDefs ?? null;
    gridCapture.rowData = props.rowData ?? null;
    const { columnDefs, rowData } = props;
    return (
      <div data-testid="ag-grid">
        <table>
          <thead>
            <tr>
              {columnDefs?.map((col) => (
                <th key={col.field ?? col.headerName}>{col.headerName}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rowData?.map((row, i) => (
              <tr key={i} data-testid={`grid-row-${i}`}>
                {columnDefs?.map((col) => (
                  <td key={col.field ?? col.headerName}>
                    {col.cellRenderer
                      ? col.cellRenderer({
                          value: col.field ? row[col.field] : undefined,
                        })
                      : String(col.field ? (row[col.field] ?? "") : "")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  },
}));

// ---------------------------------------------------------------------------
// Mock ag-grid-community (theme)
// ---------------------------------------------------------------------------

vi.mock("ag-grid-community", () => ({
  themeAlpine: {
    withParams: () => ({
      withParams: () => ({}),
    }),
  },
}));

vi.mock("../../utils/gridDefaults", () => ({
  gridTheme: {},
  defaultColDef: { sortable: true, resizable: true },
}));

// ---------------------------------------------------------------------------
// Mock antd
// ---------------------------------------------------------------------------

vi.mock("antd", () => {
  const DescriptionsItem = ({
    label,
    children,
  }: {
    label: string;
    children: ReactNode;
  }) => (
    <div
      data-testid={`desc-item-${String(label)
        .toLowerCase()
        .replace(/\s+/g, "-")}`}
    >
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );

  const Descriptions = Object.assign(
    ({ children }: { children: ReactNode }) => (
      <dl data-testid="descriptions">{children}</dl>
    ),
    { Item: DescriptionsItem },
  );

  return {
    Typography: {
      Title: ({ children, level }: { children: ReactNode; level?: number }) => (
        <h3 data-level={String(level)}>{children}</h3>
      ),
      Text: ({ children, type }: { children: ReactNode; type?: string }) => (
        <span data-type={type}>{children}</span>
      ),
    },
    Steps: ({
      items,
    }: {
      items?: Array<{
        title: string;
        status?: string;
        description?: string;
      }>;
    }) => (
      <div data-testid="steps">
        {items?.map((item, i) => (
          <div
            key={i}
            data-testid={`step-${i}`}
            data-status={item.status ?? "wait"}
            data-description={item.description ?? ""}
          >
            {item.title}
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
      type?: string;
      icon?: ReactNode;
      loading?: boolean;
    }) => (
      <button
        data-testid="run-pipeline-btn"
        disabled={disabled}
        onClick={onClick}
      >
        {children}
      </button>
    ),
    Card: ({
      children,
      title,
      loading,
    }: {
      children: ReactNode;
      title?: string;
      loading?: boolean;
    }) =>
      loading ? (
        <div data-testid="card-loading">{title}</div>
      ) : (
        <div data-testid="card">
          {title && (
            <span
              data-testid={`card-title-${String(title)
                .toLowerCase()
                .replace(/\s+/g, "-")}`}
            >
              {title}
            </span>
          )}
          {children}
        </div>
      ),
    Alert: ({
      type,
      message,
    }: {
      type: string;
      message: string;
      showIcon?: boolean;
      style?: object;
    }) => (
      <div data-testid="alert" data-type={type}>
        {message}
      </div>
    ),
    Row: ({ children }: { children: ReactNode; gutter?: unknown }) => (
      <div>{children}</div>
    ),
    Col: ({
      children,
    }: {
      children: ReactNode;
      span?: number;
      xs?: number;
      sm?: number;
      lg?: number;
    }) => <div>{children}</div>,
    Descriptions,
  };
});

// ---------------------------------------------------------------------------
// Mock icons
// ---------------------------------------------------------------------------

vi.mock("@ant-design/icons", () => ({
  CheckCircleOutlined: () => <span data-testid="check-circle-icon" />,
  ClockCircleOutlined: () => <span data-testid="clock-circle-icon" />,
  PlayCircleOutlined: () => <span data-testid="play-circle-icon" />,
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
}));

// ---------------------------------------------------------------------------
// Mock TenantContext
// ---------------------------------------------------------------------------

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(),
  useReadiness: vi.fn(),
}));

import { useTenant, useReadiness } from "../../providers/TenantContext";
import { useQuery } from "@tanstack/react-query";

const mockUseTenant = vi.mocked(useTenant);
const mockUseReadiness = vi.mocked(useReadiness);
const mockUseQuery = vi.mocked(useQuery);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const mockTenant: TenantStatusSummary = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
  topic_attribution_status: "disabled",
  topic_attribution_error: null,
};

function makeTenantReadiness(
  overrides: Partial<TenantReadiness> = {},
): TenantReadiness {
  return {
    tenant_name: "acme",
    tables_ready: true,
    has_data: true,
    pipeline_running: false,
    pipeline_stage: null,
    pipeline_current_date: null,
    last_run_status: null,
    last_run_at: null,
    permanent_failure: null,
    topic_attribution_status: "enabled",
    topic_attribution_error: null,
    ...overrides,
  };
}

function makeReadiness(
  tenantOverrides: Partial<TenantReadiness> = {},
  readinessOverrides: Partial<ReadinessResponse> = {},
): ReadinessResponse {
  return {
    status: "ready",
    version: "1.0.0",
    mode: "both",
    tenants: [makeTenantReadiness(tenantOverrides)],
    ...readinessOverrides,
  };
}

function setupTenantContext(
  tenantOverrides: Partial<TenantReadiness> = {},
  readinessOverrides: Partial<ReadinessResponse> = {},
  isReadOnly = false,
): void {
  mockUseTenant.mockReturnValue({
    currentTenant: mockTenant,
    tenants: [mockTenant],
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    setCurrentTenant: vi.fn(),
    isReadOnly,
  });
  mockUseReadiness.mockReturnValue({
    appStatus: "ready",
    readiness: makeReadiness(tenantOverrides, readinessOverrides),
  });
}

function defaultStatusData(): PipelineStatusResponse {
  return {
    tenant_name: "acme",
    is_running: false,
    last_run: null,
    last_result: null,
  };
}

function defaultStatesData(): TenantStatusDetailResponse {
  return {
    tenant_name: "acme",
    tenant_id: "t-001",
    ecosystem: "ccloud",
    states: [],
    topic_attribution_status: "disabled",
    topic_attribution_error: null,
  };
}

type QueryOpts = {
  queryKey: unknown[];
  refetchInterval?: number | false;
  queryFn?: unknown;
};

function setupDefaultQueries(
  statusOverrides: Partial<PipelineStatusResponse> = {},
  statesOverrides: Partial<TenantStatusDetailResponse> = {},
): void {
  mockUseQuery.mockImplementation((opts: unknown) => {
    const { queryKey } = opts as QueryOpts;
    if (queryKey[0] === "pipeline-status") {
      return {
        data: { ...defaultStatusData(), ...statusOverrides },
        isLoading: false,
        isError: false,
      } as unknown as ReturnType<typeof useQuery>;
    }
    if (queryKey[0] === "tenant-status") {
      return {
        data: { ...defaultStatesData(), ...statesOverrides },
        isLoading: false,
        isError: false,
      } as unknown as ReturnType<typeof useQuery>;
    }
    return {
      data: undefined,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof useQuery>;
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  gridCapture.columnDefs = null;
  gridCapture.rowData = null;
  mockUseQuery.mockReturnValue({
    data: undefined,
    isLoading: false,
    isError: false,
  } as unknown as ReturnType<typeof useQuery>);
});

// ---------------------------------------------------------------------------
// AC-1: Stepper
// ---------------------------------------------------------------------------

describe("AC-1: Stepper", () => {
  it("test 1: pipeline_running=false, last_run_status=null → all three steps have status wait", () => {
    setupTenantContext({ pipeline_running: false, last_run_status: null });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("step-0")).toHaveAttribute("data-status", "wait");
    expect(screen.getByTestId("step-1")).toHaveAttribute("data-status", "wait");
    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "wait");
  });

  it("test 2: pipeline_running=true, stage=calculating, currentDate=2026-03-14 → step 0 finish, step 1 process with description, step 2 wait", () => {
    setupTenantContext({
      pipeline_running: true,
      pipeline_stage: "calculating",
      pipeline_current_date: "2026-03-14",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("step-0")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute(
      "data-status",
      "process",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute(
      "data-description",
      "Calculating chargebacks for 2026-03-14",
    );
    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "wait");
  });

  it("test 3: pipeline_running=false, last_run_status=completed, last_run_at=2026-03-26T10:05:00Z → all steps finish, last step description contains timestamp", () => {
    setupTenantContext({
      pipeline_running: false,
      last_run_status: "completed",
      last_run_at: "2026-03-26T10:05:00Z",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("step-0")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-2")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-3")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(
      screen.getByTestId("step-3").getAttribute("data-description"),
    ).toContain("2026-03-26T10:05:00Z");
  });

  it("test 4: pipeline_running=false, last_run_status=failed, pipeline_stage=calculating → step 0 finish, step 1 error, step 2 wait", () => {
    setupTenantContext({
      pipeline_running: false,
      last_run_status: "failed",
      pipeline_stage: "calculating",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("step-0")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute(
      "data-status",
      "error",
    );
    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "wait");
  });
});

// ---------------------------------------------------------------------------
// AC-2: Run Pipeline button
// ---------------------------------------------------------------------------

describe("AC-2: Run Pipeline button", () => {
  it("test 5: pipeline_running=true → button is disabled", () => {
    setupTenantContext({ pipeline_running: true });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("run-pipeline-btn")).toBeDisabled();
  });

  it("test 6: readiness.mode=api → button is disabled", () => {
    setupTenantContext({}, { mode: "api" });
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("run-pipeline-btn")).toBeDisabled();
  });

  it("test 7: isReadOnly=true → button is disabled", () => {
    setupTenantContext({}, {}, true);
    setupDefaultQueries();
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("run-pipeline-btn")).toBeDisabled();
  });

  it("test 8: happy path — click button → POST to /pipeline/run → success Alert shown", async () => {
    setupTenantContext();
    setupDefaultQueries();
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          tenant_name: "acme",
          status: "started",
          message: "Pipeline started successfully",
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      ),
    );

    render(<PipelineStatusPage />);
    await userEvent.click(screen.getByTestId("run-pipeline-btn"));

    await waitFor(() => {
      expect(screen.getByTestId("alert")).toBeInTheDocument();
    });
    expect(screen.getByTestId("alert")).toHaveAttribute("data-type", "success");
    expect(screen.getByTestId("alert").textContent).toContain(
      "Pipeline started successfully",
    );
  });

  it("test 9: network error — fetch throws → error Alert shown with message", async () => {
    setupTenantContext();
    setupDefaultQueries();
    vi.spyOn(globalThis, "fetch").mockRejectedValueOnce(
      new Error("Network failure"),
    );

    render(<PipelineStatusPage />);
    await userEvent.click(screen.getByTestId("run-pipeline-btn"));

    await waitFor(() => {
      expect(screen.getByTestId("alert")).toBeInTheDocument();
    });
    expect(screen.getByTestId("alert")).toHaveAttribute("data-type", "error");
    expect(screen.getByTestId("alert").textContent).toContain(
      "Network failure",
    );
  });
});

// ---------------------------------------------------------------------------
// AC-3: Last Run Summary
// ---------------------------------------------------------------------------

describe("AC-3: Last Run Summary", () => {
  it("test 10: last_result=null → card shows No completed runs yet.", () => {
    setupTenantContext();
    setupDefaultQueries({ last_result: null });
    render(<PipelineStatusPage />);
    expect(screen.getByText("No completed runs yet.")).toBeInTheDocument();
  });

  it("test 11: last_result populated → Descriptions shows completed_at, dates_gathered, dates_calculated, chargeback_rows_written", () => {
    setupTenantContext();
    setupDefaultQueries({
      last_result: {
        completed_at: "2026-03-26T10:05:00Z",
        dates_gathered: 5,
        dates_calculated: 5,
        chargeback_rows_written: 120,
        errors: [],
      },
    });
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("descriptions")).toBeInTheDocument();
    expect(screen.getByTestId("desc-item-completed-at")).toBeInTheDocument();
    expect(screen.getByTestId("desc-item-dates-gathered")).toBeInTheDocument();
    expect(
      screen.getByTestId("desc-item-dates-calculated"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("desc-item-chargeback-rows-written"),
    ).toBeInTheDocument();
    expect(screen.getByText("2026-03-26T10:05:00Z")).toBeInTheDocument();
    expect(screen.getByText("120")).toBeInTheDocument();
  });

  it("test 12: last_result.errors=['some error'] → Alert with type=error and message rendered", () => {
    setupTenantContext();
    setupDefaultQueries({
      last_result: {
        completed_at: "2026-03-26T10:05:00Z",
        dates_gathered: 5,
        dates_calculated: 5,
        chargeback_rows_written: 120,
        errors: ["some error"],
      },
    });
    render(<PipelineStatusPage />);
    const alerts = screen.getAllByTestId("alert");
    const errorAlert = alerts.find(
      (a) => a.getAttribute("data-type") === "error",
    );
    expect(errorAlert).toBeDefined();
    expect(errorAlert?.textContent).toContain("some error");
  });

  it("test 13: pipeline_running=true → refetchInterval is 5000 for pipeline-status query", () => {
    setupTenantContext({ pipeline_running: true });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    const calls = mockUseQuery.mock.calls as unknown as Array<[QueryOpts]>;
    const statusCall = calls.find(
      ([opts]) =>
        Array.isArray(opts.queryKey) && opts.queryKey[0] === "pipeline-status",
    );
    expect(statusCall).toBeDefined();
    expect(statusCall?.[0].refetchInterval).toBe(5000);
  });
});

// ---------------------------------------------------------------------------
// AC-4: Per-Date Processing Status table
// ---------------------------------------------------------------------------

describe("AC-4: Per-Date Processing Status grid", () => {
  it("test 14: renders AG Grid with columns Date, Billing Gathered, Resources Gathered, Chargeback Calculated", () => {
    setupTenantContext();
    setupDefaultQueries({}, { states: [] });
    render(<PipelineStatusPage />);
    expect(screen.getByTestId("ag-grid")).toBeInTheDocument();
    expect(screen.getByText("Date")).toBeInTheDocument();
    expect(screen.getByText("Billing Gathered")).toBeInTheDocument();
    expect(screen.getByText("Resources Gathered")).toBeInTheDocument();
    expect(screen.getByText("Chargeback Calculated")).toBeInTheDocument();
  });

  it("test 15: billing_gathered=true → CheckCircleOutlined; billing_gathered=false → ClockCircleOutlined", () => {
    setupTenantContext();
    setupDefaultQueries(
      {},
      {
        states: [
          {
            tracking_date: "2026-03-26",
            billing_gathered: true,
            resources_gathered: true,
            chargeback_calculated: true,
          },
          {
            tracking_date: "2026-03-25",
            billing_gathered: false,
            resources_gathered: false,
            chargeback_calculated: false,
          },
        ],
      },
    );
    render(<PipelineStatusPage />);
    // Row 0: all true → 3 check icons; Row 1: all false → 3 clock icons
    expect(screen.getAllByTestId("check-circle-icon")).toHaveLength(3);
    expect(screen.getAllByTestId("clock-circle-icon")).toHaveLength(3);
  });

  it("test 16: tracking_date column has sort=desc", () => {
    setupTenantContext();
    setupDefaultQueries({}, { states: [] });
    render(<PipelineStatusPage />);
    expect(gridCapture.columnDefs).not.toBeNull();
    const dateCol = gridCapture.columnDefs?.find(
      (c) => c.field === "tracking_date",
    );
    expect(dateCol).toBeDefined();
    expect(dateCol?.sort).toBe("desc");
  });

  it("test 17: grid receives rowData from statesQuery", () => {
    setupTenantContext();
    const states = [
      {
        tracking_date: "2026-03-26",
        billing_gathered: true,
        resources_gathered: true,
        chargeback_calculated: true,
      },
    ];
    setupDefaultQueries({}, { states });
    render(<PipelineStatusPage />);
    expect(gridCapture.rowData).toHaveLength(1);
    expect(gridCapture.rowData?.[0].tracking_date).toBe("2026-03-26");
  });
});

// ---------------------------------------------------------------------------
// AC-5: No duplicate readiness polling
// ---------------------------------------------------------------------------

describe("AC-5: No duplicate readiness polling", () => {
  it("test 18: PipelineStatusContent does not use setInterval/setTimeout and uses useQuery for polling (not manual readiness fetch)", () => {
    const dir = dirname(fileURLToPath(import.meta.url));
    const source = readFileSync(join(dir, "status.tsx"), "utf8");
    // Must NOT set up manual polling
    expect(source).not.toMatch(/setInterval|setTimeout/);
    // Must NOT directly fetch /readiness (context already handles this)
    expect(source).not.toMatch(/\/readiness/);
    // MUST use useQuery to drive polling — proves react-query manages refetch intervals
    expect(source).toMatch(/useQuery/);
    // MUST include the pipeline-status query key
    expect(source).toMatch(/pipeline-status/);
  });
});

// ---------------------------------------------------------------------------
// AC-6: Layout and imports
// ---------------------------------------------------------------------------

describe("AC-6: Layout and imports", () => {
  it("test 19: page renders Typography.Title level={3} with text Pipeline Status", () => {
    mockUseTenant.mockReturnValue({
      currentTenant: null,
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });
    mockUseReadiness.mockReturnValue({
      appStatus: "ready",
      readiness: null,
    });
    render(<PipelineStatusPage />);
    const title = screen.getByText("Pipeline Status");
    expect(title).toBeInTheDocument();
    expect(title.closest("h3")).not.toBeNull();
  });

  it("test 20: with no tenant selected → renders Select a tenant to begin.", () => {
    mockUseTenant.mockReturnValue({
      currentTenant: null,
      tenants: [],
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      setCurrentTenant: vi.fn(),
      isReadOnly: false,
    });
    mockUseReadiness.mockReturnValue({
      appStatus: "ready",
      readiness: null,
    });
    render(<PipelineStatusPage />);
    expect(screen.getByText("Select a tenant to begin.")).toBeInTheDocument();
  });

  it("test 21: imports from ../../types/api and ../../config, no local type redefinitions", () => {
    const dir = dirname(fileURLToPath(import.meta.url));
    const source = readFileSync(join(dir, "status.tsx"), "utf8");
    // Must import types from ../../types/api
    expect(source).toMatch(/from\s+["']\.\.\/\.\.\/types\/api["']/);
    // Must import API_URL from ../../config
    expect(source).toMatch(/from\s+["']\.\.\/\.\.\/config["']/);
    // Must not locally redefine Pipeline types
    expect(source).not.toMatch(/^(?:interface|type)\s+Pipeline/m);
  });
});

// ---------------------------------------------------------------------------
// AC-7 (TASK-164): 4-Stage stepper with topic_overlay
// ---------------------------------------------------------------------------

describe("AC-7 (TASK-164): 4-stage stepper", () => {
  it("test 22: pipeline_stage=topic_overlay renders 4 steps with no underscore in labels", () => {
    setupTenantContext({
      pipeline_running: true,
      pipeline_stage: "topic_overlay",
      pipeline_current_date: "2026-04-01",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    // 4 steps must exist (indices 0–3)
    expect(screen.getByTestId("step-0")).toBeTruthy();
    expect(screen.getByTestId("step-1")).toBeTruthy();
    expect(screen.getByTestId("step-2")).toBeTruthy();
    expect(screen.getByTestId("step-3")).toBeTruthy();

    // Step 2 (topic_overlay) must be process and have no underscore in title
    expect(screen.getByTestId("step-2")).toHaveAttribute(
      "data-status",
      "process",
    );
    expect(screen.getByTestId("step-2").textContent).not.toContain("_");
    // Human-readable label: "Topic Attribution Stage"
    expect(screen.getByTestId("step-2").textContent).toContain("Topic Attribution");
  });

  it("test 23: pipeline_stage=gathering still works with 4-step stepper", () => {
    setupTenantContext({
      pipeline_running: true,
      pipeline_stage: "gathering",
      pipeline_current_date: null,
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(screen.getByTestId("step-0")).toHaveAttribute(
      "data-status",
      "process",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute("data-status", "wait");
    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "wait");
    expect(screen.getByTestId("step-3")).toHaveAttribute("data-status", "wait");
  });

  it("test 24: pipeline_stage=emitting still works with 4-step stepper — step 3 is process", () => {
    setupTenantContext({
      pipeline_running: true,
      pipeline_stage: "emitting",
      pipeline_current_date: null,
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(screen.getByTestId("step-0")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-1")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-2")).toHaveAttribute(
      "data-status",
      "finish",
    );
    expect(screen.getByTestId("step-3")).toHaveAttribute(
      "data-status",
      "process",
    );
  });

  it("test 25: per-date grid renders Topic Overlay and Topic Attribution columns", () => {
    setupTenantContext();
    setupDefaultQueries(
      {},
      {
        states: [
          {
            tracking_date: "2026-04-01",
            billing_gathered: true,
            resources_gathered: true,
            chargeback_calculated: true,
            topic_overlay_gathered: true,
            topic_attribution_calculated: false,
          },
        ],
      },
    );
    render(<PipelineStatusPage />);

    expect(screen.getByText("Topic Metrics Gathered")).toBeInTheDocument();
    expect(screen.getByText("Topic Attribution")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// TASK-187: Topic Attribution Enabled Flag
// ---------------------------------------------------------------------------

describe("TASK-187: Topic Attribution Enabled Flag", () => {
  it("test 26: topic_attribution_status=disabled → topic_overlay step has status wait and description 'Not configured'", () => {
    setupTenantContext({
      pipeline_running: false,
      last_run_status: "completed",
      last_run_at: "2026-04-01T10:00:00Z",
      topic_attribution_status: "disabled",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "wait");
    expect(screen.getByTestId("step-2")).toHaveAttribute(
      "data-description",
      "Not configured",
    );
  });

  it("test 27: topic_attribution_status=enabled → topic_overlay step renders normally (not overridden)", () => {
    setupTenantContext({
      pipeline_running: false,
      last_run_status: "completed",
      last_run_at: "2026-04-01T10:00:00Z",
      topic_attribution_status: "enabled",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "finish");
  });

  it("test 28: topic_attribution_status=disabled → grid hides Topic Overlay and Topic Attribution columns", () => {
    setupTenantContext({
      topic_attribution_status: "disabled",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(gridCapture.columnDefs).not.toBeNull();
    expect(gridCapture.columnDefs?.length).toBe(4);
    const headerNames = gridCapture.columnDefs?.map((c) => c.headerName);
    expect(headerNames).toContain("Date");
    expect(headerNames).toContain("Billing Gathered");
    expect(headerNames).toContain("Resources Gathered");
    expect(headerNames).toContain("Chargeback Calculated");
    expect(headerNames).not.toContain("Topic Metrics Gathered");
    expect(headerNames).not.toContain("Topic Attribution");
  });

  it("test 29: topic_attribution_status=enabled → grid shows all 6 columns including Topic Overlay and Topic Attribution", () => {
    setupTenantContext({
      topic_attribution_status: "enabled",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(gridCapture.columnDefs).not.toBeNull();
    expect(gridCapture.columnDefs?.length).toBe(6);
    const headerNames = gridCapture.columnDefs?.map((c) => c.headerName);
    expect(headerNames).toContain("Topic Metrics Gathered");
    expect(headerNames).toContain("Topic Attribution");
  });

  it("test 30: topic_attribution_status=config_error → topic_overlay step has status error and description 'Config error'", () => {
    setupTenantContext({
      pipeline_running: false,
      last_run_status: "completed",
      last_run_at: "2026-04-01T10:00:00Z",
      topic_attribution_status: "config_error",
      topic_attribution_error: "requires metrics",
    });
    setupDefaultQueries();
    render(<PipelineStatusPage />);

    expect(screen.getByTestId("step-2")).toHaveAttribute("data-status", "error");
    expect(screen.getByTestId("step-2")).toHaveAttribute(
      "data-description",
      "Config error",
    );
  });
});
