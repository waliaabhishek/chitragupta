import type React from "react";
import { useState } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Row,
  Steps,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  PlayCircleOutlined,
} from "@ant-design/icons";
import type { ColDef, ICellRendererParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { useQuery } from "@tanstack/react-query";
import type {
  PipelineRunResponse,
  PipelineStatusResponse,
  TenantStatusDetailResponse,
  TenantStatusSummary,
} from "../../types/api";
import { useTenant, useReadiness } from "../../providers/TenantContext";
import { API_URL } from "../../config";
import { gridTheme, defaultColDef } from "../../utils/gridDefaults";

const { Title, Text } = Typography;

// ---------------------------------------------------------------------------
// Stage helpers
// ---------------------------------------------------------------------------

const STAGES = [
  "gathering",
  "calculating",
  "topic_overlay",
  "emitting",
] as const;
type Stage = (typeof STAGES)[number];

const STAGE_LABELS: Record<Stage, string> = {
  gathering: "Gathering",
  calculating: "Calculating",
  topic_overlay: "Topic Attribution Stage",
  emitting: "Emitting",
};

function stageDescription(stage: Stage, currentDate: string | null): string {
  switch (stage) {
    case "gathering":
      return "Gathering billing and resource data";
    case "calculating":
      // currentDate is only shown for "calculating" — it represents the specific
      // chargeback date being processed, which is only meaningful in this stage.
      return currentDate
        ? `Calculating chargebacks for ${currentDate}`
        : "Calculating chargebacks";
    case "topic_overlay":
      return "Computing topic attribution overlay";
    case "emitting":
      return "Finalizing output";
  }
}

// ---------------------------------------------------------------------------
// Stepper logic (AC-1)
// ---------------------------------------------------------------------------

interface StepperConfig {
  status: "process" | "finish" | "wait" | "error";
  description?: string;
}

function buildStepperItems(
  pipelineRunning: boolean,
  pipelineStage: string | null,
  pipelineCurrentDate: string | null,
  lastRunStatus: string | null,
  lastRunAt: string | null,
  topicAttributionStatus: "disabled" | "enabled" | "config_error",
): StepperConfig[] {
  let result: StepperConfig[];

  // Never run
  if (!lastRunStatus && !pipelineRunning) {
    result = STAGES.map(() => ({ status: "wait" }));
  } else if (!pipelineRunning && lastRunStatus === "completed") {
    // Idle + success → all finish, completion timestamp on last stage
    result = STAGES.map((_, i) => ({
      status: "finish",
      description:
        i === STAGES.length - 1 && lastRunAt
          ? `Completed at ${lastRunAt}`
          : undefined,
    }));
  } else if (!pipelineRunning && lastRunStatus === "failed") {
    // Idle + failed → find the failed stage index; error on it, finish before, wait after.
    // When pipelineStage is null or unknown after failure, default error to first stage.
    const rawIdx = pipelineStage ? STAGES.indexOf(pipelineStage as Stage) : 0;
    const failedIdx = rawIdx === -1 ? 0 : rawIdx;
    result = STAGES.map((_, i) => {
      if (i < failedIdx) return { status: "finish" };
      if (i === failedIdx) return { status: "error" };
      return { status: "wait" };
    });
  } else if (pipelineRunning && pipelineStage) {
    // Running → active stage is "process", previous are "finish", future are "wait"
    const activeIdx = STAGES.indexOf(pipelineStage as Stage);
    if (activeIdx !== -1) {
      result = STAGES.map((s, i) => {
        if (i < activeIdx) return { status: "finish" };
        if (i === activeIdx) {
          return {
            status: "process",
            description: stageDescription(s, pipelineCurrentDate),
          };
        }
        return { status: "wait" };
      });
    } else {
      // Unknown stage string — fall through to "running but stage unknown" branch
      result = STAGES.map((_, i) =>
        i === 0 ? { status: "process" } : { status: "wait" },
      );
    }
  } else {
    // Running but stage unknown (transient)
    result = STAGES.map((_, i) =>
      i === 0 ? { status: "process" } : { status: "wait" },
    );
  }

  if (topicAttributionStatus === "config_error") {
    result[STAGES.indexOf("topic_overlay")] = {
      status: "error",
      description: "Config error",
    };
  } else if (topicAttributionStatus === "disabled") {
    result[STAGES.indexOf("topic_overlay")] = {
      status: "wait",
      description: "Not configured",
    };
  }
  return result;
}

// ---------------------------------------------------------------------------
// Table helpers — hoisted to module level to avoid new references each render
// ---------------------------------------------------------------------------

function BoolIconRenderer(params: ICellRendererParams): React.JSX.Element {
  // Absent fields (undefined/null) render nothing — only tenants with topic attribution have these columns populated
  if (params.value === undefined || params.value === null) return <></>;
  return params.value ? (
    <CheckCircleOutlined style={{ color: "#52c41a" }} />
  ) : (
    <ClockCircleOutlined style={{ color: "#d9d9d9" }} />
  );
}

function getStateColumnDefs(topicAttributionStatus: "disabled" | "enabled" | "config_error"): ColDef[] {
  const cols: ColDef[] = [
    { field: "tracking_date", headerName: "Date", sort: "desc", flex: 1 },
    { field: "billing_gathered", headerName: "Billing Gathered", cellRenderer: BoolIconRenderer, width: 160 },
    { field: "resources_gathered", headerName: "Resources Gathered", cellRenderer: BoolIconRenderer, width: 170 },
    { field: "chargeback_calculated", headerName: "Chargeback Calculated", cellRenderer: BoolIconRenderer, width: 190 },
  ];
  if (topicAttributionStatus === "enabled") {
    cols.push(
      { field: "topic_overlay_gathered", headerName: "Topic Metrics Gathered", cellRenderer: BoolIconRenderer, width: 180 },
      { field: "topic_attribution_calculated", headerName: "Topic Attribution", cellRenderer: BoolIconRenderer, width: 160 },
    );
  }
  return cols;
}

// ---------------------------------------------------------------------------
// Inner page component — all hooks unconditionally called (tenant always set)
// ---------------------------------------------------------------------------

interface StatusContentProps {
  tenant: TenantStatusSummary;
}

function PipelineStatusContent({
  tenant,
}: StatusContentProps): React.JSX.Element {
  const { isReadOnly } = useTenant();
  const { readiness } = useReadiness();

  const [runResult, setRunResult] = useState<{
    type: "success" | "warning" | "error";
    message: string;
  } | null>(null);
  const [isRunning, setIsRunning] = useState(false);

  const tenantReadiness = readiness?.tenants.find(
    (t) => t.tenant_name === tenant.tenant_name,
  );
  const pipelineRunning = tenantReadiness?.pipeline_running ?? false;
  const isApiOnly = readiness?.mode === "api";
  const topicAttributionStatus = tenantReadiness?.topic_attribution_status ?? "disabled";

  // AC-3: Last Run Summary — refetch faster while running
  const statusQuery = useQuery<PipelineStatusResponse>({
    queryKey: ["pipeline-status", tenant.tenant_name],
    queryFn: async () => {
      const res = await fetch(
        `${API_URL}/tenants/${tenant.tenant_name}/pipeline/status`,
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json() as Promise<PipelineStatusResponse>;
    },
    refetchInterval: pipelineRunning ? 5000 : 30000,
  });

  // AC-4: Per-Date Processing Status — refetch faster while running
  const statesQuery = useQuery<TenantStatusDetailResponse>({
    queryKey: ["tenant-status", tenant.tenant_name],
    queryFn: async () => {
      const res = await fetch(
        `${API_URL}/tenants/${tenant.tenant_name}/status`,
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return res.json() as Promise<TenantStatusDetailResponse>;
    },
    refetchInterval: pipelineRunning ? 10000 : 60000,
  });

  // AC-1: Stepper items
  const stepperItems = buildStepperItems(
    pipelineRunning,
    tenantReadiness?.pipeline_stage ?? null,
    tenantReadiness?.pipeline_current_date ?? null,
    tenantReadiness?.last_run_status ?? null,
    tenantReadiness?.last_run_at ?? null,
    topicAttributionStatus,
  ).map((cfg, i) => ({
    title: STAGE_LABELS[STAGES[i]],
    status: cfg.status,
    description: cfg.description,
  }));

  // AC-2: Run Pipeline handler
  async function handleRunPipeline(): Promise<void> {
    setIsRunning(true);
    setRunResult(null);
    try {
      const res = await fetch(
        `${API_URL}/tenants/${tenant.tenant_name}/pipeline/run`,
        { method: "POST" },
      );
      const data = (await res.json()) as PipelineRunResponse;
      if (res.ok) {
        setRunResult({ type: "success", message: data.message });
      } else {
        setRunResult({
          type: "warning",
          message: data.message ?? "Unexpected response",
        });
      }
    } catch (err) {
      setRunResult({
        type: "error",
        message: err instanceof Error ? err.message : "Network error",
      });
    } finally {
      setIsRunning(false);
    }
  }

  const lastResult = statusQuery.data?.last_result ?? null;

  return (
    <Row gutter={[16, 16]}>
      {/* Stepper — full width */}
      <Col span={24}>
        <Card>
          <Steps items={stepperItems} />
        </Card>
      </Col>

      {/* Left: Run Pipeline (AC-2) */}
      <Col xs={24} sm={10} lg={8}>
        <Card title="Run Pipeline">
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            disabled={pipelineRunning || isApiOnly || isReadOnly || isRunning}
            onClick={() => {
              void handleRunPipeline();
            }}
            loading={isRunning}
          >
            Run Pipeline
          </Button>
          {runResult && (
            <Alert
              style={{ marginTop: 12 }}
              type={runResult.type}
              message={runResult.message}
              showIcon
            />
          )}
        </Card>
      </Col>

      {/* Right: Last Run Summary (AC-3) */}
      <Col xs={24} sm={14} lg={16}>
        <Card title="Last Run Summary" loading={statusQuery.isLoading}>
          {statusQuery.isError && (
            <Alert type="error" message="Failed to load pipeline status" />
          )}
          {!statusQuery.isLoading &&
            !statusQuery.isError &&
            lastResult === null && (
              <Text type="secondary">No completed runs yet.</Text>
            )}
          {lastResult && (
            <>
              <Descriptions column={2} size="small">
                <Descriptions.Item label="Completed At">
                  {lastResult.completed_at}
                </Descriptions.Item>
                <Descriptions.Item label="Dates Gathered">
                  {lastResult.dates_gathered}
                </Descriptions.Item>
                <Descriptions.Item label="Dates Calculated">
                  {lastResult.dates_calculated}
                </Descriptions.Item>
                <Descriptions.Item label="Chargeback Rows Written">
                  {lastResult.chargeback_rows_written}
                </Descriptions.Item>
              </Descriptions>
              {lastResult.errors.map((e, i) => (
                <Alert
                  key={`${e}-${i}`}
                  type="error"
                  message={e}
                  style={{ marginTop: 8 }}
                />
              ))}
            </>
          )}
        </Card>
      </Col>

      {/* Bottom: Per-Date Processing Status (AC-4) */}
      <Col span={24}>
        <Card
          title="Per-Date Processing Status"
          loading={statesQuery.isLoading}
        >
          {statesQuery.isError && (
            <Alert type="error" message="Failed to load processing status" />
          )}
          <div style={{ height: 500 }}>
            <AgGridReact
              theme={gridTheme}
              columnDefs={getStateColumnDefs(topicAttributionStatus)}
              defaultColDef={defaultColDef}
              rowData={statesQuery.data?.states ?? []}
              getRowId={(params) => params.data.tracking_date}
            />
          </div>
        </Card>
      </Col>
    </Row>
  );
}

// ---------------------------------------------------------------------------
// Top-level page — tenant guard + context hooks
// ---------------------------------------------------------------------------

export function PipelineStatusPage(): React.JSX.Element {
  const { currentTenant } = useTenant();

  return (
    <div>
      <Title level={3}>Pipeline Status</Title>

      {!currentTenant ? (
        <Text type="secondary">Select a tenant to begin.</Text>
      ) : (
        <PipelineStatusContent tenant={currentTenant} />
      )}
    </div>
  );
}
