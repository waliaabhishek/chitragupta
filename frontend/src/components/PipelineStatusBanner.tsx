import { Alert, Spin } from "antd";
import { useTenant } from "../providers/TenantContext";

function formatStage(
  stage: string | null,
  currentDate: string | null,
): string {
  if (!stage) return "Starting up...";
  switch (stage) {
    case "gathering":
      return "Gathering billing and resource data...";
    case "calculating":
      return currentDate
        ? `Calculating chargebacks for ${currentDate}...`
        : "Calculating chargebacks...";
    case "emitting":
      return "Finalizing output...";
    default:
      return `Pipeline stage: ${stage}`;
  }
}

export function PipelineStatusBanner(): JSX.Element | null {
  const { appStatus, readiness, currentTenant } = useTenant();

  if (appStatus === "loading") {
    return (
      <Alert
        banner
        type="info"
        showIcon={false}
        message={
          <span>
            <Spin size="small" style={{ marginRight: 8 }} />
            Connecting to backend...
          </span>
        }
      />
    );
  }

  if (appStatus === "initializing") {
    return (
      <Alert
        banner
        type="info"
        showIcon={false}
        message={
          <span>
            <Spin size="small" style={{ marginRight: 8 }} />
            Setting up database...
          </span>
        }
      />
    );
  }

  if (appStatus === "error") {
    const failures = readiness?.tenants
      .filter((t) => t.permanent_failure)
      .map((t) => `${t.tenant_name}: ${t.permanent_failure}`)
      .join("; ");
    return (
      <Alert
        banner
        type="error"
        message={failures || "All tenants permanently failed"}
      />
    );
  }

  // Check current tenant's pipeline status
  const tenantStatus = readiness?.tenants.find(
    (t) => t.tenant_name === currentTenant?.tenant_name,
  );

  if (tenantStatus?.pipeline_running) {
    const stageText = formatStage(
      tenantStatus.pipeline_stage,
      tenantStatus.pipeline_current_date,
    );
    return (
      <Alert
        banner
        type="info"
        showIcon={false}
        message={
          <span>
            <Spin size="small" style={{ marginRight: 8 }} />
            Pipeline running &mdash; {stageText}
          </span>
        }
      />
    );
  }

  if (appStatus === "no_data") {
    return (
      <Alert
        banner
        type="warning"
        message="No data yet. Waiting for pipeline to run."
      />
    );
  }

  // ready + idle → no banner
  return null;
}
