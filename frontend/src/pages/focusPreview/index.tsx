import type React from "react";
import { useEffect, useRef, useState } from "react";
import { Alert, Button, DatePicker, Descriptions, Space, Typography } from "antd";
import dayjs from "dayjs";
import {
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  submitFocusPreview,
  type FocusPreviewArtifact,
  type FocusPreviewRequest,
} from "../../api/focusPreview";
import { useTenant } from "../../providers/TenantContext";
import { getCurrentUtcMonthRange } from "./dateRange";

const { Title, Text } = Typography;

const REQUEST_ERROR_MESSAGE = "FOCUS Mapping Preview request failed. Try again.";
const DOWNLOAD_ERROR_MESSAGE = "FOCUS Mapping Preview download failed. Try again.";

function isAbortError(error: unknown): boolean {
  return typeof error === "object" && error !== null && "name" in error && error.name === "AbortError";
}

const CURRENT_AUTHORITY_GAPS = [
  {
    code: "provider_billing_currency_field_unavailable",
    description: "Confluent Costs records do not carry a per-record billing currency.",
    owner: "TASK-254.03",
  },
  {
    code: "invoice_identity_unavailable",
    description: "Post-issuance invoice identity is unavailable.",
    owner: "TASK-254.04",
  },
  {
    code: "invoice_issuer_name_unavailable",
    description: "Provider legal invoice-issuer evidence is unavailable.",
    owner: "TASK-254.04",
  },
  {
    code: "provider_host_display_name_unavailable",
    description: "HostProviderName contains the raw provider cloud code, not a provider display name.",
    owner: "TASK-254.04",
  },
  {
    code: "provider_region_display_name_unavailable",
    description: "Confluent inventory does not provide a distinct region display name.",
    owner: "TASK-254.04",
  },
  {
    code: "derived_sku_identity_not_provider_authoritative",
    description: "SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers.",
    owner: "TASK-254.04",
  },
  {
    code: "allocation_lineage_and_tag_projection_pending",
    description: "General allocation lineage and tag projection are deferred.",
    owner: "TASK-254.05",
  },
  {
    code: "allocation_ratio_deferred",
    description: "Durable allocation-ratio evidence is deferred.",
    owner: "TASK-254.05",
  },
  {
    code: "allocation_method_version_deferred",
    description: "Allocation method-version evidence is deferred.",
    owner: "TASK-254.05",
  },
] as const;

interface FocusPreviewPageProps {
  now?: () => Date;
}

export function FocusPreviewPage({ now = () => new Date() }: FocusPreviewPageProps = {}): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [initialRange] = useState(() => getCurrentUtcMonthRange(now()));
  const [startDate, setStartDate] = useState(initialRange.startDate);
  const [endDate, setEndDate] = useState(initialRange.endDate);
  const [preview, setPreview] = useState<FocusPreviewRequest | null>(null);
  const [busy, setBusy] = useState(false);
  const [operationError, setOperationError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(
    () => () => {
      abortRef.current?.abort();
      abortRef.current = null;
    },
    [],
  );

  async function submit(): Promise<void> {
    if (!currentTenant) return;
    setBusy(true);
    setOperationError(null);
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const queued = await submitFocusPreview(currentTenant.tenant_name, {
        grain: "daily",
        start_date: startDate,
        end_date: endDate,
        column_profile: "full",
      });
      setPreview(queued);
      let status = queued;
      while (status.status === "queued" || status.status === "running") {
        status = await fetchFocusPreviewStatus(
          currentTenant.tenant_name,
          queued.request_id,
          controller.signal,
        );
        setPreview(status);
        if (status.status === "queued" || status.status === "running") {
          await new Promise((resolve) => window.setTimeout(resolve, 1000));
        }
      }
    } catch (error) {
      if (!isAbortError(error)) {
        setOperationError(REQUEST_ERROR_MESSAGE);
      }
    } finally {
      if (abortRef.current === controller) {
        setBusy(false);
      }
    }
  }

  async function download(item: FocusPreviewArtifact): Promise<void> {
    setOperationError(null);
    try {
      const blob = await fetchPreviewArtifact(item.download_url);
      if (typeof URL.createObjectURL !== "function") return;
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = item.name;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      if (!isAbortError(error)) {
        setOperationError(DOWNLOAD_ERROR_MESSAGE);
      }
    }
  }

  return (
    <Space direction="vertical" size="large" style={{ width: "100%" }}>
      <Title level={2}>FOCUS Mapping Preview</Title>
      <Alert
        type="warning"
        showIcon
        message="Non-conforming preview"
        description="Provider-authoritative billing currency evidence is unavailable. The manifest declares every current authority gap."
      />
      <section aria-labelledby="focus-preview-gaps">
        <Title id="focus-preview-gaps" level={4}>Current authority gaps</Title>
        <ul>
          {CURRENT_AUTHORITY_GAPS.map((gap) => (
            <li key={gap.code}>
              <Text code>{gap.code}</Text>{" "}
              <Text>{gap.description}</Text>{" "}
              <Text type="secondary">Owner: {gap.owner}</Text>
            </li>
          ))}
        </ul>
      </section>
      <Descriptions bordered size="small" column={2}>
        <Descriptions.Item label="Grain">Daily</Descriptions.Item>
        <Descriptions.Item label="Column profile">Full</Descriptions.Item>
      </Descriptions>
      {operationError && <Alert type="error" showIcon message={operationError} />}
      <Space wrap>
        <label>
          Start date
          <DatePicker
            aria-label="Start date"
            value={dayjs(startDate)}
            onChange={(_value, text) => setStartDate(String(text))}
          />
        </label>
        <label>
          End date
          <DatePicker
            aria-label="End date"
            value={dayjs(endDate)}
            onChange={(_value, text) => setEndDate(String(text))}
          />
        </label>
        <Button type="primary" loading={busy} disabled={!currentTenant} onClick={() => void submit()}>
          Generate preview
        </Button>
      </Space>
      {preview?.diagnostic && (
        <Alert
          type="error"
          message={preview.diagnostic.code}
          description={
            <Space direction="vertical">
              <Text>{preview.diagnostic.message}</Text>
              <Text>Retryable: {preview.diagnostic.retryable ? "Yes" : "No"}</Text>
              {preview.diagnostic.source_correlation_ids?.map((correlation) => (
                <Text code key={correlation}>{correlation}</Text>
              ))}
            </Space>
          }
        />
      )}
      {preview?.package && (
        <Space>
          <Button onClick={() => void download(preview.package!.manifest)}>Download manifest</Button>
          {preview.package.files.map((item) => (
            <Button key={item.name} onClick={() => void download(item)}>
              Download cost and usage
            </Button>
          ))}
        </Space>
      )}
    </Space>
  );
}
