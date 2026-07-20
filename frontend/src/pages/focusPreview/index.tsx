import type React from "react";
import { useEffect, useRef, useState } from "react";
import { Alert, Button, DatePicker, Descriptions, Space, Typography } from "antd";
import dayjs from "dayjs";
import {
  fetchFocusPreviewProfile,
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  submitFocusPreview,
  type FocusPreviewArtifact,
  type FocusPreviewRequest,
  type FocusPreviewColumnProfile,
} from "../../api/focusPreview";
import { useTenant } from "../../providers/TenantContext";
import { getCurrentUtcMonth, getCurrentUtcMonthRange } from "./dateRange";

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
] as const;

interface FocusPreviewPageProps {
  now?: () => Date;
}

export function FocusPreviewPage({ now = () => new Date() }: FocusPreviewPageProps = {}): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [initialRange] = useState(() => getCurrentUtcMonthRange(now()));
  const [grain, setGrain] = useState<"monthly" | "daily">("monthly");
  const [month, setMonth] = useState(() => getCurrentUtcMonth(now()));
  const [columnProfile, setColumnProfile] = useState<FocusPreviewColumnProfile>("full");
  const [customColumns, setCustomColumns] = useState<string[]>([]);
  const [fullColumns, setFullColumns] = useState<string[]>([]);
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

  useEffect(() => {
    if (!currentTenant) return;
    let active = true;
    void fetchFocusPreviewProfile(currentTenant.tenant_name)
      .then((profile) => {
        if (active) setFullColumns(profile.full_columns);
      })
      .catch(() => {
        if (active) setOperationError(REQUEST_ERROR_MESSAGE);
      });
    return () => {
      active = false;
    };
  }, [currentTenant]);

  async function submit(): Promise<void> {
    if (!currentTenant) return;
    setBusy(true);
    setOperationError(null);
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const selection =
        columnProfile === "custom"
          ? { column_profile: columnProfile, columns: customColumns }
          : { column_profile: columnProfile };
      const body =
        grain === "monthly"
          ? { grain, month, ...selection } as const
          : { grain, start_date: startDate, end_date: endDate, ...selection } as const;
      const queued = await submitFocusPreview(currentTenant.tenant_name, body);
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
        <Descriptions.Item label="Grain">{grain === "monthly" ? "Monthly" : "Daily"}</Descriptions.Item>
        <Descriptions.Item label="Column profile">{columnProfile}</Descriptions.Item>
      </Descriptions>
      {operationError && <Alert type="error" showIcon message={operationError} />}
      <Space wrap>
        <label>Grain
          <select aria-label="Grain" value={grain} onChange={(event) => setGrain(event.target.value as "monthly" | "daily")}>
            <option value="monthly">Monthly</option>
            <option value="daily">Daily</option>
          </select>
        </label>
        {grain === "monthly" ? (
          <label>Month
            <input aria-label="Month" type="month" value={month} onChange={(event) => setMonth(event.target.value)} />
          </label>
        ) : (
          <>
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
          </>
        )}
        <label>Column profile
          <select
            aria-label="Column profile"
            value={columnProfile}
            onChange={(event) => setColumnProfile(event.target.value as FocusPreviewColumnProfile)}
          >
            <option value="full">Full</option>
            <option value="summary">Summary</option>
            <option value="custom">Custom</option>
          </select>
        </label>
        {columnProfile === "custom" && (
          <label>Custom columns
            <select
              aria-label="Custom columns"
              multiple
              value={customColumns}
              onChange={(event) => {
                const selected = Array.from(event.target.selectedOptions, (option) => option.value);
                const selectedSet = new Set(selected);
                setCustomColumns((current) => [
                  ...current.filter((column) => selectedSet.has(column)),
                  ...selected.filter((column) => !current.includes(column)),
                ]);
              }}
            >
              {fullColumns.map((column) => <option key={column} value={column}>{column}</option>)}
            </select>
          </label>
        )}
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
      {preview?.source_snapshot?.monthly_status && (
        <Alert
          type={preview.source_snapshot.monthly_status === "provisional" ? "warning" : "success"}
          message={`Monthly status: ${preview.source_snapshot.monthly_status}`}
          description={
            preview.source_snapshot.evidence_through_date
              ? `Evidence through ${preview.source_snapshot.evidence_through_date}`
              : "No complete daily evidence is available yet."
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
