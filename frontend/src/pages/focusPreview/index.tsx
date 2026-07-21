import type React from "react";
import { useEffect, useRef, useState } from "react";
import { Alert, Button, DatePicker, Descriptions, Space, Typography } from "antd";
import dayjs from "dayjs";
import {
  fetchFocusPreviewProfile,
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  listFocusPreviewRequests,
  submitFocusPreview,
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

interface PreviewRequestDetailsProps {
  request: FocusPreviewRequest;
  onDownload: (downloadUrl: string, fileName: string) => void;
}

function PreviewRequestDetails({
  request,
  onDownload,
}: PreviewRequestDetailsProps): React.JSX.Element {
  const interval = request.month ?? `${request.start_date} to ${request.end_date}`;
  const snapshot = request.source_snapshot;
  const downloadable = request.status === "ready" && request.package !== null;

  return (
    <section aria-label={`Preview request ${request.request_id}`}>
      <Space direction="vertical" style={{ width: "100%" }}>
        <Title level={4}>{request.request_id}</Title>
        <Text>Status {request.status}</Text>
        <Text>{request.grain} {interval}</Text>
        <Text>Column profile {request.column_profile}</Text>
        <Text>Created {request.created_at}</Text>
        {request.completed_at && <Text>Completed {request.completed_at}</Text>}
        {snapshot?.calculation_timestamp && (
          <Text>Calculation timestamp {snapshot.calculation_timestamp}</Text>
        )}
        {snapshot?.source_through && (
          <Text>Source through {snapshot.source_through}</Text>
        )}
        {request.status === "expired" && request.expires_at ? (
          <Text>Expired {request.expires_at}</Text>
        ) : request.expires_at ? (
          <Text>Expires {request.expires_at}</Text>
        ) : null}
        {request.diagnostic && (
          <Alert
            type="error"
            message={request.diagnostic.code}
            description={
              <Space direction="vertical">
                <Text>{request.diagnostic.message}</Text>
                <Text>Retryable: {request.diagnostic.retryable ? "Yes" : "No"}</Text>
                {request.diagnostic.source_correlation_ids?.map((correlation) => (
                  <Text code key={correlation}>{correlation}</Text>
                ))}
              </Space>
            }
          />
        )}
        {snapshot?.monthly_status && (
          <Alert
            type={snapshot.monthly_status === "provisional" ? "warning" : "success"}
            message={`Monthly status: ${snapshot.monthly_status}`}
            description={
              snapshot.evidence_through_date
                ? `Evidence through ${snapshot.evidence_through_date}`
                : "No complete daily evidence is available yet."
            }
          />
        )}
        {downloadable && (
          <Space wrap>
            <Button
              onClick={() => onDownload(
                request.package!.manifest.download_url,
                request.package!.manifest.name,
              )}
            >
              Download {request.package!.manifest.name}
            </Button>
            {request.package!.files.map((item) => (
              <Button
                key={item.name}
                aria-label={`Download cost and usage; Download ${item.name}`}
                onClick={() => onDownload(item.download_url, item.name)}
              >
                Download {item.name}
              </Button>
            ))}
            <Button
              onClick={() => onDownload(
                request.package!.download_all_url,
                request.package!.download_all_name,
              )}
            >
              Download All
            </Button>
          </Space>
        )}
      </Space>
    </section>
  );
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
  const [recentRequests, setRecentRequests] = useState<FocusPreviewRequest[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [operationError, setOperationError] = useState<string | null>(null);
  const submitAbortRef = useRef<AbortController | null>(null);
  const controllersRef = useRef(new Set<AbortController>());
  const tenantGenerationRef = useRef(0);
  const tenantNameRef = useRef<string | null>(currentTenant?.tenant_name ?? null);
  tenantNameRef.current = currentTenant?.tenant_name ?? null;

  useEffect(() => {
    tenantGenerationRef.current += 1;
    const generation = tenantGenerationRef.current;
    const controllers = controllersRef.current;
    for (const controller of controllers) controller.abort();
    controllers.clear();
    submitAbortRef.current = null;
    setPreview(null);
    setRecentRequests([]);
    setNextCursor(null);
    setBusy(false);
    setHistoryBusy(false);
    setOperationError(null);
    if (!currentTenant) return;

    const tenantName = currentTenant.tenant_name;
    const profileController = new AbortController();
    const historyController = new AbortController();
    controllers.add(profileController);
    controllers.add(historyController);
    const active = (controller: AbortController): boolean =>
      !controller.signal.aborted &&
      tenantGenerationRef.current === generation &&
      tenantNameRef.current === tenantName;

    void fetchFocusPreviewProfile(tenantName, profileController.signal)
      .then((profile) => {
        if (active(profileController)) setFullColumns(profile.full_columns);
      })
      .catch((error: unknown) => {
        if (active(profileController) && !isAbortError(error)) {
          setOperationError(REQUEST_ERROR_MESSAGE);
        }
      })
      .finally(() => controllers.delete(profileController));
    void listFocusPreviewRequests(tenantName, { signal: historyController.signal })
      .then((page) => {
        if (!active(historyController)) return;
        setRecentRequests(page.items);
        setNextCursor(page.next_cursor);
      })
      .catch((error: unknown) => {
        if (active(historyController) && !isAbortError(error)) {
          setOperationError(REQUEST_ERROR_MESSAGE);
        }
      })
      .finally(() => controllers.delete(historyController));
    return () => {
      profileController.abort();
      historyController.abort();
      controllers.delete(profileController);
      controllers.delete(historyController);
    };
  }, [currentTenant]);

  useEffect(() => {
    const controllers = controllersRef.current;
    return () => {
      for (const controller of controllers) controller.abort();
      controllers.clear();
      submitAbortRef.current = null;
    };
  }, []);

  async function submit(): Promise<void> {
    if (!currentTenant) return;
    const tenantName = currentTenant.tenant_name;
    const generation = tenantGenerationRef.current;
    setBusy(true);
    setOperationError(null);
    submitAbortRef.current?.abort();
    const controller = new AbortController();
    submitAbortRef.current = controller;
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      tenantGenerationRef.current === generation &&
      tenantNameRef.current === tenantName;
    try {
      const selection =
        columnProfile === "custom"
          ? { column_profile: columnProfile, columns: customColumns }
          : { column_profile: columnProfile };
      const body =
        grain === "monthly"
          ? { grain, month, ...selection } as const
          : { grain, start_date: startDate, end_date: endDate, ...selection } as const;
      const queued = await submitFocusPreview(tenantName, body, controller.signal);
      if (!active()) return;
      setPreview(queued);
      let status = queued;
      while (status.status === "queued" || status.status === "running") {
        status = await fetchFocusPreviewStatus(
          tenantName,
          queued.request_id,
          controller.signal,
        );
        if (!active()) return;
        setPreview(status);
        if (status.status === "queued" || status.status === "running") {
          await new Promise((resolve) => window.setTimeout(resolve, 1000));
          if (!active()) return;
        }
      }
      const recent = await listFocusPreviewRequests(tenantName, { signal: controller.signal });
      if (active()) {
        setRecentRequests(recent.items);
        setNextCursor(recent.next_cursor);
      }
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setOperationError(REQUEST_ERROR_MESSAGE);
      }
    } finally {
      controllersRef.current.delete(controller);
      if (submitAbortRef.current === controller) {
        submitAbortRef.current = null;
      }
      if (active()) {
        setBusy(false);
      }
    }
  }

  async function loadMore(): Promise<void> {
    if (!currentTenant || !nextCursor) return;
    const tenantName = currentTenant.tenant_name;
    const generation = tenantGenerationRef.current;
    const cursor = nextCursor;
    const controller = new AbortController();
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      tenantGenerationRef.current === generation &&
      tenantNameRef.current === tenantName;
    setHistoryBusy(true);
    setOperationError(null);
    try {
      const page = await listFocusPreviewRequests(tenantName, {
        cursor,
        signal: controller.signal,
      });
      if (!active()) return;
      setRecentRequests((current) => [...current, ...page.items]);
      setNextCursor(page.next_cursor);
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setOperationError(REQUEST_ERROR_MESSAGE);
      }
    } finally {
      controllersRef.current.delete(controller);
      if (active()) setHistoryBusy(false);
    }
  }

  async function download(downloadUrl: string, fileName: string): Promise<void> {
    const tenantName = currentTenant?.tenant_name;
    if (!tenantName) return;
    const generation = tenantGenerationRef.current;
    const controller = new AbortController();
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      tenantGenerationRef.current === generation &&
      tenantNameRef.current === tenantName;
    setOperationError(null);
    try {
      const blob = await fetchPreviewArtifact(downloadUrl, controller.signal);
      if (!active() || typeof URL.createObjectURL !== "function") return;
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = fileName;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setOperationError(DOWNLOAD_ERROR_MESSAGE);
      }
    } finally {
      controllersRef.current.delete(controller);
    }
  }

  const displayedRequests = preview
    ? [preview, ...recentRequests.filter((item) => item.request_id !== preview.request_id)]
    : recentRequests;

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
      <section aria-labelledby="focus-preview-recent-requests">
        <Space direction="vertical" size="large" style={{ width: "100%" }}>
          <Title id="focus-preview-recent-requests" level={3}>Recent requests</Title>
          {displayedRequests.map((request) => (
            <PreviewRequestDetails
              key={request.request_id}
              request={request}
              onDownload={(downloadUrl, fileName) => {
                void download(downloadUrl, fileName);
              }}
            />
          ))}
          {nextCursor && (
            <Button loading={historyBusy} onClick={() => void loadMore()}>
              Load more
            </Button>
          )}
        </Space>
      </section>
    </Space>
  );
}
