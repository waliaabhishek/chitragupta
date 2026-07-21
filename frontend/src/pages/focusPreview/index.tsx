import type React from "react";
import { useEffect, useRef, useState } from "react";
import { Alert, Button, DatePicker, Descriptions, Space, Tag, Typography } from "antd";
import dayjs from "dayjs";
import {
  fetchFocusPreviewProfile,
  fetchFocusPreviewRevision,
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  listFocusPreviewRequests,
  listFocusPreviewRevisions,
  submitFocusPreview,
  type FocusPreviewColumnProfile,
  type FocusPreviewRequest,
  type FocusPreviewRevision,
  type FocusPreviewRevisionSummary,
} from "../../api/focusPreview";
import { useTenant } from "../../providers/TenantContext";
import { getCurrentUtcMonth, getCurrentUtcMonthRange } from "./dateRange";

const { Title, Text } = Typography;

const REQUEST_ERROR_MESSAGE = "FOCUS Mapping Preview request failed. Try again.";
const DOWNLOAD_ERROR_MESSAGE = "FOCUS Mapping Preview download failed. Try again.";
const REVISION_ERROR_MESSAGE = "Published revision operation failed. Try again.";

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

interface PreviewRevisionDetailsProps {
  revision: FocusPreviewRevisionSummary;
  onView: (revisionId: string) => void;
}

function PreviewRevisionDetails({
  revision,
  onView,
}: PreviewRevisionDetailsProps): React.JSX.Element {
  return (
    <section aria-label={`Published revision ${revision.revision_id}`}>
      <Space direction="vertical" style={{ width: "100%" }}>
        <Title level={4}>{revision.revision_id}</Title>
        <Space wrap>
          <Tag color={revision.lifecycle === "current" ? "green" : "default"}>
            Lifecycle {revision.lifecycle}
          </Tag>
          <Tag color={revision.monthly_status === "provisional" ? "orange" : "green"}>
            Monthly status {revision.monthly_status}
          </Tag>
        </Space>
        <Text>Published {revision.published_at}</Text>
        {revision.source_snapshot.calculation_timestamp && (
          <Text>
            {revision.lifecycle === "current" ? "Calculation" : "Snapshot calculated at"}{" "}
            {revision.source_snapshot.calculation_timestamp}
          </Text>
        )}
        {revision.source_snapshot.source_through && (
          <Text>
            {revision.lifecycle === "current" ? "Source through" : "Source data through"}{" "}
            {revision.source_snapshot.source_through}
          </Text>
        )}
        {revision.supersedes_revision_id && (
          <Text>Supersedes {revision.supersedes_revision_id}</Text>
        )}
        {revision.superseded_by_revision_id && (
          <Text>Superseded by {revision.superseded_by_revision_id}</Text>
        )}
        <Text>Validation {revision.validation.status}</Text>
        <Text>Mapping profile {revision.validation.mapping_profile_version}</Text>
        <Text>Source records {revision.validation.source_records}</Text>
        <Text>Rows {revision.validation.rows}</Text>
        <Text>Artifact integrity {revision.validation.artifact_integrity}</Text>
        <Button onClick={() => onView(revision.revision_id)}>View and download</Button>
      </Space>
    </section>
  );
}

interface PreviewRevisionDownloadProps {
  revision: FocusPreviewRevision;
  onDownload: (downloadUrl: string, fileName: string) => void;
}

function PreviewRevisionDownload({
  revision,
  onDownload,
}: PreviewRevisionDownloadProps): React.JSX.Element {
  return (
    <section aria-label={`Revision downloads ${revision.revision_id}`}>
      <Space wrap>
        <Button
          onClick={() => onDownload(
            revision.package.manifest.download_url,
            revision.package.manifest.name,
          )}
        >
          Download {revision.package.manifest.name}
        </Button>
        {revision.package.files.map((item) => (
          <Button
            key={item.name}
            onClick={() => onDownload(item.download_url, item.name)}
          >
            Download {item.name}
          </Button>
        ))}
        <Button
          onClick={() => onDownload(
            revision.package.download_all_url,
            revision.package.download_all_name,
          )}
        >
          Download All
        </Button>
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
  const [revisionMonth, setRevisionMonth] = useState(() => getCurrentUtcMonth(now()));
  const [columnProfile, setColumnProfile] = useState<FocusPreviewColumnProfile>("full");
  const [customColumns, setCustomColumns] = useState<string[]>([]);
  const [fullColumns, setFullColumns] = useState<string[]>([]);
  const [startDate, setStartDate] = useState(initialRange.startDate);
  const [endDate, setEndDate] = useState(initialRange.endDate);
  const [preview, setPreview] = useState<FocusPreviewRequest | null>(null);
  const [recentRequests, setRecentRequests] = useState<FocusPreviewRequest[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [revisions, setRevisions] = useState<FocusPreviewRevisionSummary[]>([]);
  const [revisionNextCursor, setRevisionNextCursor] = useState<string | null>(null);
  const [selectedRevision, setSelectedRevision] = useState<FocusPreviewRevision | null>(null);
  const [busy, setBusy] = useState(false);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [revisionBusy, setRevisionBusy] = useState(false);
  const [revisionControlsVisible, setRevisionControlsVisible] = useState(false);
  const [revisionError, setRevisionError] = useState<string | null>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const submitAbortRef = useRef<AbortController | null>(null);
  const revisionDetailAbortRef = useRef<AbortController | null>(null);
  const controllersRef = useRef(new Set<AbortController>());
  const revisionControllersRef = useRef(new Set<AbortController>());
  const tenantGenerationRef = useRef(0);
  const revisionGenerationRef = useRef(0);
  const tenantNameRef = useRef<string | null>(currentTenant?.tenant_name ?? null);
  tenantNameRef.current = currentTenant?.tenant_name ?? null;

  useEffect(() => {
    tenantGenerationRef.current += 1;
    const generation = tenantGenerationRef.current;
    const controllers = controllersRef.current;
    for (const controller of controllers) controller.abort();
    controllers.clear();
    revisionControllersRef.current.clear();
    submitAbortRef.current = null;
    revisionDetailAbortRef.current = null;
    setPreview(null);
    setRecentRequests([]);
    setNextCursor(null);
    setRevisions([]);
    setRevisionNextCursor(null);
    setSelectedRevision(null);
    setBusy(false);
    setHistoryBusy(false);
    setRevisionBusy(false);
    setRevisionControlsVisible(false);
    setOperationError(null);
    setRevisionError(null);
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
    revisionGenerationRef.current += 1;
    const revisionGeneration = revisionGenerationRef.current;
    const tenantGeneration = tenantGenerationRef.current;
    const controllers = revisionControllersRef.current;
    const allControllers = controllersRef.current;
    for (const controller of controllers) controller.abort();
    controllers.clear();
    setRevisions([]);
    setRevisionNextCursor(null);
    setSelectedRevision(null);
    setRevisionBusy(false);
    setRevisionControlsVisible(false);
    setRevisionError(null);
    if (!currentTenant) return;

    const tenantName = currentTenant.tenant_name;
    const controller = new AbortController();
    controllers.add(controller);
    allControllers.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      revisionGenerationRef.current === revisionGeneration &&
      tenantGenerationRef.current === tenantGeneration &&
      tenantNameRef.current === tenantName;

    const visibilityTimer = window.setTimeout(() => {
      if (active()) setRevisionControlsVisible(true);
    }, 0);

    setRevisionBusy(true);
    void listFocusPreviewRevisions(tenantName, {
      month: revisionMonth,
      signal: controller.signal,
    })
      .then((page) => {
        if (!active()) return;
        setRevisions(page.items);
        setRevisionNextCursor(page.next_cursor);
      })
      .catch((error: unknown) => {
        if (active() && !isAbortError(error)) {
          setRevisionError(REVISION_ERROR_MESSAGE);
        }
      })
      .finally(() => {
        controllers.delete(controller);
        allControllers.delete(controller);
        if (active()) setRevisionBusy(false);
      });

    return () => {
      window.clearTimeout(visibilityTimer);
      controller.abort();
      controllers.delete(controller);
      allControllers.delete(controller);
    };
  }, [currentTenant, revisionMonth]);

  useEffect(() => {
    const controllers = controllersRef.current;
    const revisionControllers = revisionControllersRef.current;
    return () => {
      for (const controller of controllers) controller.abort();
      controllers.clear();
      revisionControllers.clear();
      submitAbortRef.current = null;
      revisionDetailAbortRef.current = null;
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

  async function refreshRevisions(): Promise<void> {
    if (!currentTenant) return;
    revisionGenerationRef.current += 1;
    const revisionGeneration = revisionGenerationRef.current;
    const tenantGeneration = tenantGenerationRef.current;
    const tenantName = currentTenant.tenant_name;
    for (const controller of revisionControllersRef.current) controller.abort();
    revisionControllersRef.current.clear();
    revisionDetailAbortRef.current = null;
    setRevisions([]);
    setRevisionNextCursor(null);
    setSelectedRevision(null);
    setRevisionBusy(true);
    setRevisionError(null);
    const controller = new AbortController();
    revisionControllersRef.current.add(controller);
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      revisionGenerationRef.current === revisionGeneration &&
      tenantGenerationRef.current === tenantGeneration &&
      tenantNameRef.current === tenantName;
    try {
      const page = await listFocusPreviewRevisions(tenantName, {
        month: revisionMonth,
        signal: controller.signal,
      });
      if (!active()) return;
      setRevisions(page.items);
      setRevisionNextCursor(page.next_cursor);
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setRevisionError(REVISION_ERROR_MESSAGE);
      }
    } finally {
      revisionControllersRef.current.delete(controller);
      controllersRef.current.delete(controller);
      if (active()) setRevisionBusy(false);
    }
  }

  async function loadMoreRevisions(): Promise<void> {
    if (!currentTenant || !revisionNextCursor) return;
    const tenantName = currentTenant.tenant_name;
    const tenantGeneration = tenantGenerationRef.current;
    const revisionGeneration = revisionGenerationRef.current;
    const cursor = revisionNextCursor;
    const controller = new AbortController();
    revisionControllersRef.current.add(controller);
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      revisionGenerationRef.current === revisionGeneration &&
      tenantGenerationRef.current === tenantGeneration &&
      tenantNameRef.current === tenantName;
    setRevisionBusy(true);
    setRevisionError(null);
    try {
      const page = await listFocusPreviewRevisions(tenantName, {
        month: revisionMonth,
        cursor,
        signal: controller.signal,
      });
      if (!active()) return;
      setRevisions((current) => [...current, ...page.items]);
      setRevisionNextCursor(page.next_cursor);
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setRevisionError(REVISION_ERROR_MESSAGE);
      }
    } finally {
      revisionControllersRef.current.delete(controller);
      controllersRef.current.delete(controller);
      if (active()) setRevisionBusy(false);
    }
  }

  async function viewRevision(revisionId: string): Promise<void> {
    if (!currentTenant) return;
    const tenantName = currentTenant.tenant_name;
    const tenantGeneration = tenantGenerationRef.current;
    const revisionGeneration = revisionGenerationRef.current;
    revisionDetailAbortRef.current?.abort();
    const controller = new AbortController();
    revisionDetailAbortRef.current = controller;
    revisionControllersRef.current.add(controller);
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      revisionGenerationRef.current === revisionGeneration &&
      tenantGenerationRef.current === tenantGeneration &&
      tenantNameRef.current === tenantName;
    setSelectedRevision(null);
    setRevisionError(null);
    try {
      const detail = await fetchFocusPreviewRevision(
        tenantName,
        revisionId,
        controller.signal,
      );
      if (active()) setSelectedRevision(detail);
    } catch (error) {
      if (active() && !isAbortError(error)) {
        setRevisionError(REVISION_ERROR_MESSAGE);
      }
    } finally {
      revisionControllersRef.current.delete(controller);
      controllersRef.current.delete(controller);
      if (revisionDetailAbortRef.current === controller) {
        revisionDetailAbortRef.current = null;
      }
    }
  }

  async function downloadRevision(downloadUrl: string, fileName: string): Promise<void> {
    const tenantName = currentTenant?.tenant_name;
    if (!tenantName) return;
    const tenantGeneration = tenantGenerationRef.current;
    const revisionGeneration = revisionGenerationRef.current;
    const controller = new AbortController();
    revisionControllersRef.current.add(controller);
    controllersRef.current.add(controller);
    const active = (): boolean =>
      !controller.signal.aborted &&
      revisionGenerationRef.current === revisionGeneration &&
      tenantGenerationRef.current === tenantGeneration &&
      tenantNameRef.current === tenantName;
    setRevisionError(null);
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
        setRevisionError(REVISION_ERROR_MESSAGE);
      }
    } finally {
      revisionControllersRef.current.delete(controller);
      controllersRef.current.delete(controller);
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
      <section>
        <Space direction="vertical" size="large" style={{ width: "100%" }}>
          <Title id="focus-preview-published-revisions" level={3}>
            Published monthly revisions
          </Title>
          <Alert
            type="warning"
            showIcon
            message="Each revision is a complete replacement. Use the current revision; do not aggregate revisions."
          />
          {revisionError && <Alert type="error" showIcon message={revisionError} />}
          {revisionControlsVisible && (
            <Space wrap>
              <label>
                Revision month
                <input
                  aria-label="Revision month"
                  type="month"
                  value={revisionMonth}
                  onChange={(event) => setRevisionMonth(event.target.value)}
                />
              </label>
              <Button
                loading={revisionBusy}
                disabled={!currentTenant}
                onClick={() => void refreshRevisions()}
              >
                Refresh revisions
              </Button>
            </Space>
          )}
          {revisions.map((revision) => (
            <PreviewRevisionDetails
              key={revision.revision_id}
              revision={revision}
              onView={(revisionId) => {
                void viewRevision(revisionId);
              }}
            />
          ))}
          {selectedRevision && (
            <PreviewRevisionDownload
              revision={selectedRevision}
              onDownload={(downloadUrl, fileName) => {
                void downloadRevision(downloadUrl, fileName);
              }}
            />
          )}
          {revisionNextCursor && (
            <Button loading={revisionBusy} onClick={() => void loadMoreRevisions()}>
              Load more revisions
            </Button>
          )}
        </Space>
      </section>
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
