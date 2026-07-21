import { API_URL } from "../config";

export interface FocusPreviewDiagnostic {
  code: string;
  message: string;
  retryable: boolean;
  source_correlation_ids?: string[];
}

export interface FocusPreviewArtifact {
  name: string;
  media_type: string;
  size_bytes: number;
  sha256: string;
  order?: number;
  download_url: string;
  [key: string]: unknown;
}

export interface FocusPreviewSourceSnapshot {
  calculation_timestamp: string | null;
  calculation_coverage: unknown[];
  source_through: string | null;
  effective_coverage_start_date: string;
  effective_coverage_end_date: string;
  evidence_through_date: string | null;
  availability_cutoff_end_date: string | null;
  monthly_status: "provisional" | "settled" | null;
}

export interface FocusPreviewRequest {
  request_id: string;
  tenant_name: string;
  grain: "daily" | "monthly";
  start_date: string;
  end_date: string;
  month: string | null;
  column_profile: "full" | "summary" | "custom";
  effective_columns: string[];
  status: "queued" | "running" | "ready" | "failed" | "expired";
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  expires_at: string | null;
  diagnostic: FocusPreviewDiagnostic | null;
  source_snapshot: FocusPreviewSourceSnapshot | null;
  package: {
    manifest: FocusPreviewArtifact;
    files: FocusPreviewArtifact[];
    download_all_name: string;
    download_all_url: string;
  } | null;
}

export interface FocusPreviewRequestPage {
  items: FocusPreviewRequest[];
  next_cursor: string | null;
}

export interface FocusPreviewRevisionValidationSummary {
  status: "passed";
  mapping_profile_version: string;
  source_records: number;
  rows: number;
  mapping_errors: 0;
  artifact_integrity: "passed";
}

export interface FocusPreviewRevisionSummary {
  revision_id: string;
  tenant_name: string;
  month: string;
  start_date: string;
  end_date: string;
  lifecycle: "current" | "superseded";
  monthly_status: "provisional" | "settled";
  published_at: string;
  supersedes_revision_id: string | null;
  superseded_by_revision_id: string | null;
  material_sha256: string;
  source_snapshot: FocusPreviewSourceSnapshot;
  validation: FocusPreviewRevisionValidationSummary;
  replacement_semantics: "complete_replacement";
  consumer_action: "replace_do_not_aggregate";
  detail_url: string;
}

export interface FocusPreviewRevisionPage {
  items: FocusPreviewRevisionSummary[];
  next_cursor: string | null;
  replacement_semantics: "complete_replacement";
  consumer_action: "replace_do_not_aggregate";
}

export interface FocusPreviewRevision extends FocusPreviewRevisionSummary {
  self_url: string;
  package: {
    manifest: FocusPreviewArtifact;
    files: FocusPreviewArtifact[];
    download_all_name: string;
    download_all_url: string;
  };
}

export interface ListFocusPreviewRequestsOptions {
  limit?: number;
  cursor?: string;
  signal?: AbortSignal;
}

export interface ListFocusPreviewRevisionsOptions {
  month: string;
  limit?: number;
  cursor?: string;
  signal?: AbortSignal;
}

export type FocusPreviewColumnProfile = "full" | "summary" | "custom";

interface FocusPreviewColumnSelection {
  column_profile: FocusPreviewColumnProfile;
  columns?: readonly string[];
}

export interface SubmitDailyFocusPreviewBody extends FocusPreviewColumnSelection {
  grain: "daily";
  start_date: string;
  end_date: string;
}

export interface SubmitMonthlyFocusPreviewBody extends FocusPreviewColumnSelection {
  grain: "monthly";
  month: string;
}

export type SubmitFocusPreviewBody =
  | SubmitDailyFocusPreviewBody
  | SubmitMonthlyFocusPreviewBody;

export interface FocusPreviewProfile {
  mapping_profile_version: string;
  full_columns: string[];
  summary_columns: string[];
}

async function requireOk(response: Response): Promise<Response> {
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  return response;
}

export async function submitFocusPreview(
  tenantName: string,
  body: SubmitFocusPreviewBody,
  signal?: AbortSignal,
): Promise<FocusPreviewRequest> {
  const response = await requireOk(
    await fetch(`${API_URL}/tenants/${tenantName}/focus-preview/requests`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      ...(signal ? { signal } : {}),
    }),
  );
  return response.json() as Promise<FocusPreviewRequest>;
}

export async function fetchFocusPreviewStatus(
  tenantName: string,
  requestId: string,
  signal?: AbortSignal,
): Promise<FocusPreviewRequest> {
  const response = await requireOk(
    await fetch(
      `${API_URL}/tenants/${tenantName}/focus-preview/requests/${requestId}`,
      { signal },
    ),
  );
  return response.json() as Promise<FocusPreviewRequest>;
}

export async function listFocusPreviewRequests(
  tenantName: string,
  options: ListFocusPreviewRequestsOptions = {},
): Promise<FocusPreviewRequestPage> {
  const params = new URLSearchParams();
  if (options.limit !== undefined) {
    params.set("limit", String(options.limit));
  }
  if (options.cursor !== undefined) {
    params.set("cursor", options.cursor);
  }
  const query = params.toString();
  const url = `${API_URL}/tenants/${tenantName}/focus-preview/requests${query ? `?${query}` : ""}`;
  const response = await requireOk(
    await (options.signal ? fetch(url, { signal: options.signal }) : fetch(url)),
  );
  return response.json() as Promise<FocusPreviewRequestPage>;
}

export async function listFocusPreviewRevisions(
  tenantName: string,
  options: ListFocusPreviewRevisionsOptions,
): Promise<FocusPreviewRevisionPage> {
  const params = new URLSearchParams({ month: options.month });
  if (options.limit !== undefined) {
    params.set("limit", String(options.limit));
  }
  if (options.cursor !== undefined) {
    params.set("cursor", options.cursor);
  }
  const url = `${API_URL}/tenants/${tenantName}/focus-preview/revisions?${params.toString()}`;
  const response = await requireOk(
    await (options.signal ? fetch(url, { signal: options.signal }) : fetch(url)),
  );
  return response.json() as Promise<FocusPreviewRevisionPage>;
}

export async function fetchFocusPreviewRevision(
  tenantName: string,
  revisionId: string,
  signal?: AbortSignal,
): Promise<FocusPreviewRevision> {
  const url = `${API_URL}/tenants/${tenantName}/focus-preview/revisions/${revisionId}`;
  const response = await requireOk(
    await (signal ? fetch(url, { signal }) : fetch(url)),
  );
  return response.json() as Promise<FocusPreviewRevision>;
}

export async function fetchFocusPreviewProfile(
  tenantName: string,
  signal?: AbortSignal,
): Promise<FocusPreviewProfile> {
  const url = `${API_URL}/tenants/${tenantName}/focus-preview/profile`;
  const response = await requireOk(
    await (signal ? fetch(url, { signal }) : fetch(url)),
  );
  return response.json() as Promise<FocusPreviewProfile>;
}

export function resolvePreviewDownloadUrl(downloadUrl: string): string {
  const apiBase = new URL(API_URL, window.location.origin);
  const resolved = downloadUrl.startsWith("/")
    ? new URL(downloadUrl, apiBase.origin)
    : new URL(downloadUrl, `${apiBase.toString().replace(/\/?$/, "/")}`);
  if (resolved.origin !== apiBase.origin) {
    throw new Error("Preview download URL has an unexpected origin");
  }
  return resolved.toString();
}

export async function fetchPreviewArtifact(
  downloadUrl: string,
  signal?: AbortSignal,
): Promise<Blob> {
  const url = resolvePreviewDownloadUrl(downloadUrl);
  const response = await requireOk(
    await (signal ? fetch(url, { signal }) : fetch(url)),
  );
  return response.blob();
}
