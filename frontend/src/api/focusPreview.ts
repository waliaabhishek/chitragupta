import { API_URL } from "../config";

export interface FocusPreviewDiagnostic {
  code: string;
  message: string;
  retryable: boolean;
}

export interface FocusPreviewArtifact {
  name: string;
  media_type: string;
  size_bytes: number;
  order?: number;
  download_url: string;
  [key: string]: unknown;
}

export interface FocusPreviewRequest {
  request_id: string;
  tenant_name: string;
  grain: "daily";
  start_date: string;
  end_date: string;
  column_profile: "full";
  status: "queued" | "running" | "ready" | "failed" | "expired";
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  diagnostic: FocusPreviewDiagnostic | null;
  source_snapshot: unknown | null;
  package: {
    manifest: FocusPreviewArtifact;
    files: FocusPreviewArtifact[];
  } | null;
}

export interface SubmitFocusPreviewBody {
  grain: "daily";
  start_date: string;
  end_date: string;
  column_profile: "full";
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
): Promise<FocusPreviewRequest> {
  const response = await requireOk(
    await fetch(`${API_URL}/tenants/${tenantName}/focus-preview/requests`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
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
): Promise<Blob> {
  const response = await requireOk(
    await fetch(resolvePreviewDownloadUrl(downloadUrl)),
  );
  return response.blob();
}
