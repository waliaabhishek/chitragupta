import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "../test/mocks/server";

const API_ORIGIN = "https://api.example.test";
const API_BASE = `${API_ORIGIN}/api/v1`;

const queued = {
  request_id: "request-1",
  tenant_name: "production",
  grain: "daily",
  start_date: "2026-07-01",
  end_date: "2026-07-02",
  month: null,
  column_profile: "full",
  effective_columns: ["BilledCost"],
  status: "queued",
  created_at: "2026-07-03T00:00:00Z",
  started_at: null,
  completed_at: null,
  expires_at: null,
  diagnostic: null,
  source_snapshot: null,
  package: null,
};

const readyPackage = {
  ...queued,
  status: "ready",
  completed_at: "2026-07-03T00:01:00Z",
  expires_at: "2026-07-10T00:01:00Z",
  source_snapshot: {
    calculation_timestamp: "2026-07-02T23:55:00Z",
    calculation_coverage: [],
    source_through: "2026-07-02T23:50:00Z",
    effective_coverage_start_date: "2026-07-01",
    effective_coverage_end_date: "2026-07-02",
    evidence_through_date: "2026-07-01",
    availability_cutoff_end_date: "2026-07-02",
    monthly_status: null,
  },
  package: {
    manifest: {
      name: "manifest.json",
      media_type: "application/json",
      size_bytes: 321,
      sha256: "a".repeat(64),
      download_url:
        "/api/v1/tenants/production/focus-preview/requests/request-1/manifest",
    },
    files: [
      {
        name: "cost-and-usage-part-00001-of-00002.csv",
        media_type: "text/csv",
        size_bytes: 12,
        sha256: "b".repeat(64),
        order: 1,
        download_url:
          "/api/v1/tenants/production/focus-preview/requests/request-1/files/cost-and-usage-part-00001-of-00002.csv",
      },
      {
        name: "cost-and-usage-part-00002-of-00002.csv",
        media_type: "text/csv",
        size_bytes: 11,
        sha256: "c".repeat(64),
        order: 2,
        download_url:
          "/api/v1/tenants/production/focus-preview/requests/request-1/files/cost-and-usage-part-00002-of-00002.csv",
      },
    ],
    download_all_name: "focus-mapping-preview-request-1.zip",
    download_all_url:
      "/api/v1/tenants/production/focus-preview/requests/request-1/archive",
  },
};

const revisionSummary = {
  revision_id: "revision-2",
  tenant_name: "production",
  month: "2026-07",
  start_date: "2026-07-01",
  end_date: "2026-08-01",
  lifecycle: "current",
  monthly_status: "provisional",
  published_at: "2026-07-21T12:00:00Z",
  supersedes_revision_id: "revision-1",
  superseded_by_revision_id: null,
  material_sha256: "d".repeat(64),
  source_snapshot: {
    calculation_timestamp: "2026-07-21T11:55:00Z",
    calculation_coverage: [],
    source_through: "2026-07-21T11:50:00Z",
    effective_coverage_start_date: "2026-07-01",
    effective_coverage_end_date: "2026-07-21",
    evidence_through_date: "2026-07-20",
    availability_cutoff_end_date: "2026-07-21",
    monthly_status: "provisional",
  },
  validation: {
    status: "passed",
    mapping_profile_version: "focus-1.4-preview-v5",
    source_records: 12,
    rows: 10,
    mapping_errors: 0,
    artifact_integrity: "passed",
  },
  replacement_semantics: "complete_replacement",
  consumer_action: "replace_do_not_aggregate",
  detail_url:
    "/api/v1/tenants/production/focus-preview/revisions/revision-2",
};

const revisionDetail = {
  ...revisionSummary,
  self_url:
    "/api/v1/tenants/production/focus-preview/revisions/revision-2",
  package: {
    manifest: {
      name: "manifest.json",
      media_type: "application/json",
      size_bytes: 321,
      sha256: "a".repeat(64),
      download_url:
        "/api/v1/tenants/production/focus-preview/revisions/revision-2/manifest",
    },
    files: [
      {
        name: "cost-and-usage.csv",
        media_type: "text/csv",
        size_bytes: 12,
        sha256: "b".repeat(64),
        order: 1,
        download_url:
          "/api/v1/tenants/production/focus-preview/revisions/revision-2/files/cost-and-usage.csv",
      },
    ],
    download_all_name: "focus-mapping-preview-revision-2.zip",
    download_all_url:
      "/api/v1/tenants/production/focus-preview/revisions/revision-2/archive",
  },
};

const correlatedDiagnostic: import("./focusPreview").FocusPreviewDiagnostic = {
  code: "preview_source_record_malformed",
  message: "One or more persisted Confluent Costs API records are malformed.",
  retryable: false,
  source_correlation_ids: [`src:v1:${"a".repeat(64)}`],
};

expect(correlatedDiagnostic.source_correlation_ids).toHaveLength(1);

async function loadClient() {
  vi.resetModules();
  vi.stubEnv("VITE_API_URL", API_BASE);
  return import("./focusPreview");
}

describe("FOCUS Mapping Preview API delegation", () => {
  beforeEach(() => {
    vi.stubEnv("VITE_API_URL", API_BASE);
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("submits the fixed Daily Full request to the configured API origin", async () => {
    let capturedUrl = "";
    let capturedBody: unknown;
    server.use(
      http.post(
        `${API_BASE}/tenants/production/focus-preview/requests`,
        async ({ request }) => {
          capturedUrl = request.url;
          capturedBody = await request.json();
          return HttpResponse.json(queued, { status: 202 });
        },
      ),
    );
    const { submitFocusPreview } = await loadClient();

    const response = await submitFocusPreview("production", {
      grain: "daily",
      start_date: "2026-07-01",
      end_date: "2026-07-02",
      column_profile: "full",
    });

    expect(capturedUrl).toBe(
      `${API_BASE}/tenants/production/focus-preview/requests`,
    );
    expect(capturedBody).toEqual({
      grain: "daily",
      start_date: "2026-07-01",
      end_date: "2026-07-02",
      column_profile: "full",
    });
    expect(response).toEqual(queued);
  });

  it("polls status through the API and honors cancellation", async () => {
    let requests = 0;
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests/request-1`,
        () => {
          requests += 1;
          return HttpResponse.json({
            ...queued,
            status: requests === 1 ? "running" : "ready",
          });
        },
      ),
    );
    const { fetchFocusPreviewStatus } = await loadClient();
    const controller = new AbortController();

    const running = await fetchFocusPreviewStatus(
      "production",
      "request-1",
      controller.signal,
    );
    controller.abort();

    expect(running.status).toBe("running");
    await expect(
      fetchFocusPreviewStatus(
        "production",
        "request-1",
        controller.signal,
      ),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(requests).toBe(1);
  });

  it("lists recent requests using the server cursor without reordering the response", async () => {
    let capturedUrl = "";
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests`,
        ({ request }) => {
          capturedUrl = request.url;
          return HttpResponse.json({
            items: [
              { ...readyPackage, request_id: "request-3" },
              { ...queued, request_id: "request-2", status: "failed" },
            ],
            next_cursor: "request-2",
          });
        },
      ),
    );
    const { listFocusPreviewRequests } = await loadClient();

    const page = await listFocusPreviewRequests("production", {
      limit: 2,
      cursor: "request-4",
    });

    expect(capturedUrl).toBe(
      `${API_BASE}/tenants/production/focus-preview/requests?limit=2&cursor=request-4`,
    );
    expect(page.items.map((item: { request_id: string }) => item.request_id)).toEqual([
      "request-3",
      "request-2",
    ]);
    expect(page.next_cursor).toBe("request-2");
  });

  it("lists published revisions for one month using the server keyset cursor", async () => {
    let capturedUrl = "";
    let capturedSignal: AbortSignal | undefined;
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions`,
        ({ request }) => {
          capturedUrl = request.url;
          capturedSignal = request.signal;
          return HttpResponse.json({
            items: [revisionSummary],
            next_cursor: "revision-2",
            replacement_semantics: "complete_replacement",
            consumer_action: "replace_do_not_aggregate",
          });
        },
      ),
    );
    const { listFocusPreviewRevisions } = await loadClient();
    const controller = new AbortController();

    const page = await listFocusPreviewRevisions("production", {
      month: "2026-07",
      limit: 1,
      cursor: "revision-3",
      signal: controller.signal,
    });

    expect(capturedUrl).toBe(
      `${API_BASE}/tenants/production/focus-preview/revisions?month=2026-07&limit=1&cursor=revision-3`,
    );
    expect(capturedSignal).toBeInstanceOf(AbortSignal);
    controller.abort();
    expect(capturedSignal?.aborted).toBe(true);
    expect(page).toEqual({
      items: [revisionSummary],
      next_cursor: "revision-2",
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });
  });

  it("covers minimal revision queries and optional signal branches", async () => {
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions`,
        () => HttpResponse.json({
          items: [],
          next_cursor: null,
          replacement_semantics: "complete_replacement",
          consumer_action: "replace_do_not_aggregate",
        }),
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2`,
        () => HttpResponse.json(revisionDetail),
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/profile`,
        () => HttpResponse.json({
          mapping_profile_version: "focus-1.4-preview-v5",
          full_columns: ["BilledCost"],
          summary_columns: ["BilledCost"],
        }),
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/manifest`,
        () => new HttpResponse("{}\n"),
      ),
    );
    const {
      fetchFocusPreviewProfile,
      fetchFocusPreviewRevision,
      fetchPreviewArtifact,
      listFocusPreviewRevisions,
    } = await loadClient();
    const controller = new AbortController();

    await expect(listFocusPreviewRevisions("production", { month: "2026-07" }))
      .resolves.toMatchObject({ items: [], next_cursor: null });
    await expect(fetchFocusPreviewRevision(
      "production",
      "revision-2",
      controller.signal,
    )).resolves.toEqual(revisionDetail);
    await expect(fetchFocusPreviewProfile("production"))
      .resolves.toMatchObject({ mapping_profile_version: "focus-1.4-preview-v5" });
    await expect(fetchPreviewArtifact(
      revisionDetail.package.manifest.download_url,
      controller.signal,
    )).resolves.toBeInstanceOf(Blob);
  });

  it("rejects non-success revision responses through the shared HTTP guard", async () => {
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions`,
        () => new HttpResponse(null, { status: 503, statusText: "Unavailable" }),
      ),
    );
    const { listFocusPreviewRevisions } = await loadClient();

    await expect(listFocusPreviewRevisions("production", { month: "2026-07" }))
      .rejects.toThrow("HTTP 503: Unavailable");
  });

  it("covers optional signal and empty request-list query branches", async () => {
    const seen: string[] = [];
    server.use(
      http.post(
        `${API_BASE}/tenants/production/focus-preview/requests`,
        ({ request }) => {
          seen.push(request.url);
          return HttpResponse.json(queued, { status: 202 });
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests`,
        ({ request }) => {
          seen.push(request.url);
          return HttpResponse.json({ items: [], next_cursor: null });
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/profile`,
        ({ request }) => {
          seen.push(request.url);
          return HttpResponse.json({
            mapping_profile_version: "focus-1.4-preview-v5",
            full_columns: ["BilledCost"],
            summary_columns: ["BilledCost"],
          });
        },
      ),
    );
    const {
      fetchFocusPreviewProfile,
      listFocusPreviewRequests,
      submitFocusPreview,
    } = await loadClient();
    const controller = new AbortController();

    await submitFocusPreview("production", {
      grain: "monthly",
      month: "2026-07",
      column_profile: "full",
    }, controller.signal);
    await listFocusPreviewRequests("production");
    await listFocusPreviewRequests("production", { signal: controller.signal });
    await fetchFocusPreviewProfile("production", controller.signal);

    expect(seen).toEqual([
      `${API_BASE}/tenants/production/focus-preview/requests`,
      `${API_BASE}/tenants/production/focus-preview/requests`,
      `${API_BASE}/tenants/production/focus-preview/requests`,
      `${API_BASE}/tenants/production/focus-preview/profile`,
    ]);
  });

  it("retrieves immutable revision detail and all direct artifact URLs", async () => {
    const requested: string[] = [];
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2`,
        ({ request }) => {
          requested.push(request.url);
          return HttpResponse.json(revisionDetail);
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/manifest`,
        ({ request }) => {
          requested.push(request.url);
          return new HttpResponse("{}\n");
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/files/cost-and-usage.csv`,
        ({ request }) => {
          requested.push(request.url);
          return new HttpResponse("a,b\n");
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/archive`,
        ({ request }) => {
          requested.push(request.url);
          return new HttpResponse(new Uint8Array([0x50, 0x4b]));
        },
      ),
    );
    const { fetchFocusPreviewRevision, fetchPreviewArtifact } = await loadClient();

    const detail = await fetchFocusPreviewRevision(
      "production",
      "revision-2",
    );
    const manifest = await fetchPreviewArtifact(detail.package.manifest.download_url);
    const file = await fetchPreviewArtifact(detail.package.files[0].download_url);
    const archive = await fetchPreviewArtifact(detail.package.download_all_url);

    expect(detail).toEqual(revisionDetail);
    expect(await manifest.text()).toBe("{}\n");
    expect(await file.text()).toBe("a,b\n");
    expect([...new Uint8Array(await archive.arrayBuffer())]).toEqual([0x50, 0x4b]);
    expect(requested).toEqual([
      `${API_BASE}/tenants/production/focus-preview/revisions/revision-2`,
      `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/manifest`,
      `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/files/cost-and-usage.csv`,
      `${API_BASE}/tenants/production/focus-preview/revisions/revision-2/archive`,
    ]);
  });

  it("consumes ready status and package bytes exactly as supplied by the API", async () => {
    const archiveBytes = new Uint8Array([0x50, 0x4b, 0x03, 0x04, 0x07]);
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests/request-1`,
        () => HttpResponse.json(readyPackage),
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests/request-1/archive`,
        () => new HttpResponse(archiveBytes),
      ),
    );
    const { fetchFocusPreviewStatus, fetchPreviewArtifact } = await loadClient();

    const status = await fetchFocusPreviewStatus("production", "request-1");
    const archive = await fetchPreviewArtifact(status.package!.download_all_url);

    expect(status).toEqual(readyPackage);
    expect(status.package!.files.map((file) => file.name)).toEqual([
      "cost-and-usage-part-00001-of-00002.csv",
      "cost-and-usage-part-00002-of-00002.csv",
    ]);
    expect([...new Uint8Array(await archive.arrayBuffer())]).toEqual([
      0x50, 0x4b, 0x03, 0x04, 0x07,
    ]);
  });

  it("resolves origin-relative manifest and file URLs against an absolute API origin", async () => {
    const { resolvePreviewDownloadUrl } = await loadClient();

    expect(
      resolvePreviewDownloadUrl(
        "/api/v1/tenants/production/focus-preview/requests/request-1/manifest",
      ),
    ).toBe(
      `${API_BASE}/tenants/production/focus-preview/requests/request-1/manifest`,
    );
    expect(
      resolvePreviewDownloadUrl(
        "tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv",
      ),
    ).toBe(
      `${API_BASE}/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv`,
    );
  });

  it("downloads manifest and CSV bytes through the API client", async () => {
    const requested: string[] = [];
    server.use(
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests/request-1/manifest`,
        ({ request }) => {
          requested.push(request.url);
          return new HttpResponse("{}\n", {
            headers: { "Content-Type": "application/json" },
          });
        },
      ),
      http.get(
        `${API_BASE}/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv`,
        ({ request }) => {
          requested.push(request.url);
          return new HttpResponse("a,b\n", {
            headers: { "Content-Type": "text/csv" },
          });
        },
      ),
    );
    const { fetchPreviewArtifact } = await loadClient();

    const manifest = await fetchPreviewArtifact(
      "/api/v1/tenants/production/focus-preview/requests/request-1/manifest",
    );
    const csv = await fetchPreviewArtifact(
      "/api/v1/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv",
    );

    expect(await manifest.text()).toBe("{}\n");
    expect(await csv.text()).toBe("a,b\n");
    expect(requested).toEqual([
      `${API_BASE}/tenants/production/focus-preview/requests/request-1/manifest`,
      `${API_BASE}/tenants/production/focus-preview/requests/request-1/files/cost-and-usage.csv`,
    ]);
  });

  it("rejects cross-origin API-provided URLs before fetch", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const { fetchPreviewArtifact } = await loadClient();

    await expect(
      fetchPreviewArtifact("https://evil.example/steal"),
    ).rejects.toThrow(/origin/i);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("contains no mapping, CSV generation, checksum mutation, archive construction, or filesystem logic", async () => {
    const source = await import("./focusPreview?raw");
    const text = source.default;

    expect(text).not.toContain("core.preview.mapping");
    expect(text).not.toContain("csv.writer");
    expect(text).not.toContain("crypto.subtle");
    expect(text).not.toContain("createHash");
    expect(text).not.toContain("digest(");
    expect(text).not.toContain("storage_key");
    expect(text).not.toContain("server_path");
    expect(text).not.toContain("JSZip");
    expect(text).not.toContain("ZipWriter");
    expect(text).not.toContain("createArchive");
  });
});
