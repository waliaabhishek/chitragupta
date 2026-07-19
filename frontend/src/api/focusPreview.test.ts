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
  column_profile: "full",
  status: "queued",
  created_at: "2026-07-03T00:00:00Z",
  started_at: null,
  completed_at: null,
  diagnostic: null,
  source_snapshot: null,
  package: null,
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

  it("contains no mapping, CSV generation, checksum, or filesystem logic", async () => {
    const source = await import("./focusPreview?raw");
    const text = source.default;

    expect(text).not.toContain("mapping");
    expect(text).not.toContain("csv.writer");
    expect(text).not.toContain("sha256");
    expect(text).not.toContain("storage_key");
  });
});
