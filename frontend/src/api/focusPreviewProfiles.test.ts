import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "../test/mocks/server";

const API_ORIGIN = "https://api.example.test";
const API_BASE = `${API_ORIGIN}/api/v1`;

async function loadClient() {
  vi.resetModules();
  vi.stubEnv("VITE_API_URL", API_BASE);
  return import("./focusPreview");
}

describe("FOCUS Preview grain and profile API contracts", () => {
  beforeEach(() => vi.stubEnv("VITE_API_URL", API_BASE));
  afterEach(() => vi.unstubAllEnvs());

  it.each([
    [
      { grain: "monthly", month: "2026-07", column_profile: "full" },
      { grain: "monthly", month: "2026-07", column_profile: "full" },
    ],
    [
      {
        grain: "daily",
        start_date: "2026-07-01",
        end_date: "2026-07-02",
        column_profile: "summary",
      },
      {
        grain: "daily",
        start_date: "2026-07-01",
        end_date: "2026-07-02",
        column_profile: "summary",
      },
    ],
    [
      {
        grain: "monthly",
        month: "2026-07",
        column_profile: "custom",
        columns: ["Tags", "BilledCost"],
      },
      {
        grain: "monthly",
        month: "2026-07",
        column_profile: "custom",
        columns: ["Tags", "BilledCost"],
      },
    ],
  ] as const)("serializes one discriminated request without changing Custom order", async (body, expected) => {
    let captured: unknown;
    server.use(
      http.post(`${API_BASE}/tenants/production/focus-preview/requests`, async ({ request }) => {
        captured = await request.json();
        return HttpResponse.json(
          {
            request_id: "request-1",
            tenant_name: "production",
            grain: body.grain,
            start_date: "2026-07-01",
            end_date: body.grain === "monthly" ? "2026-08-01" : "2026-07-02",
            month: body.grain === "monthly" ? "2026-07" : null,
            column_profile: body.column_profile,
            effective_columns: body.column_profile === "custom" ? body.columns : ["BilledCost"],
            status: "queued",
            created_at: "2026-07-03T00:00:00Z",
            started_at: null,
            completed_at: null,
            diagnostic: null,
            source_snapshot: null,
            package: null,
          },
          { status: 202 },
        );
      }),
    );
    const { submitFocusPreview } = await loadClient();

    await submitFocusPreview("production", body);

    expect(captured).toEqual(expected);
  });

  it("loads code-owned profile metadata from the static endpoint", async () => {
    server.use(
      http.get(`${API_BASE}/tenants/production/focus-preview/profile`, () =>
        HttpResponse.json({
          mapping_profile_version: "focus-1.4-preview-v5",
          full_columns: ["BilledCost", "Tags"],
          summary_columns: ["BilledCost"],
        }),
      ),
    );
    const { fetchFocusPreviewProfile } = await loadClient();

    await expect(fetchFocusPreviewProfile("production")).resolves.toEqual({
      mapping_profile_version: "focus-1.4-preview-v5",
      full_columns: ["BilledCost", "Tags"],
      summary_columns: ["BilledCost"],
    });
  });
});
