import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FocusPreviewPage } from ".";
import {
  fetchFocusPreviewProfile,
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  submitFocusPreview,
} from "../../api/focusPreview";

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: { tenant_name: "production", tenant_id: "tenant-1" },
    tenants: [],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
}));

vi.mock("../../api/focusPreview", () => ({
  submitFocusPreview: vi.fn(),
  fetchFocusPreviewProfile: vi.fn(),
  fetchFocusPreviewStatus: vi.fn(),
  fetchPreviewArtifact: vi.fn(),
}));

const baseRequest = {
  request_id: "request-1",
  tenant_name: "production",
  grain: "daily" as const,
  start_date: "2026-07-01",
  end_date: "2026-07-02",
  month: null,
  column_profile: "full" as const,
  effective_columns: ["BilledCost", "Tags"],
  status: "queued" as const,
  created_at: "2026-07-03T00:00:00Z",
  started_at: null,
  completed_at: null,
  diagnostic: null,
  source_snapshot: null,
  package: null,
};

async function submitForm(): Promise<void> {
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: /generate preview/i }));
}

describe("FOCUS Mapping Preview page delegation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(fetchFocusPreviewProfile).mockResolvedValue({
      mapping_profile_version: "focus-1.4-preview-v5",
      full_columns: ["BilledCost", "Tags", "AllocatedResourceId"],
      summary_columns: ["AllocatedResourceId", "BilledCost", "Tags"],
    });
    vi.mocked(submitFocusPreview).mockResolvedValue(baseRequest);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    ["2026-08-01T00:30:00.000Z", "2026-08"],
    ["2026-12-15T23:30:00.000Z", "2026-12"],
  ])(
    "defaults to Monthly and the current UTC month at %s",
    (now, expectedMonth) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date(now));

      render(<FocusPreviewPage />);

      expect((screen.getByLabelText(/^grain$/i) as HTMLSelectElement).value).toBe("monthly");
      expect((screen.getByLabelText(/month/i) as HTMLInputElement).value).toBe(expectedMonth);
    },
  );

  it("shows both grains and all column profiles while retaining non-conformance gaps", async () => {
    render(<FocusPreviewPage />);

    expect(screen.getByRole("heading", { name: "FOCUS Mapping Preview" })).toBeTruthy();
    expect(screen.getByRole("option", { name: "Monthly" })).toBeTruthy();
    expect(screen.getByRole("option", { name: "Daily" })).toBeTruthy();
    expect(screen.getByRole("option", { name: "Full" })).toBeTruthy();
    expect(screen.getByRole("option", { name: "Summary" })).toBeTruthy();
    expect(screen.getByRole("option", { name: "Custom" })).toBeTruthy();
    await waitFor(() => expect(fetchFocusPreviewProfile).toHaveBeenCalledWith("production"));
    expect(screen.getByText(/non-conforming/i)).toBeTruthy();
    for (const description of [
      "Confluent Costs records do not carry a per-record billing currency.",
      "Post-issuance invoice identity is unavailable.",
      "Provider legal invoice-issuer evidence is unavailable.",
      "HostProviderName contains the raw provider cloud code, not a provider display name.",
      "Confluent inventory does not provide a distinct region display name.",
      "SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers.",
    ]) {
      expect(screen.getByText(description)).toBeTruthy();
    }
    expect(screen.queryByText("allocation_lineage_and_tag_projection_pending")).toBeNull();
    expect(screen.queryByText("allocation_ratio_deferred")).toBeNull();
    expect(screen.queryByText("allocation_method_version_deferred")).toBeNull();
    expect(screen.queryByText("Billing account and issuer mapping is pending.")).toBeNull();
    expect(screen.queryByText("Authoritative provider billing-period mapping is pending.")).toBeNull();
    expect(screen.queryByText("Provider applicability and mapping are pending.")).toBeNull();
    expect(screen.queryByText(/billing period evidence is not yet available/i)).toBeNull();
  });

  it("submits, polls, and downloads only through the API module", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
      ...baseRequest,
      status: "ready",
      package: {
        manifest: {
          name: "manifest.json",
          media_type: "application/json",
          size_bytes: 3,
          sha256: "a".repeat(64),
          download_url: "/api/v1/manifest",
        },
        files: [
          {
            name: "cost-and-usage.csv",
            media_type: "text/csv",
            size_bytes: 4,
            sha256: "b".repeat(64),
            order: 1,
            download_url: "/api/v1/cost-and-usage.csv",
          },
        ],
      },
    });
    vi.mocked(fetchPreviewArtifact).mockResolvedValue(new Blob(["bytes"]));
    render(<FocusPreviewPage />);
    const expectedMonth = (screen.getByLabelText(/month/i) as HTMLInputElement).value;

    await submitForm();
    await waitFor(() => expect(fetchFocusPreviewStatus).toHaveBeenCalled());
    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: /download manifest/i }),
    );
    await user.click(
      screen.getByRole("button", { name: /download cost and usage/i }),
    );

    expect(submitFocusPreview).toHaveBeenCalledWith("production", {
      grain: "monthly",
      month: expectedMonth,
      column_profile: "full",
    });
    expect(fetchFocusPreviewStatus).toHaveBeenCalledWith(
      "production",
      "request-1",
      expect.any(AbortSignal),
    );
    expect(fetchPreviewArtifact).toHaveBeenNthCalledWith(
      1,
      "/api/v1/manifest",
    );
    expect(fetchPreviewArtifact).toHaveBeenNthCalledWith(
      2,
      "/api/v1/cost-and-usage.csv",
    );
  });

  it("submits Daily Custom bounds and columns in caller selection order", async () => {
    render(<FocusPreviewPage />);
    const user = userEvent.setup();

    await waitFor(() => expect(fetchFocusPreviewProfile).toHaveBeenCalledWith("production"));
    await user.selectOptions(screen.getByLabelText(/^grain$/i), "daily");

    const startDate = screen.getByLabelText(/start date/i);
    await user.clear(startDate);
    await user.type(startDate, "2026-07-05");
    await user.tab();
    const endDate = screen.getByLabelText(/end date/i);
    await user.clear(endDate);
    await user.type(endDate, "2026-07-12");
    await user.tab();

    await user.selectOptions(screen.getByLabelText(/column profile/i), "custom");
    const columns = await screen.findByLabelText(/custom columns/i);
    await user.selectOptions(columns, "Tags");
    await user.selectOptions(columns, "BilledCost");
    await user.click(screen.getByRole("button", { name: /generate preview/i }));

    expect(submitFocusPreview).toHaveBeenCalledWith("production", {
      grain: "daily",
      start_date: "2026-07-05",
      end_date: "2026-07-12",
      column_profile: "custom",
      columns: ["Tags", "BilledCost"],
    });
  });

  it("renders provisional Monthly evidence coverage from persisted status", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
      ...baseRequest,
      grain: "monthly",
      start_date: "2026-07-01",
      end_date: "2026-08-01",
      month: "2026-07",
      status: "ready",
      source_snapshot: {
        calculation_timestamp: "2026-07-15T02:00:00Z",
        calculation_coverage: [],
        source_through: "2026-07-15T00:00:00Z",
        effective_coverage_start_date: "2026-07-01",
        effective_coverage_end_date: "2026-07-15",
        evidence_through_date: "2026-07-14",
        availability_cutoff_end_date: "2026-07-15",
        monthly_status: "provisional",
      },
      package: null,
    });
    render(<FocusPreviewPage />);

    await submitForm();

    expect(await screen.findByText(/provisional/i)).toBeTruthy();
    expect(screen.getByText(/2026-07-14/)).toBeTruthy();
  });

  it("renders a submit rejection and restores the submit control", async () => {
    vi.mocked(submitFocusPreview).mockRejectedValue(new Error("submit unavailable"));
    render(<FocusPreviewPage />);

    await submitForm();

    expect(await screen.findByText("FOCUS Mapping Preview request failed. Try again.")).toBeTruthy();
    expect(screen.queryByText("submit unavailable")).toBeNull();
    expect(screen.getByRole("button", { name: /generate preview/i })).not.toBeDisabled();
  });

  it("renders a poll rejection without resubmitting", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockRejectedValue(new Error("poll unavailable"));
    render(<FocusPreviewPage />);

    await submitForm();

    expect(await screen.findByText("FOCUS Mapping Preview request failed. Try again.")).toBeTruthy();
    expect(screen.queryByText("poll unavailable")).toBeNull();
    expect(submitFocusPreview).toHaveBeenCalledTimes(1);
  });

  it("suppresses AbortError from a cancelled poll", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockRejectedValue(new DOMException("poll aborted", "AbortError"));
    render(<FocusPreviewPage />);

    await submitForm();

    await waitFor(() =>
      expect(screen.getByRole("button", { name: /generate preview/i })).not.toBeDisabled(),
    );
    expect(screen.queryByText("poll aborted")).toBeNull();
  });

  it("renders a download rejection without navigating", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
      ...baseRequest,
      status: "ready",
      package: {
        manifest: {
          name: "manifest.json",
          media_type: "application/json",
          size_bytes: 3,
          sha256: "a".repeat(64),
          download_url: "/api/v1/manifest",
        },
        files: [],
      },
    });
    vi.mocked(fetchPreviewArtifact).mockRejectedValue(new Error("download unavailable"));
    render(<FocusPreviewPage />);
    await submitForm();

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /download manifest/i }));

    expect(await screen.findByText("FOCUS Mapping Preview download failed. Try again.")).toBeTruthy();
    expect(screen.queryByText("download unavailable")).toBeNull();
  });

  it.each([
    [
      "calculation_metadata_unavailable",
      "One or more requested dates lack preview calculation metadata.",
      false,
    ],
    [
      "calculation_unavailable",
      "No successful persisted calculation is available for the requested dates; run the pipeline and retry.",
      true,
    ],
    [
      "calculation_coverage_incomplete",
      "No successful persisted calculation covers every requested date; run the pipeline and retry.",
      true,
    ],
  ])(
    "renders persisted diagnostic %s and retryability without resubmitting",
    async (code, message, retryable) => {
      vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
        ...baseRequest,
        status: "failed",
        diagnostic: { code, message, retryable },
      });
      render(<FocusPreviewPage />);

      await submitForm();

      expect(await screen.findByText(code)).toBeTruthy();
      expect(screen.getByText(message)).toBeTruthy();
      expect(
        screen.getByText(retryable ? /retryable: yes/i : /retryable: no/i),
      ).toBeTruthy();
      expect(submitFocusPreview).toHaveBeenCalledTimes(1);
      expect(fetchPreviewArtifact).not.toHaveBeenCalled();
      if (!retryable) {
        expect(screen.queryByText(/repair|backfill|edit/i)).toBeNull();
      }
    },
  );

  it("renders safe source correlation identifiers from the persisted diagnostic", async () => {
    const correlations = [
      `src:v1:${"a".repeat(64)}`,
      `src:v1:${"b".repeat(64)}`,
    ];
    vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
      ...baseRequest,
      status: "failed",
      diagnostic: {
        code: "preview_source_record_malformed",
        message: "One or more persisted Confluent Costs API records are malformed.",
        retryable: false,
        source_correlation_ids: correlations,
      },
    });

    render(<FocusPreviewPage />);
    await submitForm();

    expect(await screen.findByText(correlations[0])).toBeTruthy();
    expect(screen.getByText(correlations[1])).toBeTruthy();
    expect(screen.queryByText(/provider payload|storage path|credential/i)).toBeNull();
  });
});
