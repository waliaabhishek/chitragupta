import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FocusPreviewPage } from ".";
import {
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
  fetchFocusPreviewStatus: vi.fn(),
  fetchPreviewArtifact: vi.fn(),
}));

const baseRequest = {
  request_id: "request-1",
  tenant_name: "production",
  grain: "daily" as const,
  start_date: "2026-07-01",
  end_date: "2026-07-02",
  column_profile: "full" as const,
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
    vi.mocked(submitFocusPreview).mockResolvedValue(baseRequest);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    ["2026-08-01T00:30:00.000Z", "2026-08-01", "2026-09-01"],
    ["2026-12-15T23:30:00.000Z", "2026-12-01", "2027-01-01"],
  ])(
    "defaults to the full current UTC month at %s",
    (now, expectedStart, expectedEnd) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date(now));

      render(<FocusPreviewPage />);

      expect((screen.getByLabelText(/start date/i) as HTMLInputElement).value).toBe(expectedStart);
      expect((screen.getByLabelText(/end date/i) as HTMLInputElement).value).toBe(expectedEnd);
    },
  );

  it("shows fixed Daily Full scope, non-conformance, and current authority gaps before submit", () => {
    render(<FocusPreviewPage />);

    expect(screen.getByRole("heading", { name: "FOCUS Mapping Preview" })).toBeTruthy();
    expect(screen.getByText("Daily")).toBeTruthy();
    expect(screen.getByText("Full")).toBeTruthy();
    expect(screen.getByText(/non-conforming/i)).toBeTruthy();
    for (const description of [
      "Billing account and issuer mapping is pending.",
      "Authoritative provider billing-period mapping is pending.",
      "Commercial arrangement and authoritative billing currency are unavailable.",
      "Provider-authoritative SKU identity is unavailable.",
      "Post-issuance invoice identity is unavailable.",
      "Allocation lineage and tag projection are pending.",
      "Provider applicability and mapping are pending.",
    ]) {
      expect(screen.getByText(description)).toBeTruthy();
    }
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
    const expectedStart = (screen.getByLabelText(/start date/i) as HTMLInputElement).value;
    const expectedEnd = (screen.getByLabelText(/end date/i) as HTMLInputElement).value;

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
      grain: "daily",
      start_date: expectedStart,
      end_date: expectedEnd,
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
});
