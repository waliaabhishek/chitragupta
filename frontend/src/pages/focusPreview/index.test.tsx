import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FocusPreviewPage } from ".";
import {
  fetchFocusPreviewProfile,
  fetchFocusPreviewRevision,
  fetchFocusPreviewStatus,
  fetchPreviewArtifact,
  listFocusPreviewRequests,
  listFocusPreviewRevisions,
  submitFocusPreview,
} from "../../api/focusPreview";

const tenantState = vi.hoisted(() => ({
  current: { tenant_name: "production", tenant_id: "tenant-1" },
}));

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: tenantState.current,
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
  fetchFocusPreviewRevision: vi.fn(),
  fetchFocusPreviewStatus: vi.fn(),
  fetchPreviewArtifact: vi.fn(),
  listFocusPreviewRequests: vi.fn(),
  listFocusPreviewRevisions: vi.fn(),
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
  expires_at: null,
  diagnostic: null,
  source_snapshot: null,
  package: null,
};

const currentRevision = {
  revision_id: "revision-current",
  tenant_name: "production",
  month: "2026-07",
  start_date: "2026-07-01",
  end_date: "2026-08-01",
  lifecycle: "current" as const,
  monthly_status: "provisional" as const,
  published_at: "2026-07-21T12:00:00Z",
  supersedes_revision_id: "revision-old",
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
    monthly_status: "provisional" as const,
  },
  validation: {
    status: "passed" as const,
    mapping_profile_version: "focus-1.4-preview-v5",
    source_records: 12,
    rows: 10,
    mapping_errors: 0 as const,
    artifact_integrity: "passed" as const,
  },
  replacement_semantics: "complete_replacement" as const,
  consumer_action: "replace_do_not_aggregate" as const,
  detail_url:
    "/api/v1/tenants/production/focus-preview/revisions/revision-current",
};

const supersededRevision = {
  ...currentRevision,
  revision_id: "revision-old",
  lifecycle: "superseded" as const,
  monthly_status: "settled" as const,
  published_at: "2026-07-20T12:00:00Z",
  supersedes_revision_id: null,
  superseded_by_revision_id: "revision-current",
  source_snapshot: {
    ...currentRevision.source_snapshot,
    monthly_status: "settled" as const,
  },
  detail_url:
    "/api/v1/tenants/production/focus-preview/revisions/revision-old",
};

const currentRevisionDetail = {
  ...currentRevision,
  self_url: currentRevision.detail_url,
  package: {
    manifest: {
      name: "manifest.json",
      media_type: "application/json",
      size_bytes: 3,
      sha256: "a".repeat(64),
      download_url: `${currentRevision.detail_url}/manifest`,
    },
    files: [
      {
        name: "cost-and-usage.csv",
        media_type: "text/csv",
        size_bytes: 4,
        sha256: "b".repeat(64),
        order: 1,
        download_url: `${currentRevision.detail_url}/files/cost-and-usage.csv`,
      },
    ],
    download_all_name: "focus-mapping-preview-revision-current.zip",
    download_all_url: `${currentRevision.detail_url}/archive`,
  },
};

async function submitForm(): Promise<void> {
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: /generate preview/i }));
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

describe("FOCUS Mapping Preview page delegation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    tenantState.current = { tenant_name: "production", tenant_id: "tenant-1" };
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [],
      next_cursor: null,
    });
    vi.mocked(listFocusPreviewRevisions).mockResolvedValue({
      items: [],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });
    vi.mocked(fetchFocusPreviewRevision).mockResolvedValue(currentRevisionDetail);
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
    await waitFor(() => expect(fetchFocusPreviewProfile).toHaveBeenCalledWith(
      "production",
      expect.any(AbortSignal),
    ));
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
      completed_at: "2026-07-03T00:01:00Z",
      expires_at: "2026-07-10T00:01:00Z",
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
        download_all_name: "focus-mapping-preview-request-1.zip",
        download_all_url: "/api/v1/archive",
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

    expect(submitFocusPreview).toHaveBeenCalledWith(
      "production",
      {
        grain: "monthly",
        month: expectedMonth,
        column_profile: "full",
      },
      expect.any(AbortSignal),
    );
    expect(fetchFocusPreviewStatus).toHaveBeenCalledWith(
      "production",
      "request-1",
      expect.any(AbortSignal),
    );
    expect(fetchPreviewArtifact).toHaveBeenNthCalledWith(
      1,
      "/api/v1/manifest",
      expect.any(AbortSignal),
    );
    expect(fetchPreviewArtifact).toHaveBeenNthCalledWith(
      2,
      "/api/v1/cost-and-usage.csv",
      expect.any(AbortSignal),
    );
    await waitFor(() => expect(listFocusPreviewRequests).toHaveBeenCalledTimes(2));
  });

  it("submits Daily Custom bounds and columns in caller selection order", async () => {
    render(<FocusPreviewPage />);
    const user = userEvent.setup();

    await waitFor(() => expect(fetchFocusPreviewProfile).toHaveBeenCalledWith(
      "production",
      expect.any(AbortSignal),
    ));
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

    expect(submitFocusPreview).toHaveBeenCalledWith(
      "production",
      {
        grain: "daily",
        start_date: "2026-07-05",
        end_date: "2026-07-12",
        column_profile: "custom",
        columns: ["Tags", "BilledCost"],
      },
      expect.any(AbortSignal),
    );
  });

  it("renders provisional Monthly evidence coverage from persisted status", async () => {
    vi.mocked(fetchFocusPreviewStatus).mockResolvedValue({
      ...baseRequest,
      grain: "monthly",
      start_date: "2026-07-01",
      end_date: "2026-08-01",
      month: "2026-07",
      status: "ready",
      completed_at: "2026-07-03T00:01:00Z",
      expires_at: "2026-07-10T00:01:00Z",
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
        download_all_name: "focus-mapping-preview-request-1.zip",
        download_all_url: "/api/v1/archive",
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

  it("loads recent requests in API order and follows the Load more cursor", async () => {
    vi.mocked(listFocusPreviewRequests)
      .mockResolvedValueOnce({
        items: [
          { ...baseRequest, request_id: "request-3", created_at: "2026-07-03T03:00:00Z" },
          { ...baseRequest, request_id: "request-2", created_at: "2026-07-03T02:00:00Z" },
        ],
        next_cursor: "request-2",
      })
      .mockResolvedValueOnce({
        items: [
          { ...baseRequest, request_id: "request-1", created_at: "2026-07-03T01:00:00Z" },
        ],
        next_cursor: null,
      });
    render(<FocusPreviewPage />);

    const newest = await screen.findByText("request-3");
    const middle = screen.getByText("request-2");
    expect(newest.compareDocumentPosition(middle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    await userEvent.click(screen.getByRole("button", { name: /load more/i }));

    expect(await screen.findByText("request-1")).toBeTruthy();
    expect(listFocusPreviewRequests).toHaveBeenNthCalledWith(1, "production", {
      signal: expect.any(AbortSignal),
    });
    expect(listFocusPreviewRequests).toHaveBeenNthCalledWith(2, "production", {
      cursor: "request-2",
      signal: expect.any(AbortSignal),
    });
    expect(screen.queryByRole("button", { name: /load more/i })).toBeNull();
  });

  it("loads monthly revision history and renders replacement, lifecycle, freshness, and validation evidence", async () => {
    vi.mocked(listFocusPreviewRevisions).mockResolvedValue({
      items: [currentRevision, supersededRevision],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });

    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByRole("heading", { name: /published monthly revisions/i })).toBeTruthy();
    expect(listFocusPreviewRevisions).toHaveBeenCalledWith("production", {
      month: "2026-07",
      signal: expect.any(AbortSignal),
    });
    expect(screen.getByText("revision-current")).toBeTruthy();
    expect(screen.getByText("revision-old")).toBeTruthy();
    expect(screen.getByText(/each revision is a complete replacement.*do not aggregate revisions/i)).toBeTruthy();
    expect(screen.getByText(/lifecycle current/i)).toBeTruthy();
    expect(screen.getByText(/lifecycle superseded/i)).toBeTruthy();
    expect(screen.getByText(/monthly status provisional/i)).toBeTruthy();
    expect(screen.getByText(/monthly status settled/i)).toBeTruthy();
    expect(screen.getByText(/published.*2026-07-21T12:00:00Z/i)).toBeTruthy();
    expect(screen.getByText(/calculation.*2026-07-21T11:55:00Z/i)).toBeTruthy();
    expect(screen.getByText(/source through.*2026-07-21T11:50:00Z/i)).toBeTruthy();
    expect(screen.getByText(/supersedes.*revision-old/i)).toBeTruthy();
    expect(screen.getByText(/superseded by.*revision-current/i)).toBeTruthy();
    expect(screen.getAllByText(/validation passed/i)).toHaveLength(2);
    expect(screen.getAllByText(/mapping profile.*focus-1\.4-preview-v5/i)).toHaveLength(2);
    expect(screen.getAllByText(/source records.*12/i)).toHaveLength(2);
    expect(screen.getAllByText(/rows.*10/i)).toHaveLength(2);
    expect(screen.getAllByText(/artifact integrity passed/i)).toHaveLength(2);
  });

  it("renders settled request evidence through the persisted daily boundary", async () => {
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [{
        ...baseRequest,
        request_id: "request-settled",
        status: "ready",
        source_snapshot: {
          calculation_timestamp: "2026-08-02T02:00:00Z",
          calculation_coverage: [],
          source_through: "2026-08-01T00:00:00Z",
          effective_coverage_start_date: "2026-07-01",
          effective_coverage_end_date: "2026-08-01",
          evidence_through_date: "2026-07-31",
          availability_cutoff_end_date: "2026-08-01",
          monthly_status: "settled",
        },
      }],
      next_cursor: null,
    });

    render(<FocusPreviewPage />);

    expect(await screen.findByText(/monthly status: settled/i)).toBeTruthy();
    expect(screen.getByText(/evidence through 2026-07-31/i)).toBeTruthy();
  });

  it("paginates revision history with the returned cursor", async () => {
    vi.mocked(listFocusPreviewRevisions)
      .mockResolvedValueOnce({
        items: [currentRevision],
        next_cursor: "revision-current",
        replacement_semantics: "complete_replacement",
        consumer_action: "replace_do_not_aggregate",
      })
      .mockResolvedValueOnce({
        items: [supersededRevision],
        next_cursor: null,
        replacement_semantics: "complete_replacement",
        consumer_action: "replace_do_not_aggregate",
      });
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /load more revisions/i }));

    expect(await screen.findByText("revision-old")).toBeTruthy();
    expect(listFocusPreviewRevisions).toHaveBeenNthCalledWith(2, "production", {
      month: "2026-07",
      cursor: "revision-current",
      signal: expect.any(AbortSignal),
    });
    expect(screen.queryByRole("button", { name: /load more revisions/i })).toBeNull();
  });

  it("loads direct revision detail and downloads its manifest, file, and archive URLs", async () => {
    vi.mocked(listFocusPreviewRevisions).mockResolvedValue({
      items: [currentRevision],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });
    vi.mocked(fetchPreviewArtifact).mockResolvedValue(new Blob(["bytes"]));
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /view and download/i }));
    expect(fetchFocusPreviewRevision).toHaveBeenCalledWith(
      "production",
      "revision-current",
      expect.any(AbortSignal),
    );

    await userEvent.click(await screen.findByRole("button", { name: /download manifest\.json/i }));
    await userEvent.click(screen.getByRole("button", { name: /download cost-and-usage\.csv/i }));
    await userEvent.click(screen.getByRole("button", { name: /download all/i }));

    expect(vi.mocked(fetchPreviewArtifact).mock.calls.map(([url]) => url)).toEqual([
      `${currentRevision.detail_url}/manifest`,
      `${currentRevision.detail_url}/files/cost-and-usage.csv`,
      `${currentRevision.detail_url}/archive`,
    ]);
    for (const [, signal] of vi.mocked(fetchPreviewArtifact).mock.calls) {
      expect(signal).toBeInstanceOf(AbortSignal);
    }
  });

  it("shows a generic revision artifact failure without internal paths", async () => {
    vi.mocked(listFocusPreviewRevisions).mockResolvedValue({
      items: [currentRevision],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });
    vi.mocked(fetchPreviewArtifact).mockRejectedValueOnce(
      new Error("/srv/private/artifacts/revision-current/manifest.json"),
    );
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /view and download/i }));
    await userEvent.click(await screen.findByRole("button", { name: /download manifest\.json/i }));

    expect(await screen.findByText(/published.*revision.*failed.*try again/i)).toBeTruthy();
    expect(screen.queryByText(/\/srv\/private\/artifacts\/revision-current/i)).toBeNull();
  });

  it("aborts and clears revision history when the revision month changes", async () => {
    let oldHistoryAborted = false;
    const oldHistory = deferred<{
      items: Array<typeof currentRevision>;
      next_cursor: string | null;
      replacement_semantics: "complete_replacement";
      consumer_action: "replace_do_not_aggregate";
    }>();
    vi.mocked(listFocusPreviewRevisions).mockImplementation(
      (
        _tenant: string,
        options: { month: string; signal?: AbortSignal },
      ) => {
        if (options.month === "2026-07") {
          options.signal?.addEventListener("abort", () => {
            oldHistoryAborted = true;
          });
          return oldHistory.promise;
        }
        return Promise.resolve({
          items: [],
          next_cursor: null,
          replacement_semantics: "complete_replacement",
          consumer_action: "replace_do_not_aggregate",
        });
      },
    );
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    const revisionMonth = await screen.findByLabelText(/revision month/i);
    fireEvent.change(revisionMonth, { target: { value: "2026-06" } });

    await waitFor(() => expect(oldHistoryAborted).toBe(true));
    expect(listFocusPreviewRevisions).toHaveBeenLastCalledWith("production", {
      month: "2026-06",
      signal: expect.any(AbortSignal),
    });
    expect(screen.queryByText("revision-current")).toBeNull();
  });

  it("aborts and clears selected revision and artifact work on tenant change", async () => {
    let artifactAborted = false;
    vi.mocked(listFocusPreviewRevisions).mockImplementation(async (tenantName: string) => ({
      items: tenantName === "production" ? [currentRevision] : [],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    }));
    vi.mocked(fetchPreviewArtifact).mockImplementation(
      (_url, signal) => new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          artifactAborted = true;
          reject(new DOMException("artifact aborted", "AbortError"));
        });
      }),
    );
    const { rerender } = render(
      <FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />,
    );
    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /view and download/i }));
    await userEvent.click(await screen.findByRole("button", { name: /download manifest\.json/i }));

    tenantState.current = { tenant_name: "staging", tenant_id: "tenant-2" };
    rerender(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    await waitFor(() => expect(artifactAborted).toBe(true));
    expect(screen.queryByText("revision-current")).toBeNull();
    expect(listFocusPreviewRevisions).toHaveBeenLastCalledWith("staging", {
      month: "2026-07",
      signal: expect.any(AbortSignal),
    });
    expect(screen.queryByText("artifact aborted")).toBeNull();
  });

  it("removes no-longer-visible revisions after refresh", async () => {
    vi.mocked(listFocusPreviewRevisions)
      .mockResolvedValueOnce({
        items: [currentRevision],
        next_cursor: null,
        replacement_semantics: "complete_replacement",
        consumer_action: "replace_do_not_aggregate",
      })
      .mockResolvedValueOnce({
        items: [],
        next_cursor: null,
        replacement_semantics: "complete_replacement",
        consumer_action: "replace_do_not_aggregate",
      });
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /refresh revisions/i }));

    await waitFor(() => expect(screen.queryByText("revision-current")).toBeNull());
  });

  it("shows a generic history failure without internal paths", async () => {
    vi.mocked(listFocusPreviewRevisions).mockRejectedValueOnce(
      new Error("/srv/private/tenants/tenant-1/revisions.sqlite"),
    );
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText(/published.*revision.*failed.*try again/i)).toBeTruthy();
    expect(screen.queryByText(/\/srv\/private|tenant-1|revisions\.sqlite/i)).toBeNull();
  });

  it("shows a generic direct-detail failure without internal paths", async () => {
    vi.mocked(listFocusPreviewRevisions).mockResolvedValue({
      items: [currentRevision],
      next_cursor: null,
      replacement_semantics: "complete_replacement",
      consumer_action: "replace_do_not_aggregate",
    });
    vi.mocked(fetchFocusPreviewRevision).mockRejectedValueOnce(
      new Error("/srv/private/artifacts/revision-current/manifest.json"),
    );
    render(<FocusPreviewPage now={() => new Date("2026-07-21T20:00:00Z")} />);

    expect(await screen.findByText("revision-current")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /view and download/i }));

    expect(await screen.findByText(/published.*revision.*failed.*try again/i)).toBeTruthy();
    expect(screen.queryByText(/\/srv\/private|artifacts|manifest\.json/i)).toBeNull();
  });

  it("clears recent requests and aborts the old tenant poll when tenant changes", async () => {
    let oldPollAborted = false;
    vi.mocked(listFocusPreviewRequests).mockImplementation(async (tenantName: string) => ({
      items: [{
        ...baseRequest,
        tenant_name: tenantName,
        request_id: `${tenantName}-request`,
      }],
      next_cursor: null,
    }));
    vi.mocked(fetchFocusPreviewStatus).mockImplementation(
      (_tenant, _request, signal) => new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          oldPollAborted = true;
          reject(new DOMException("aborted", "AbortError"));
        });
      }),
    );
    const { rerender } = render(<FocusPreviewPage />);
    expect(await screen.findByText("production-request")).toBeTruthy();
    await submitForm();

    tenantState.current = { tenant_name: "staging", tenant_id: "tenant-2" };
    rerender(<FocusPreviewPage />);

    expect(await screen.findByText("staging-request")).toBeTruthy();
    expect(screen.queryByText("production-request")).toBeNull();
    expect(listFocusPreviewRequests).toHaveBeenLastCalledWith("staging", {
      signal: expect.any(AbortSignal),
    });
    expect(oldPollAborted).toBe(true);
  });

  it("ignores a deferred POST result after the tenant changes", async () => {
    const submitted = deferred<typeof baseRequest>();
    vi.mocked(submitFocusPreview).mockReturnValue(submitted.promise);
    vi.mocked(listFocusPreviewRequests).mockImplementation(async (tenantName: string) => ({
      items: [{
        ...baseRequest,
        tenant_name: tenantName,
        request_id: `${tenantName}-history`,
      }],
      next_cursor: null,
    }));
    const { rerender } = render(<FocusPreviewPage />);
    expect(await screen.findByText("production-history")).toBeTruthy();
    await submitForm();
    expect(submitFocusPreview).toHaveBeenCalledWith(
      "production",
      expect.any(Object),
      expect.any(AbortSignal),
    );

    tenantState.current = { tenant_name: "staging", tenant_id: "tenant-2" };
    rerender(<FocusPreviewPage />);
    expect(await screen.findByText("staging-history")).toBeTruthy();

    await act(async () => {
      submitted.resolve({ ...baseRequest, request_id: "late-production-request" });
      await submitted.promise;
    });

    await waitFor(() => expect(screen.queryByText("late-production-request")).toBeNull());
    expect(screen.getByText("staging-history")).toBeTruthy();
    expect(fetchFocusPreviewStatus).not.toHaveBeenCalled();
  });

  it("ignores a deferred Load more page after the tenant changes", async () => {
    const oldPage = deferred<{
      items: Array<typeof baseRequest>;
      next_cursor: string | null;
    }>();
    vi.mocked(listFocusPreviewRequests).mockImplementation(
      (tenantName: string, options?: { cursor?: string; signal?: AbortSignal }) => {
        if (tenantName === "production" && options?.cursor) return oldPage.promise;
        return Promise.resolve({
          items: [{
            ...baseRequest,
            tenant_name: tenantName,
            request_id: `${tenantName}-history`,
          }],
          next_cursor: tenantName === "production" ? "production-cursor" : null,
        });
      },
    );
    const { rerender } = render(<FocusPreviewPage />);
    expect(await screen.findByText("production-history")).toBeTruthy();
    await userEvent.click(screen.getByRole("button", { name: /load more/i }));

    tenantState.current = { tenant_name: "staging", tenant_id: "tenant-2" };
    rerender(<FocusPreviewPage />);
    expect(await screen.findByText("staging-history")).toBeTruthy();

    await act(async () => {
      oldPage.resolve({
        items: [{ ...baseRequest, request_id: "late-production-page" }],
        next_cursor: "stale-cursor",
      });
      await oldPage.promise;
    });

    await waitFor(() => expect(screen.queryByText("late-production-page")).toBeNull());
    expect(screen.getByText("staging-history")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /load more/i })).toBeNull();
  });

  it("shows persisted freshness and the exact ready expiry timestamp", async () => {
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [{
        ...baseRequest,
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
      }],
      next_cursor: null,
    });
    render(<FocusPreviewPage />);

    expect(await screen.findByText(/calculation.*2026-07-02T23:55:00Z/i)).toBeTruthy();
    expect(screen.getByText(/source through.*2026-07-02T23:50:00Z/i)).toBeTruthy();
    expect(screen.getByText("Expires 2026-07-10T00:01:00Z")).toBeTruthy();
  });

  it("downloads the manifest, every actual part name, and Download All from API URLs", async () => {
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [{
        ...baseRequest,
        status: "ready",
        completed_at: "2026-07-03T00:01:00Z",
        expires_at: "2026-07-10T00:01:00Z",
        package: {
          manifest: {
            name: "manifest.json",
            media_type: "application/json",
            size_bytes: 3,
            sha256: "a".repeat(64),
            download_url: "/api/v1/request-1/manifest",
          },
          files: [
            {
              name: "cost-and-usage-part-00001-of-00002.csv",
              media_type: "text/csv",
              size_bytes: 4,
              sha256: "b".repeat(64),
              order: 1,
              download_url: "/api/v1/request-1/files/part-1",
            },
            {
              name: "cost-and-usage-part-00002-of-00002.csv",
              media_type: "text/csv",
              size_bytes: 4,
              sha256: "c".repeat(64),
              order: 2,
              download_url: "/api/v1/request-1/files/part-2",
            },
          ],
          download_all_name: "focus-mapping-preview-request-1.zip",
          download_all_url: "/api/v1/request-1/archive",
        },
      }],
      next_cursor: null,
    });
    vi.mocked(fetchPreviewArtifact).mockResolvedValue(new Blob(["bytes"]));
    render(<FocusPreviewPage />);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("button", { name: /download manifest\.json/i }));
    await user.click(screen.getByRole("button", { name: /download cost-and-usage-part-00001-of-00002\.csv/i }));
    await user.click(screen.getByRole("button", { name: /download cost-and-usage-part-00002-of-00002\.csv/i }));
    await user.click(screen.getByRole("button", { name: /download all/i }));

    expect(vi.mocked(fetchPreviewArtifact).mock.calls.map(([url]) => url)).toEqual([
      "/api/v1/request-1/manifest",
      "/api/v1/request-1/files/part-1",
      "/api/v1/request-1/files/part-2",
      "/api/v1/request-1/archive",
    ]);
  });

  it("shows exact expiry without downloads for an expired recent request", async () => {
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [{
        ...baseRequest,
        status: "expired",
        completed_at: "2026-07-03T00:01:00Z",
        expires_at: "2026-07-10T00:01:00Z",
        package: null,
      }],
      next_cursor: null,
    });
    render(<FocusPreviewPage />);

    expect(await screen.findByText("Expired 2026-07-10T00:01:00Z")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /download/i })).toBeNull();
  });

  it("keeps the exact persisted diagnostic for a failed recent request", async () => {
    vi.mocked(listFocusPreviewRequests).mockResolvedValue({
      items: [{
        ...baseRequest,
        status: "failed",
        diagnostic: {
          code: "preview_csv_row_exceeds_file_size_limit",
          message: "A Preview CSV header or row exceeds the configured file-size limit.",
          retryable: false,
        },
      }],
      next_cursor: null,
    });
    render(<FocusPreviewPage />);

    expect(await screen.findByText("preview_csv_row_exceeds_file_size_limit")).toBeTruthy();
    expect(screen.getByText("A Preview CSV header or row exceeds the configured file-size limit.")).toBeTruthy();
    expect(screen.getByText(/retryable: no/i)).toBeTruthy();
  });
});
