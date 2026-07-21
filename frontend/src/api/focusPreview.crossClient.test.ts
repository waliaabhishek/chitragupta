import { afterEach, describe, expect, it, vi } from "vitest";

interface CrossClientFixture {
  status: Record<string, unknown>;
  bodies: Record<string, string>;
}

const encodedFixture = import.meta.env
  .VITE_FOCUS_PREVIEW_CROSS_CLIENT_FIXTURE as string | undefined;
const acceptance = encodedFixture ? describe : describe.skip;

async function sha256(bytes: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
}

acceptance("FOCUS Preview real-package cross-client fixture", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("retrieves the captured package through the production API adapter without changing bytes", async () => {
    const fixture = JSON.parse(atob(encodedFixture as string)) as CrossClientFixture;
    const status = fixture.status as {
      request_id: string;
      package: {
        manifest: { download_url: string; sha256: string };
        files: Array<{ download_url: string; sha256: string }>;
        download_all_url: string;
      };
    };
    const statusPath = `/api/v1/tenants/production/focus-preview/requests/${status.request_id}`;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), "http://localhost");
        if (url.pathname === statusPath) {
          return new Response(JSON.stringify(fixture.status), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        const encoded = fixture.bodies[url.pathname];
        if (encoded === undefined) {
          return new Response("missing", { status: 404 });
        }
        return new Response(Uint8Array.from(atob(encoded), (value) => value.charCodeAt(0)), {
          status: 200,
        });
      }),
    );
    const adapter = await import("./focusPreview");

    const fetched = await adapter.fetchFocusPreviewStatus(
      "production",
      status.request_id,
    );
    expect(fetched).toEqual(fixture.status);
    for (const artifact of [
      fetched.package!.manifest,
      ...fetched.package!.files,
    ]) {
      const blob = await adapter.fetchPreviewArtifact(artifact.download_url);
      expect(await sha256(await blob.arrayBuffer())).toBe(artifact.sha256);
    }
    const archive = await adapter.fetchPreviewArtifact(
      fetched.package!.download_all_url,
    );
    expect(archive.size).toBeGreaterThan(0);
  });
});
