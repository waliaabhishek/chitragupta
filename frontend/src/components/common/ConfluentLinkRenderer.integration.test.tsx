import type React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TenantProvider } from "../../providers/TenantContext";
import { ResourceLinkProvider } from "../../providers/ResourceLinkContext";
import { ConfluentLinkRenderer } from "./ConfluentLinkRenderer";

// ---------------------------------------------------------------------------
// Integration test — real ResourceLinkProvider (no mocks)
// Verifies: Provider → fetch → index → resolveUrl → renderer → link
// ---------------------------------------------------------------------------

function Wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return (
    <TenantProvider>
      <ResourceLinkProvider>{children}</ResourceLinkProvider>
    </TenantProvider>
  );
}

beforeEach(() => {
  localStorage.setItem("chargeback_deep_links_enabled", "true");
  server.use(
    http.get("/api/v1/tenants/acme/resources", () =>
      HttpResponse.json({
        items: [
          {
            ecosystem: "ccloud",
            tenant_id: "t-001",
            resource_id: "env-abc123",
            resource_type: "environment",
            display_name: null,
            parent_id: null,
            owner_id: null,
            status: "active",
            created_at: null,
            deleted_at: null,
            last_seen_at: null,
            metadata: {},
          },
          {
            ecosystem: "ccloud",
            tenant_id: "t-001",
            resource_id: "lkc-def456",
            resource_type: "kafka_cluster",
            display_name: null,
            parent_id: "env-abc123",
            owner_id: null,
            status: "active",
            created_at: null,
            deleted_at: null,
            last_seen_at: null,
            metadata: {},
          },
        ],
        total: 2,
        page: 1,
        page_size: 100,
        pages: 1,
      }),
    ),
  );
});

afterEach(() => {
  localStorage.clear();
});

describe("ConfluentLinkRenderer — integration with real ResourceLinkProvider", () => {
  it("renders link with correct href after index loads from real provider", async () => {
    // lkc- has no prefix fallback — link appears only after index fetch completes
    render(
      <Wrapper>
        <ConfluentLinkRenderer value="lkc-def456" />
      </Wrapper>,
    );

    await waitFor(() => {
      const link = screen.queryByRole("link");
      expect(link).toBeTruthy();
    });

    const link = screen.getByRole("link");
    expect(link.getAttribute("href")).toBe(
      "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456",
    );
    expect(link.textContent).toBe("lkc-def456");
  });
});
