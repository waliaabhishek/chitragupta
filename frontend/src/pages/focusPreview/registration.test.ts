import { describe, expect, it } from "vitest";
import appSource from "../../App.tsx?raw";
import layoutSource from "../../components/Layout.tsx?raw";
import pageSource from "./index.tsx?raw";

describe("FOCUS Mapping Preview registration", () => {
  it("registers the page route in App", () => {
    expect(appSource).toContain('path="/focus-preview"');
    expect(appSource).toContain("<FocusPreviewPage />");
  });

  it("registers tenant-scoped navigation with the product name", () => {
    expect(layoutSource).toContain('key: "/focus-preview"');
    expect(layoutSource).toContain('label: "FOCUS Mapping Preview"');
    expect(layoutSource).toContain("disabled: tenantRequired");
  });

  it("keeps the page free of mapping, CSV, checksum, and server-path construction", () => {
    expect(pageSource).not.toContain("core.preview");
    expect(pageSource).not.toContain("csv");
    expect(pageSource).not.toContain("sha256(");
    expect(pageSource).not.toContain("storage_key");
    expect(pageSource).not.toContain("artifact_root");
  });
});
