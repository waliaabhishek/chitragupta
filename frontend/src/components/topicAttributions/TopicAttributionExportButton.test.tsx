import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { notification } from "antd";
import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TopicAttributionExportButton } from "./TopicAttributionExportButton";

// jsdom doesn't support createObjectURL — stub it.
const revokeObjectURL = vi.fn();
Object.defineProperty(URL, "createObjectURL", {
  writable: true,
  value: vi.fn(() => "blob:mock-url"),
});
Object.defineProperty(URL, "revokeObjectURL", {
  writable: true,
  value: revokeObjectURL,
});

describe("TopicAttributionExportButton", () => {
  it("renders Export CSV button", () => {
    render(<TopicAttributionExportButton tenantName="acme" filters={{}} />);
    expect(screen.getByText("Export CSV")).toBeTruthy();
  });

  it("triggers download on click and revokes URL", async () => {
    const appendSpy = vi.spyOn(document.body, "appendChild");
    const removeSpy = vi.spyOn(document.body, "removeChild");

    server.use(
      http.post(
        "/api/v1/tenants/acme/topic-attributions/export",
        () =>
          new HttpResponse("date,amount\n2026-01-01,10.00\n", {
            headers: { "Content-Type": "text/csv" },
          }),
      ),
    );

    render(
      <TopicAttributionExportButton
        tenantName="acme"
        filters={{ start_date: "2026-01-01" }}
      />,
    );
    fireEvent.click(screen.getByText("Export CSV"));

    await waitFor(() => {
      expect(appendSpy).toHaveBeenCalled();
      expect(removeSpy).toHaveBeenCalled();
      expect(revokeObjectURL).toHaveBeenCalledWith("blob:mock-url");
    });
  });

  it("calls notification.error on export failure", async () => {
    server.use(
      http.post(
        "/api/v1/tenants/acme/topic-attributions/export",
        () => new HttpResponse(null, { status: 500 }),
      ),
    );

    const errorSpy = vi
      .spyOn(notification, "error")
      .mockImplementation(vi.fn());

    render(<TopicAttributionExportButton tenantName="acme" filters={{}} />);
    fireEvent.click(screen.getByText("Export CSV"));

    await waitFor(() => {
      expect(errorSpy).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Export failed" }),
      );
    });

    errorSpy.mockRestore();
  });

  it("is disabled when disabled prop is true", () => {
    render(
      <TopicAttributionExportButton tenantName="acme" filters={{}} disabled />,
    );
    const btn = screen.getByText("Export CSV").closest("button");
    expect(btn).toBeDefined();
    expect(btn?.disabled).toBe(true);
  });

  it("uses POST method with query params on URL — no Content-Type header, no request body", async () => {
    let capturedMethod = "";
    let capturedUrl = "";
    let capturedContentType: string | null = null;
    let capturedBody = "";

    server.use(
      http.post(
        "/api/v1/tenants/acme/topic-attributions/export",
        async ({ request }) => {
          capturedMethod = request.method;
          capturedUrl = request.url;
          capturedContentType = request.headers.get("content-type");
          capturedBody = await request.text();
          return new HttpResponse("date,amount\n", {
            headers: { "Content-Type": "text/csv" },
          });
        },
      ),
    );

    render(
      <TopicAttributionExportButton
        tenantName="acme"
        filters={{
          start_date: "2026-01-01",
          end_date: "2026-01-31",
          timezone: "America/Chicago",
        }}
      />,
    );
    fireEvent.click(screen.getByText("Export CSV"));

    await waitFor(() => {
      expect(capturedMethod).toBe("POST");
      expect(capturedUrl).toContain("start_date=2026-01-01");
      expect(capturedUrl).toContain("end_date=2026-01-31");
      expect(capturedUrl).toContain("timezone=America%2FChicago");
      // No Content-Type header (no JSON body)
      expect(capturedContentType).toBeNull();
      // No request body
      expect(capturedBody).toBe("");
    });
  });
});
