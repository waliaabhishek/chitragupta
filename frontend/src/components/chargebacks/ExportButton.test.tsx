import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { notification } from "antd";
import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { ExportButton } from "./ExportButton";

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

describe("ExportButton", () => {
  it("renders Export CSV button", () => {
    render(<ExportButton tenantName="acme" filters={{}} />);
    expect(screen.getByText("Export CSV")).toBeTruthy();
  });

  it("triggers download on click and revokes URL", async () => {
    const appendSpy = vi.spyOn(document.body, "appendChild");
    const removeSpy = vi.spyOn(document.body, "removeChild");

    render(<ExportButton tenantName="acme" filters={{ start_date: "2024-01-01" }} />);
    fireEvent.click(screen.getByText("Export CSV"));

    await waitFor(() => {
      expect(appendSpy).toHaveBeenCalled();
      expect(removeSpy).toHaveBeenCalled();
      expect(revokeObjectURL).toHaveBeenCalledWith("blob:mock-url");
    });
  });

  it("calls notification.error on export failure", async () => {
    server.use(
      http.post("/api/v1/tenants/acme/export", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    const errorSpy = vi.spyOn(notification, "error").mockImplementation(vi.fn());

    render(<ExportButton tenantName="acme" filters={{}} />);
    fireEvent.click(screen.getByText("Export CSV"));

    await waitFor(() => {
      expect(errorSpy).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Export failed" }),
      );
    });

    errorSpy.mockRestore();
  });
});
