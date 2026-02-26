import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { notification } from "antd";
import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { BulkTagModal } from "./BulkTagModal";

describe("BulkTagModal", () => {
  const baseProps = {
    tenantName: "acme",
    onClose: vi.fn(),
    onSuccess: vi.fn(),
  };

  it("renders modal with by-IDs title", () => {
    render(
      <BulkTagModal
        {...baseProps}
        selectedIds={[1, 2, 3]}
        filters={null}
        totalCount={3}
      />,
    );
    expect(screen.getByText("Add Tags to 3 Selected Rows")).toBeTruthy();
  });

  it("renders modal with by-filter title", () => {
    render(
      <BulkTagModal
        {...baseProps}
        selectedIds={null}
        filters={{ identity_id: "user@example.com" }}
        totalCount={42}
      />,
    );
    expect(screen.getByText("Tag All 42 Filtered Rows")).toBeTruthy();
  });

  it("shows singular row label for count=1", () => {
    render(
      <BulkTagModal
        {...baseProps}
        selectedIds={[5]}
        filters={null}
        totalCount={1}
      />,
    );
    expect(screen.getByText("Add Tags to 1 Selected Row")).toBeTruthy();
  });

  it("calls onClose when modal X close button clicked", () => {
    const onClose = vi.fn();
    render(
      <BulkTagModal
        {...baseProps}
        onClose={onClose}
        selectedIds={[1]}
        filters={null}
        totalCount={1}
      />,
    );
    // Ant Design Modal renders a close button with aria-label="Close"
    const closeBtn = document.querySelector(".ant-modal-close");
    expect(closeBtn).toBeTruthy();
    fireEvent.click(closeBtn!);
    expect(onClose).toHaveBeenCalledOnce();
  });

  it("submits bulk-by-ids and calls onSuccess", async () => {
    const onSuccess = vi.fn();
    render(
      <BulkTagModal
        {...baseProps}
        onSuccess={onSuccess}
        selectedIds={[1, 2]}
        filters={null}
        totalCount={2}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("e.g. cost_center"), {
      target: { value: "cost_center" },
    });
    fireEvent.change(screen.getByPlaceholderText("e.g. Engineering"), {
      target: { value: "Engineering" },
    });

    fireEvent.click(screen.getByText("Apply Tags"));

    await waitFor(() => {
      expect(onSuccess).toHaveBeenCalledOnce();
    });
  });

  it("submits bulk-by-filter and calls onSuccess", async () => {
    const onSuccess = vi.fn();
    render(
      <BulkTagModal
        {...baseProps}
        onSuccess={onSuccess}
        selectedIds={null}
        filters={{ product_type: "KAFKA_NUM_BYTES" }}
        totalCount={10}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("e.g. cost_center"), {
      target: { value: "dept" },
    });
    fireEvent.change(screen.getByPlaceholderText("e.g. Engineering"), {
      target: { value: "Finance" },
    });

    fireEvent.click(screen.getByText("Apply Tags"));

    await waitFor(() => {
      expect(onSuccess).toHaveBeenCalledOnce();
    });
  });

  it("calls notification.error on API failure", async () => {
    server.use(
      http.post("/api/v1/tenants/acme/tags/bulk", () => {
        return new HttpResponse("Internal Server Error", { status: 500 });
      }),
    );

    const errorSpy = vi.spyOn(notification, "error").mockImplementation(vi.fn());

    render(
      <BulkTagModal
        {...baseProps}
        selectedIds={[1]}
        filters={null}
        totalCount={1}
      />,
    );

    fireEvent.change(screen.getByPlaceholderText("e.g. cost_center"), {
      target: { value: "env" },
    });
    fireEvent.change(screen.getByPlaceholderText("e.g. Engineering"), {
      target: { value: "Prod" },
    });

    fireEvent.click(screen.getByText("Apply Tags"));

    await waitFor(() => {
      expect(errorSpy).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to apply tags" }),
      );
    });

    errorSpy.mockRestore();
  });
});
