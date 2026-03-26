// TASK-160.02 TDD red phase — EntityTagEditor does not exist yet; import will fail.
import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
// This import WILL fail — component does not exist yet (red state).
import { EntityTagEditor } from "./EntityTagEditor";
// Type imports are erased at runtime; they do not cause red state by themselves.
import type { EntityTagCreateRequest, EntityTagResponse } from "../../types/api";

// Mock antd — Tag with optional close button, Form, Input, Button, Space, Typography, Spin
vi.mock("antd", () => ({
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number }) => (
      <h5>{children}</h5>
    ),
    Text: ({ children }: { children: ReactNode; type?: string }) => (
      <span>{children}</span>
    ),
  },
  Spin: ({ size }: { size?: string }) => <div data-testid="spinner" data-size={size}>Loading</div>,
  Tag: ({
    children,
    closable,
    onClose,
  }: {
    children: ReactNode;
    closable?: boolean;
    onClose?: () => void;
  }) => (
    <span>
      {children}
      {closable && (
        <button className="ant-tag-close-icon" onClick={onClose} aria-label="remove-tag">
          x
        </button>
      )}
    </span>
  ),
  Form: Object.assign(
    ({
      children,
      onFinish,
    }: {
      children: ReactNode;
      layout?: string;
      onFinish?: (values: unknown) => void;
      form?: unknown;
    }) => {
      const ref = React.useRef<HTMLFormElement>(null);
      return (
        <form
          ref={ref}
          onSubmit={(e) => {
            e.preventDefault();
            if (!onFinish) return;
            const values: Record<string, string> = {};
            const items = ref.current?.querySelectorAll<HTMLElement>("[data-name]");
            items?.forEach((item) => {
              const name = item.getAttribute("data-name");
              const input = item.querySelector("input");
              if (name && input) values[name] = input.value;
            });
            onFinish(values);
          }}
        >
          {children}
        </form>
      );
    },
    {
      Item: ({
        children,
        name,
      }: {
        children: ReactNode;
        name?: string;
        rules?: unknown[];
        label?: string;
      }) => <div data-name={name}>{children}</div>,
      useForm: () => [
        {
          resetFields: vi.fn(),
          setFieldsValue: vi.fn(),
          getFieldValue: vi.fn(),
          validateFields: vi.fn().mockResolvedValue({ tag_key: "env", tag_value: "prod" }),
        },
      ],
    },
  ),
  Input: ({
    placeholder,
    onChange,
    maxLength,
    style,
  }: {
    placeholder?: string;
    value?: string;
    onChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
    allowClear?: boolean;
    maxLength?: number;
    style?: object;
  }) => (
    <input
      placeholder={placeholder}
      maxLength={maxLength}
      onChange={onChange}
      style={style}
    />
  ),
  Button: ({
    children,
    onClick,
    htmlType,
    loading,
  }: {
    children: ReactNode;
    onClick?: () => void;
    type?: string;
    htmlType?: "button" | "submit" | "reset";
    loading?: boolean;
  }) => (
    <button type={htmlType ?? "button"} onClick={onClick} disabled={loading}>
      {children}
    </button>
  ),
  Space: ({ children }: { children: ReactNode; wrap?: boolean; style?: object }) => (
    <span>{children}</span>
  ),
  notification: {
    error: vi.fn(),
  },
}));

const mockTenantBase = {
  tenant_name: "acme",
  tenant_id: "t-001",
  ecosystem: "ccloud",
  dates_pending: 0,
  dates_calculated: 10,
  last_calculated_date: null,
};

vi.mock("../../providers/TenantContext", () => ({
  useTenant: vi.fn(() => ({
    currentTenant: mockTenantBase,
    tenants: [mockTenantBase],
    setCurrentTenant: vi.fn(),
    isLoading: false,
    error: null,
    refetch: vi.fn(),
    isReadOnly: false,
  })),
}));

const entityTagFixture: EntityTagResponse = {
  tag_id: 1,
  tenant_id: "acme",
  entity_type: "resource",
  entity_id: "42",
  tag_key: "env",
  tag_value: "prod",
  created_by: "ui",
  created_at: null,
};

beforeEach(() => {
  server.use(
    http.get("/api/v1/tenants/acme/entities/resource/42/tags", () => {
      return HttpResponse.json([entityTagFixture]);
    }),
    http.post("/api/v1/tenants/acme/entities/resource/42/tags", async ({ request }) => {
      const body = (await request.json()) as EntityTagCreateRequest;
      return HttpResponse.json({
        ...body,
        entity_type: "resource",
        entity_id: "42",
        created_at: null,
      }, { status: 201 });
    }),
    http.delete("/api/v1/tenants/acme/entities/resource/42/tags/:key", () => {
      return new HttpResponse(null, { status: 204 });
    }),
  );
});

describe("EntityTagEditor", () => {
  it("EntityTagEditor_renders_existing_tags", async () => {
    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByText("env: prod")).toBeTruthy();
    });
  });

  it("EntityTagEditor_add_flow_posts_correct_payload", async () => {
    let capturedBody: EntityTagCreateRequest | undefined;

    server.use(
      http.post("/api/v1/tenants/acme/entities/resource/42/tags", async ({ request }) => {
        capturedBody = (await request.json()) as EntityTagCreateRequest;
        return HttpResponse.json({
          entity_type: "resource",
          entity_id: "42",
          tag_key: capturedBody.tag_key,
          tag_value: capturedBody.tag_value,
          created_by: capturedBody.created_by,
          created_at: null,
        }, { status: 201 });
      }),
    );

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByPlaceholderText("Key")).toBeTruthy();
    });

    fireEvent.change(screen.getByPlaceholderText("Key"), { target: { value: "team" } });
    fireEvent.change(screen.getByPlaceholderText("Value"), { target: { value: "platform" } });
    fireEvent.click(screen.getByText("Add"));

    await waitFor(() => {
      expect(capturedBody).toBeDefined();
      expect(capturedBody!.tag_key).toBe("team");
      expect(capturedBody!.tag_value).toBe("platform");
      expect(capturedBody!.created_by).toBe("ui");
    });
  });

  it("EntityTagEditor_remove_flow_calls_delete_and_refetches", async () => {
    let deletedKey: string | undefined;
    let refetchCount = 0;

    server.use(
      http.get("/api/v1/tenants/acme/entities/resource/42/tags", () => {
        refetchCount += 1;
        return HttpResponse.json(refetchCount === 1 ? [entityTagFixture] : []);
      }),
      http.delete("/api/v1/tenants/acme/entities/resource/42/tags/:key", ({ params }) => {
        deletedKey = params.key as string;
        return new HttpResponse(null, { status: 204 });
      }),
    );

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByText("env: prod")).toBeTruthy();
    });

    fireEvent.click(screen.getByLabelText("remove-tag"));

    await waitFor(() => {
      expect(deletedKey).toBe("env");
      expect(refetchCount).toBeGreaterThan(1);
    });
  });

  it("EntityTagEditor_fetch_failure_shows_error_notification", async () => {
    const { notification } = await import("antd");

    server.use(
      http.get("/api/v1/tenants/acme/entities/resource/42/tags", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to load tags" }),
      );
    });
  });

  it("EntityTagEditor_add_failure_shows_error_notification", async () => {
    const { notification } = await import("antd");

    server.use(
      http.post("/api/v1/tenants/acme/entities/resource/42/tags", () => {
        return new HttpResponse("Bad Request", { status: 400 });
      }),
    );

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByPlaceholderText("Key")).toBeTruthy();
    });

    fireEvent.change(screen.getByPlaceholderText("Key"), { target: { value: "env" } });
    fireEvent.change(screen.getByPlaceholderText("Value"), { target: { value: "prod" } });
    fireEvent.click(screen.getByText("Add"));

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to add tag" }),
      );
    });
  });

  it("EntityTagEditor_delete_failure_shows_error_notification", async () => {
    const { notification } = await import("antd");

    server.use(
      http.delete("/api/v1/tenants/acme/entities/resource/42/tags/:tagKey", () => {
        return new HttpResponse(null, { status: 500 });
      }),
    );

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByText("env: prod")).toBeTruthy();
    });

    fireEvent.click(screen.getByLabelText("remove-tag"));

    await waitFor(() => {
      expect(vi.mocked(notification.error)).toHaveBeenCalledWith(
        expect.objectContaining({ message: "Failed to remove tag" }),
      );
    });
  });

  it("EntityTagEditor_read_only_mode_hides_form_and_close_buttons", async () => {
    const { useTenant } = await import("../../providers/TenantContext");
    vi.mocked(useTenant).mockReturnValue({
      currentTenant: mockTenantBase,
      tenants: [mockTenantBase],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
      isReadOnly: true,
    });

    render(
      <EntityTagEditor tenantName="acme" entityType="resource" entityId="42" />,
    );

    await waitFor(() => {
      expect(screen.getByText("env: prod")).toBeTruthy();
    });

    // No add form in read-only mode
    expect(screen.queryByPlaceholderText("Key")).toBeNull();
    expect(screen.queryByText("Add")).toBeNull();
    // No close buttons on chips
    expect(screen.queryByLabelText("remove-tag")).toBeNull();
  });
});
