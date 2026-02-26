import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import type { ChargebackDimensionResponse } from "../../types/api";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

// Mock antd to avoid jsdom incompatibilities (Drawer portals, responsive observers).
vi.mock("antd", () => ({
  Drawer: ({
    open,
    onClose,
    title,
    children,
  }: {
    open: boolean;
    onClose: () => void;
    title: string;
    children: ReactNode;
  }) =>
    open ? (
      <div data-testid="drawer">
        <span>{title}</span>
        <button
          className="ant-drawer-close"
          aria-label="close"
          onClick={onClose}
        >
          ×
        </button>
        {children}
      </div>
    ) : null,
  Descriptions: Object.assign(
    ({
      children,
    }: {
      children: ReactNode;
      column?: number;
      size?: string;
      bordered?: boolean;
    }) => <dl>{children}</dl>,
    {
      Item: ({
        label,
        children,
      }: {
        label: string;
        children: ReactNode;
      }) => (
        <div>
          <dt>{label}</dt>
          <dd>{children}</dd>
        </div>
      ),
    },
  ),
  Divider: () => <hr />,
  Spin: () => <div data-testid="spinner">Loading</div>,
  notification: {
    error: vi.fn(),
  },
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
        <button className="ant-tag-close-icon" onClick={onClose} aria-label="remove">
          x
        </button>
      )}
    </span>
  ),
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number }) => (
      <h5>{children}</h5>
    ),
  },
  Space: ({ children }: { children: ReactNode; wrap?: boolean; style?: object }) => (
    <span>{children}</span>
  ),
  Form: Object.assign(
    ({ children, onFinish }: { children: ReactNode; layout?: string; onFinish?: (v: unknown) => void }) => (
      <form
        onSubmit={(e) => {
          e.preventDefault();
          onFinish?.({});
        }}
      >
        {children}
      </form>
    ),
    {
      Item: ({ children, name }: { children: ReactNode; name?: string; rules?: unknown[] }) => (
        <div data-name={name}>{children}</div>
      ),
      useForm: () => [
        {
          resetFields: vi.fn(),
          setFieldsValue: vi.fn(),
          getFieldValue: vi.fn(),
          validateFields: vi.fn().mockResolvedValue({}),
        },
      ],
    },
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
    htmlType?: string;
    loading?: boolean;
  }) => (
    <button type={htmlType ?? "button"} onClick={onClick} disabled={loading}>
      {children}
    </button>
  ),
  Input: ({
    placeholder,
    maxLength,
    onChange,
    value,
    style,
  }: {
    placeholder?: string;
    maxLength?: number;
    onChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
    value?: string;
    style?: object;
  }) => (
    <input
      placeholder={placeholder}
      maxLength={maxLength}
      onChange={onChange}
      value={value}
      style={style}
    />
  ),
}));

// Mock TenantContext — stable object reference prevents spurious useEffect re-runs
vi.mock("../../providers/TenantContext", () => {
  const tenant = {
    tenant_name: "acme",
    tenant_id: "t-001",
    ecosystem: "ccloud",
    dates_pending: 0,
    dates_calculated: 10,
    last_calculated_date: null,
  };
  return {
    useTenant: () => ({
      currentTenant: tenant,
      tenants: [tenant],
      setCurrentTenant: vi.fn(),
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    }),
  };
});

const dimensionFixture: ChargebackDimensionResponse = {
  dimension_id: 42,
  ecosystem: "ccloud",
  tenant_id: "t-001",
  resource_id: "r-001",
  product_category: "KAFKA",
  product_type: "KAFKA_NUM_BYTES",
  identity_id: "user@example.com",
  cost_type: "usage",
  allocation_method: "ratio",
  allocation_detail: null,
  tags: [
    {
      tag_id: 1,
      dimension_id: 42,
      tag_key: "env",
      tag_value: "uuid-prod",
      display_name: "prod",
      created_by: "ui",
      created_at: null,
    },
  ],
};

function wrapper({ children }: { children: ReactNode }): JSX.Element {
  return (
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      {children}
    </MemoryRouter>
  );
}

beforeEach(() => {
  server.use(
    http.get("/api/v1/tenants/acme/chargebacks/42", () => {
      return HttpResponse.json(dimensionFixture);
    }),
    http.patch("/api/v1/tenants/acme/chargebacks/42", async ({ request }) => {
      const body = (await request.json()) as {
        add_tags?: unknown[];
        remove_tag_ids?: number[];
      };
      return HttpResponse.json({
        ...dimensionFixture,
        tags: body.remove_tag_ids?.length ? [] : [...dimensionFixture.tags],
      });
    }),
  );
});

describe("ChargebackDetailDrawer", () => {
  it("does not render when dimensionId is null", () => {
    const { container } = render(
      <ChargebackDetailDrawer
        dimensionId={null}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );
    expect(container.firstChild).toBeNull();
  });

  it("shows loading spinner while fetching", () => {
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );
    // Initially loading
    expect(screen.getByTestId("spinner")).toBeTruthy();
  });

  it("renders drawer with dimension data after fetch", async () => {
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    expect(screen.getByTestId("drawer")).toBeTruthy();
    expect(screen.getByText("KAFKA_NUM_BYTES")).toBeTruthy();
    expect(screen.getByText("user@example.com")).toBeTruthy();
  });

  it("calls onClose when close button clicked", async () => {
    const onClose = vi.fn();
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={onClose}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    fireEvent.click(screen.getByLabelText("close"));
    expect(onClose).toHaveBeenCalled();
  });

  it("shows not found message when API returns 404", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/999", () => {
        return new HttpResponse(null, { status: 404 });
      }),
    );

    render(
      <ChargebackDetailDrawer
        dimensionId={999}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.getByText("Dimension not found.")).toBeTruthy();
    });
  });

  it("calls onTagsChanged after adding a tag", async () => {
    const onTagsChanged = vi.fn();
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={vi.fn()}
        onTagsChanged={onTagsChanged}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    // Find the add button in TagEditor and click it via form submit
    const keyInput = screen.getByPlaceholderText("Key");
    const valueInput = screen.getByPlaceholderText("Display Name");
    fireEvent.change(keyInput, { target: { value: "newkey" } });
    fireEvent.change(valueInput, { target: { value: "New Value" } });
    fireEvent.click(screen.getByText("Add"));

    await waitFor(() => {
      expect(onTagsChanged).toHaveBeenCalled();
    });
  });

  it("calls onTagsChanged after removing a tag", async () => {
    const onTagsChanged = vi.fn();
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={vi.fn()}
        onTagsChanged={onTagsChanged}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.getByText("prod")).toBeTruthy();
    });

    const removeBtn = screen.getByLabelText("remove");
    fireEvent.click(removeBtn);

    await waitFor(() => {
      expect(onTagsChanged).toHaveBeenCalled();
    });
  });

  it("fetches new dimension when dimensionId changes", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/43", () => {
        return HttpResponse.json({
          ...dimensionFixture,
          dimension_id: 43,
          identity_id: "other-user@example.com",
          tags: [],
        });
      }),
    );

    const { rerender } = render(
      <ChargebackDetailDrawer
        dimensionId={42}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.getByText("user@example.com")).toBeTruthy();
    });

    // rerender uses the same wrapper — no nested router
    rerender(
      <ChargebackDetailDrawer
        dimensionId={43}
        onClose={vi.fn()}
        onTagsChanged={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("other-user@example.com")).toBeTruthy();
    });
  });
});
