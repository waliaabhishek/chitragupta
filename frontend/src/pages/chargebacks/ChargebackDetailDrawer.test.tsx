// TASK-160.02 TDD red phase — ChargebackDetailDrawer rewrite.
// New interface: { dimensionId, inheritedTags, onClose }
// Old interface had: { dimensionId, onTagsChanged, onClose } + fetched tags from API.
// Tests 8-9 will FAIL because current implementation:
//   - uses onTagsChanged (not in new interface)
//   - shows TagEditor add/remove form (not in new interface)
//   - does not accept inheritedTags prop
//   - shows display_name ("prod") not "key: value" format ("env: prod")
import type React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import type { ChargebackDimensionResponse } from "../../types/api";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

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
      Item: ({ label, children }: { label: string; children: ReactNode }) => (
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
        <button
          className="ant-tag-close-icon"
          onClick={onClose}
          aria-label="remove"
        >
          x
        </button>
      )}
    </span>
  ),
  Typography: {
    Title: ({ children }: { children: ReactNode; level?: number }) => (
      <h5>{children}</h5>
    ),
    Text: ({ children }: { children: ReactNode; type?: string }) => (
      <span>{children}</span>
    ),
  },
  Space: ({
    children,
  }: {
    children: ReactNode;
    wrap?: boolean;
    style?: object;
  }) => <span>{children}</span>,
  Form: Object.assign(
    ({
      children,
      onFinish,
    }: {
      children: ReactNode;
      layout?: string;
      onFinish?: (v: unknown) => void;
    }) => (
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
      Item: ({
        children,
        name,
      }: {
        children: ReactNode;
        name?: string;
        rules?: unknown[];
      }) => <div data-name={name}>{children}</div>,
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
    htmlType?: "button" | "submit" | "reset";
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
      isReadOnly: false,
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
  tags: {},
};

function wrapper({ children }: { children: ReactNode }): React.JSX.Element {
  return <MemoryRouter>{children}</MemoryRouter>;
}

beforeEach(() => {
  server.use(
    http.get("/api/v1/tenants/acme/chargebacks/42", () => {
      return HttpResponse.json(dimensionFixture);
    }),
  );
});

describe("ChargebackDetailDrawer", () => {
  it("does not render when dimensionId is null", () => {
    const { container } = render(
      <ChargebackDetailDrawer
        dimensionId={null}
        inheritedTags={{}}
        onClose={vi.fn()}
      />,
      { wrapper },
    );
    expect(container.firstChild).toBeNull();
  });

  it("calls onClose when close button clicked", async () => {
    const onClose = vi.fn();
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        inheritedTags={{}}
        onClose={onClose}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    fireEvent.click(screen.getByLabelText("close"));
    expect(onClose).toHaveBeenCalled();
  });

  it("ChargebackDetailDrawer_shows_inherited_tags_as_key_value_chips", async () => {
    // TASK-160.02 test 8: inherited tags shown as "key: value" chips.
    // FAILS: current impl does not accept inheritedTags prop; shows TagEditor form.
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        inheritedTags={{ env: "prod" }}
        onClose={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    // "env: prod" chip visible
    expect(screen.getByText("env: prod")).toBeTruthy();

    // No add/remove controls (read-only inherited tags display)
    expect(screen.queryByPlaceholderText("Key")).toBeNull();
    expect(screen.queryByText("Add")).toBeNull();
    expect(screen.queryByLabelText("remove")).toBeNull();
  });

  it("ChargebackDetailDrawer_shows_no_tags_text_for_empty_inheritedTags", async () => {
    // TASK-160.02 test 9: empty inheritedTags shows "No tags" fallback.
    // FAILS: current impl shows TagEditor form (add tag UI) for empty tags.
    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        inheritedTags={{}}
        onClose={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.queryByTestId("spinner")).toBeNull();
    });

    expect(screen.getByText("No tags")).toBeTruthy();
  });

  it("shows dimension not found when API returns 404", async () => {
    server.use(
      http.get("/api/v1/tenants/acme/chargebacks/42", () => {
        return new HttpResponse(null, { status: 404 });
      }),
    );

    render(
      <ChargebackDetailDrawer
        dimensionId={42}
        inheritedTags={{}}
        onClose={vi.fn()}
      />,
      { wrapper },
    );

    await waitFor(() => {
      expect(screen.getByText("Dimension not found.")).toBeTruthy();
    });
  });
});
