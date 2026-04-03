import { render, screen } from "@testing-library/react";
import type { ColDef } from "ag-grid-community";
import type { JSX, Ref } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "msw";
import { server } from "../../test/mocks/server";
import { TopicAttributionGrid } from "./TopicAttributionGrid";

type AgGridProps = {
  columnDefs?: ColDef[];
  datasource?: { getRows: (params: unknown) => void };
  rowModelType?: string;
  cacheBlockSize?: number;
  style?: object;
  ref?: Ref<unknown>;
};

type RenderOverrideFn = (props: AgGridProps) => JSX.Element;
let renderOverride: RenderOverrideFn | undefined;

vi.mock("ag-grid-react", () => ({
  AgGridReact: (props: AgGridProps) => {
    const { columnDefs, datasource } = props;

    if (renderOverride) {
      const impl = renderOverride;
      renderOverride = undefined;
      return impl(props);
    }

    return (
      <div
        data-testid="ag-grid"
        data-has-datasource={datasource ? "true" : "false"}
        data-columns={columnDefs?.map((c) => c.field).join(",")}
      >
        AG Grid
      </div>
    ) as JSX.Element;
  },
}));

beforeEach(() => {
  renderOverride = undefined;
});

describe("TopicAttributionGrid", () => {
  it("renders AG Grid wrapper", () => {
    render(<TopicAttributionGrid tenantName="acme" filters={{}} />);
    expect(screen.getByTestId("ag-grid")).toBeTruthy();
  });

  it("passes datasource to AG Grid (infinite scroll)", () => {
    render(
      <TopicAttributionGrid
        tenantName="acme"
        filters={{ start_date: "2026-01-01" }}
      />,
    );
    expect(
      screen.getByTestId("ag-grid").getAttribute("data-has-datasource"),
    ).toBe("true");
  });

  it("renders expected column fields: timestamp, topic_name, cluster_resource_id, product_type, attribution_method, amount", () => {
    let capturedColDefs: ColDef[] | undefined;

    renderOverride = ({ columnDefs }: AgGridProps) => {
      capturedColDefs = columnDefs;
      return <div data-testid="ag-grid" />;
    };

    render(<TopicAttributionGrid tenantName="acme" filters={{}} />);

    expect(capturedColDefs).toBeDefined();
    const fields = capturedColDefs!.map((c) => c.field);
    expect(fields).toContain("timestamp");
    expect(fields).toContain("topic_name");
    expect(fields).toContain("cluster_resource_id");
    expect(fields).toContain("product_type");
    expect(fields).toContain("attribution_method");
    expect(fields).toContain("amount");
  });

  it("datasource fetches data from API and calls successCallback", async () => {
    let capturedDatasource:
      | {
          getRows: (p: {
            startRow: number;
            successCallback: (rows: unknown[], total: number) => void;
            failCallback: () => void;
          }) => void;
        }
      | undefined;

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

    server.use(
      http.get("/api/v1/tenants/acme/topic-attributions", () =>
        HttpResponse.json({
          items: [
            {
              dimension_id: 1,
              topic_name: "my-topic",
              cluster_resource_id: "lkc-abc",
              amount: "10.00",
            },
          ],
          total: 1,
          page: 1,
          page_size: 100,
          pages: 1,
        }),
      ),
    );

    render(<TopicAttributionGrid tenantName="acme" filters={{}} />);

    expect(capturedDatasource).toBeDefined();

    const successCallback = vi.fn();
    const failCallback = vi.fn();

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback,
      failCallback,
    });

    await vi.waitFor(() => {
      expect(successCallback).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({ topic_name: "my-topic" }),
        ]),
        1,
      );
    });
  });

  it("datasource calls failCallback on API error", async () => {
    let capturedDatasource:
      | {
          getRows: (p: {
            startRow: number;
            successCallback: (rows: unknown[], total: number) => void;
            failCallback: () => void;
          }) => void;
        }
      | undefined;

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

    server.use(
      http.get(
        "/api/v1/tenants/acme/topic-attributions",
        () => new HttpResponse(null, { status: 500 }),
      ),
    );

    render(<TopicAttributionGrid tenantName="acme" filters={{}} />);

    const successCallback = vi.fn();
    const failCallback = vi.fn();

    capturedDatasource!.getRows({
      startRow: 0,
      successCallback,
      failCallback,
    });

    await vi.waitFor(() => {
      expect(failCallback).toHaveBeenCalled();
    });
  });

  it("datasource calculates page from startRow for pagination", async () => {
    let capturedDatasource:
      | {
          getRows: (p: {
            startRow: number;
            successCallback: (rows: unknown[], total: number) => void;
            failCallback: () => void;
          }) => void;
        }
      | undefined;
    let capturedUrl = "";

    renderOverride = ({ datasource }: AgGridProps) => {
      capturedDatasource = datasource as typeof capturedDatasource;
      return <div data-testid="ag-grid" />;
    };

    server.use(
      http.get("/api/v1/tenants/acme/topic-attributions", ({ request }) => {
        capturedUrl = request.url;
        return HttpResponse.json({
          items: [],
          total: 0,
          page: 2,
          page_size: 100,
          pages: 0,
        });
      }),
    );

    render(<TopicAttributionGrid tenantName="acme" filters={{}} />);

    capturedDatasource!.getRows({
      startRow: 100, // page 2
      successCallback: vi.fn(),
      failCallback: vi.fn(),
    });

    await vi.waitFor(() => {
      expect(capturedUrl).toContain("page=2");
    });
  });
});
