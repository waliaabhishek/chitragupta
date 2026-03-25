import type React from "react";
import { useEffect, useState } from "react";
import { Button, DatePicker, Form, Input, Select } from "antd";
import dayjs from "dayjs";
import { API_URL } from "../../config";
import type { PaginatedResponse, ResourceResponse } from "../../types/api";
import type { BillingFilters } from "../../types/filters";
import type { SelectOption } from "../../hooks/useFilterOptions";
import { filterByLabel } from "../../utils/filterHelpers";

interface BillingFilterPanelProps {
  filters: BillingFilters;
  onChange: (key: keyof BillingFilters, value: string | null) => void;
  onBatchChange?: (updates: Partial<BillingFilters>) => void;
  onReset: () => void;
  onRefresh?: () => void;
  tenantName: string;
}

export function BillingFilterPanel({
  filters,
  onChange,
  onBatchChange,
  onReset,
  onRefresh,
  tenantName,
}: BillingFilterPanelProps): React.JSX.Element {
  const [resourceOptions, setResourceOptions] = useState<SelectOption[]>([]);
  const [resourcesLoading, setResourcesLoading] = useState(false);

  useEffect(() => {
    if (!tenantName) return;
    const controller = new AbortController();
    setResourcesLoading(true);

    const resourceUrl = `${API_URL}/tenants/${tenantName}/resources?page_size=1000`;
    fetch(resourceUrl, { signal: controller.signal })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<PaginatedResponse<ResourceResponse>>;
      })
      .then((resources) => {
        setResourceOptions(
          resources.items.map((r) => ({
            label: r.display_name ? `${r.display_name} (${r.resource_id})` : r.resource_id,
            value: r.resource_id,
          })),
        );
        setResourcesLoading(false);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setResourcesLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [tenantName]);

  const startValue = filters.start_date ? dayjs(filters.start_date) : null;
  const endValue = filters.end_date ? dayjs(filters.end_date) : null;

  return (
    <Form layout="inline" style={{ padding: "8px 0", flexWrap: "wrap" }}>
      <Form.Item label="Date Range">
        <DatePicker.RangePicker
          value={startValue && endValue ? [startValue, endValue] : [null, null]}
          onChange={(dates) => {
            const start = dates?.[0] ? dates[0].format("YYYY-MM-DD") : null;
            const end = dates?.[1] ? dates[1].format("YYYY-MM-DD") : null;
            if (onBatchChange) {
              onBatchChange({ start_date: start, end_date: end });
            } else {
              onChange("start_date", start);
              onChange("end_date", end);
            }
          }}
          allowClear
        />
      </Form.Item>

      <Form.Item label="Product Type">
        <Input
          placeholder="Any product type"
          value={filters.product_type ?? ""}
          onChange={(e) => onChange("product_type", e.target.value || null)}
          allowClear
          style={{ width: 180 }}
        />
      </Form.Item>

      <Form.Item label="Resource">
        <Select
          placeholder="Any resource"
          value={filters.resource_id ?? undefined}
          onChange={(val: string | undefined) => onChange("resource_id", val ?? null)}
          options={resourceOptions}
          showSearch
          allowClear
          loading={resourcesLoading}
          filterOption={filterByLabel}
          style={{ width: 220 }}
        />
      </Form.Item>

      <Form.Item>
        <Button onClick={onReset}>Reset</Button>
      </Form.Item>
      {onRefresh !== undefined && (
        <Form.Item>
          <Button type="primary" onClick={onRefresh}>
            Refresh Data
          </Button>
        </Form.Item>
      )}
    </Form>
  );
}
