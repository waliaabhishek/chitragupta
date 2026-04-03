import type React from "react";
import { Button, DatePicker, Form, Input, Select } from "antd";
import dayjs from "dayjs";
import { useFilterOptions } from "../../hooks/useFilterOptions";
import type { ChargebackFilters } from "../../types/filters";
import { filterByLabel } from "../../utils/filterHelpers";
import { TIMEZONE_OPTIONS } from "../../utils/timezoneOptions";

const COST_TYPE_OPTIONS = [
  { label: "Usage", value: "usage" },
  { label: "Shared", value: "shared" },
];

interface FilterPanelProps {
  filters: ChargebackFilters;
  onChange: (key: keyof ChargebackFilters, value: string | null) => void;
  onBatchChange?: (updates: Partial<ChargebackFilters>) => void;
  onReset: () => void;
  onRefresh?: () => void;
  tenantName: string;
}

export function FilterPanel({
  filters,
  onChange,
  onBatchChange,
  onReset,
  onRefresh,
  tenantName,
}: FilterPanelProps): React.JSX.Element {
  const startValue = filters.start_date ? dayjs(filters.start_date) : null;
  const endValue = filters.end_date ? dayjs(filters.end_date) : null;

  const { identityOptions, resourceOptions, productTypeOptions, isLoading } =
    useFilterOptions(tenantName, filters.start_date, filters.end_date);

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

      <Form.Item label="Identity">
        <Select
          placeholder="Any identity"
          value={filters.identity_id ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("identity_id", val ?? null)
          }
          options={identityOptions}
          showSearch
          allowClear
          loading={isLoading}
          filterOption={filterByLabel}
          style={{ width: 220 }}
        />
      </Form.Item>

      <Form.Item label="Product Type">
        <Select
          placeholder="Any product type"
          value={filters.product_type ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("product_type", val ?? null)
          }
          options={productTypeOptions}
          showSearch
          allowClear
          loading={isLoading}
          filterOption={filterByLabel}
          style={{ width: 180 }}
        />
      </Form.Item>

      <Form.Item label="Resource">
        <Select
          placeholder="Any resource"
          value={filters.resource_id ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("resource_id", val ?? null)
          }
          options={resourceOptions}
          showSearch
          allowClear
          loading={isLoading}
          filterOption={filterByLabel}
          style={{ width: 220 }}
        />
      </Form.Item>

      <Form.Item label="Cost Type">
        <Select
          placeholder="Any"
          value={filters.cost_type ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("cost_type", val ?? null)
          }
          options={COST_TYPE_OPTIONS}
          allowClear
          style={{ width: 120 }}
        />
      </Form.Item>

      <Form.Item label="Timezone">
        <Select
          value={filters.timezone ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("timezone", val ?? null)
          }
          options={TIMEZONE_OPTIONS}
          showSearch
          filterOption={filterByLabel}
          style={{ width: 220 }}
        />
      </Form.Item>

      <Form.Item label="Tag Key">
        <Input
          placeholder="e.g. cost_center"
          value={filters.tag_key ?? ""}
          onChange={(e) => onChange("tag_key", e.target.value || null)}
          style={{ width: 160 }}
        />
      </Form.Item>

      <Form.Item label="Tag Value">
        <Input
          placeholder="e.g. engineering"
          value={filters.tag_value ?? ""}
          onChange={(e) => onChange("tag_value", e.target.value || null)}
          style={{ width: 160 }}
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
