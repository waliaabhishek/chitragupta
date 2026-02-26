import { Button, DatePicker, Form, Input, Select, Space } from "antd";
import dayjs from "dayjs";
import type { ChargebackFilters } from "../../types/filters";

const COST_TYPE_OPTIONS = [
  { label: "Usage", value: "usage" },
  { label: "Shared", value: "shared" },
];

interface FilterPanelProps {
  filters: ChargebackFilters;
  onChange: (key: keyof ChargebackFilters, value: string | null) => void;
  onReset: () => void;
}

export function FilterPanel({
  filters,
  onChange,
  onReset,
}: FilterPanelProps): JSX.Element {
  const startValue = filters.start_date ? dayjs(filters.start_date) : null;
  const endValue = filters.end_date ? dayjs(filters.end_date) : null;

  return (
    <Form layout="inline" style={{ padding: "8px 0", flexWrap: "wrap" }}>
      <Form.Item label="Date Range">
        <DatePicker.RangePicker
          value={
            startValue && endValue ? [startValue, endValue] : [null, null]
          }
          onChange={(dates) => {
            onChange(
              "start_date",
              dates?.[0] ? dates[0].format("YYYY-MM-DD") : null,
            );
            onChange(
              "end_date",
              dates?.[1] ? dates[1].format("YYYY-MM-DD") : null,
            );
          }}
          allowClear
        />
      </Form.Item>

      <Form.Item label="Identity">
        <Input
          placeholder="Identity ID"
          value={filters.identity_id ?? ""}
          onChange={(e) => onChange("identity_id", e.target.value || null)}
          style={{ width: 180 }}
          allowClear
        />
      </Form.Item>

      <Form.Item label="Product Type">
        <Input
          placeholder="Product type"
          value={filters.product_type ?? ""}
          onChange={(e) => onChange("product_type", e.target.value || null)}
          style={{ width: 160 }}
          allowClear
        />
      </Form.Item>

      <Form.Item label="Resource">
        <Input
          placeholder="Resource ID"
          value={filters.resource_id ?? ""}
          onChange={(e) => onChange("resource_id", e.target.value || null)}
          style={{ width: 160 }}
          allowClear
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

      <Form.Item>
        <Space>
          <Button onClick={onReset}>Reset</Button>
        </Space>
      </Form.Item>
    </Form>
  );
}
