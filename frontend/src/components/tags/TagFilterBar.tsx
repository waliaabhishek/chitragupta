import type React from "react";
import { Button, Form, Input, Select } from "antd";
import { ENTITY_TYPE_OPTIONS } from "../../hooks/useEntityFilterOptions";
import type { TagFilters } from "../../types/filters";

interface TagFilterBarProps {
  filters: TagFilters;
  onChange: (key: keyof TagFilters, value: string | null) => void;
  onReset: () => void;
}

export function TagFilterBar({
  filters,
  onChange,
  onReset,
}: TagFilterBarProps): React.JSX.Element {
  return (
    <Form layout="inline" style={{ padding: "8px 0", flexWrap: "wrap" }}>
      <Form.Item label="Tag Key">
        <Input
          placeholder="Search tag key"
          value={filters.tag_key ?? ""}
          onChange={(e) => onChange("tag_key", e.target.value || null)}
          allowClear
          style={{ width: 200 }}
        />
      </Form.Item>
      <Form.Item label="Entity Type">
        <Select
          placeholder="Any type"
          value={filters.entity_type ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("entity_type", val ?? null)
          }
          options={ENTITY_TYPE_OPTIONS}
          allowClear
          style={{ width: 140 }}
        />
      </Form.Item>
      <Form.Item>
        <Button onClick={onReset}>Reset</Button>
      </Form.Item>
    </Form>
  );
}
