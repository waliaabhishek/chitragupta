import type React from "react";
import { Button, Form, Input, Select } from "antd";
import { useResourceFilterOptions } from "../../hooks/useEntityFilterOptions";
import { filterByLabel } from "../../utils/filterHelpers";
import type { ResourceFilters } from "../../types/filters";

interface ResourceFilterBarProps {
  tenantName: string;
  filters: ResourceFilters;
  onChange: (key: keyof ResourceFilters, value: string | null) => void;
  onReset: () => void;
}

export function ResourceFilterBar({
  tenantName,
  filters,
  onChange,
  onReset,
}: ResourceFilterBarProps): React.JSX.Element {
  const { resourceTypeOptions, resourceStatusOptions, isLoading } =
    useResourceFilterOptions(tenantName);

  return (
    <Form layout="inline" style={{ padding: "8px 0", flexWrap: "wrap" }}>
      <Form.Item label="Search">
        <Input
          placeholder="Resource ID or display name"
          value={filters.search ?? ""}
          onChange={(e) => onChange("search", e.target.value || null)}
          allowClear
          style={{ width: 240 }}
        />
      </Form.Item>
      <Form.Item label="Type">
        <Select
          placeholder="Any type"
          value={filters.resource_type ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("resource_type", val ?? null)
          }
          options={resourceTypeOptions}
          showSearch
          allowClear
          loading={isLoading}
          filterOption={filterByLabel}
          style={{ width: 180 }}
        />
      </Form.Item>
      <Form.Item label="Status">
        <Select
          placeholder="Any status"
          value={filters.status ?? undefined}
          onChange={(val: string | undefined) =>
            onChange("status", val ?? null)
          }
          options={resourceStatusOptions}
          allowClear
          style={{ width: 130 }}
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
    </Form>
  );
}
