import type React from "react";
import { Button, Form, Input, Select } from "antd";
import { useIdentityFilterOptions } from "../../hooks/useEntityFilterOptions";
import { filterByLabel } from "../../utils/filterHelpers";
import type { IdentityFilters } from "../../types/filters";

interface IdentityFilterBarProps {
  tenantName: string;
  filters: IdentityFilters;
  onChange: (key: keyof IdentityFilters, value: string | null) => void;
  onReset: () => void;
}

export function IdentityFilterBar({
  tenantName,
  filters,
  onChange,
  onReset,
}: IdentityFilterBarProps): React.JSX.Element {
  const { identityTypeOptions, isLoading } = useIdentityFilterOptions(tenantName);

  return (
    <Form layout="inline" style={{ padding: "8px 0", flexWrap: "wrap" }}>
      <Form.Item label="Search">
        <Input
          placeholder="Identity ID or display name"
          value={filters.search ?? ""}
          onChange={(e) => onChange("search", e.target.value || null)}
          allowClear
          style={{ width: 240 }}
        />
      </Form.Item>
      <Form.Item label="Type">
        <Select
          placeholder="Any type"
          value={filters.identity_type ?? undefined}
          onChange={(val: string | undefined) => onChange("identity_type", val ?? null)}
          options={identityTypeOptions}
          showSearch
          allowClear
          loading={isLoading}
          filterOption={filterByLabel}
          style={{ width: 180 }}
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
