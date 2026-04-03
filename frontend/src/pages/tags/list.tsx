import type React from "react";
import { Typography } from "antd";
import { TagsGrid } from "../../components/tags/TagsGrid";
import { TagFilterBar } from "../../components/tags/TagFilterBar";
import { useTagFilters } from "../../hooks/useTagFilters";
import { useTenant } from "../../providers/TenantContext";

export function TagManagementPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const { filters, setFilter, resetFilters, queryParams } = useTagFilters();

  if (!currentTenant) {
    return (
      <div>
        <Typography.Title level={3}>Tags</Typography.Title>
        <Typography.Text type="secondary">
          Select a tenant to view tags.
        </Typography.Text>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Typography.Title level={3} style={{ margin: "0 0 16px 0" }}>
        Tag Management
      </Typography.Title>
      <TagFilterBar
        filters={filters}
        onChange={setFilter}
        onReset={resetFilters}
      />
      <TagsGrid
        tenantName={currentTenant.tenant_name}
        queryParams={queryParams}
        isReadOnly={isReadOnly}
      />
    </div>
  );
}
