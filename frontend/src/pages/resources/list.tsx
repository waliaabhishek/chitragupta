import type React from "react";
import { useState } from "react";
import { Typography } from "antd";
import { ResourceGrid } from "../../components/resources/ResourceGrid";
import { ResourceFilterBar } from "../../components/resources/ResourceFilterBar";
import { ResourceDetailDrawer } from "../../components/resources/ResourceDetailDrawer";
import { useResourceFilters } from "../../hooks/useResourceFilters";
import { useTenant } from "../../providers/TenantContext";
import type { ResourceResponse } from "../../types/api";

export function ResourceListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, resetFilters, queryParams } =
    useResourceFilters();
  const [selected, setSelected] = useState<ResourceResponse | null>(null);

  if (!currentTenant) {
    return (
      <div>
        <Typography.Title level={3}>Resources</Typography.Title>
        <Typography.Text type="secondary">
          Select a tenant to begin.
        </Typography.Text>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Typography.Title level={3} style={{ margin: "0 0 16px 0" }}>
        Resources
      </Typography.Title>
      <ResourceFilterBar
        tenantName={currentTenant.tenant_name}
        filters={filters}
        onChange={setFilter}
        onReset={resetFilters}
      />
      <ResourceGrid
        tenantName={currentTenant.tenant_name}
        queryParams={queryParams}
        onRowClick={setSelected}
      />
      {selected && (
        <ResourceDetailDrawer
          resource={selected}
          tenantName={currentTenant.tenant_name}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
