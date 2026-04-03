import type React from "react";
import { useState } from "react";
import { Typography } from "antd";
import { IdentityGrid } from "../../components/identities/IdentityGrid";
import { IdentityFilterBar } from "../../components/identities/IdentityFilterBar";
import { IdentityDetailDrawer } from "../../components/identities/IdentityDetailDrawer";
import { useIdentityFilters } from "../../hooks/useIdentityFilters";
import { useTenant } from "../../providers/TenantContext";
import type { IdentityResponse } from "../../types/api";

export function IdentityListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, resetFilters, queryParams } =
    useIdentityFilters();
  const [selected, setSelected] = useState<IdentityResponse | null>(null);

  if (!currentTenant) {
    return (
      <div>
        <Typography.Title level={3}>Identities</Typography.Title>
        <Typography.Text type="secondary">
          Select a tenant to begin.
        </Typography.Text>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Typography.Title level={3} style={{ margin: "0 0 16px 0" }}>
        Identities
      </Typography.Title>
      <IdentityFilterBar
        tenantName={currentTenant.tenant_name}
        filters={filters}
        onChange={setFilter}
        onReset={resetFilters}
      />
      <IdentityGrid
        tenantName={currentTenant.tenant_name}
        queryParams={queryParams}
        onRowClick={setSelected}
      />
      {selected && (
        <IdentityDetailDrawer
          identity={selected}
          tenantName={currentTenant.tenant_name}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
