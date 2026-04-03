import type React from "react";
import { Typography } from "antd";
import { useRef } from "react";
import type { AgGridReact } from "ag-grid-react";
import { BillingGrid } from "../../components/billing/BillingGrid";
import { BillingFilterPanel } from "../../components/billing/BillingFilterPanel";
import { useBillingFilters } from "../../hooks/useBillingFilters";
import { useTenant } from "../../providers/TenantContext";

const { Text, Title } = Typography;

export function BillingListPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, queryParams } =
    useBillingFilters();
  const gridRef = useRef<AgGridReact>(null);

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Billing</Title>
        <Text type="secondary">Select a tenant to begin.</Text>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Title level={3} style={{ margin: "0 0 8px 0" }}>
        Billing
      </Title>
      <BillingFilterPanel
        filters={filters}
        onChange={setFilter}
        onBatchChange={setFilters}
        onReset={resetFilters}
        onRefresh={() => gridRef.current?.api?.refreshInfiniteCache()}
        tenantName={currentTenant.tenant_name}
      />
      <BillingGrid
        key={currentTenant.tenant_name}
        ref={gridRef}
        tenantName={currentTenant.tenant_name}
        filters={queryParams}
      />
    </div>
  );
}
