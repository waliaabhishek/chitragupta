import { Typography } from "antd";
import { useCallback, useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { ChargebackGrid } from "../../components/chargebacks/ChargebackGrid";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import { useTenant } from "../../providers/TenantContext";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

const { Text, Title } = Typography;

export function ChargebackListPage(): JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, resetFilters, toQueryParams } =
    useChargebackFilters();
  const [selectedDimensionId, setSelectedDimensionId] = useState<
    number | null
  >(null);
  const gridRef = useRef<AgGridReact>(null);

  const handleTagsChanged = useCallback(() => {
    gridRef.current?.api?.refreshInfiniteCache();
  }, []);

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Chargebacks</Title>
        <Text type="secondary">Select a tenant to begin.</Text>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <Title level={3} style={{ margin: "0 0 8px 0" }}>
        Chargebacks
      </Title>
      <FilterPanel
        filters={filters}
        onChange={setFilter}
        onReset={resetFilters}
      />
      <ChargebackGrid
        key={currentTenant.tenant_name}
        ref={gridRef}
        tenantName={currentTenant.tenant_name}
        filters={toQueryParams()}
        onRowClick={(dimensionId) => setSelectedDimensionId(dimensionId)}
      />
      <ChargebackDetailDrawer
        dimensionId={selectedDimensionId}
        onClose={() => setSelectedDimensionId(null)}
        onTagsChanged={handleTagsChanged}
      />
    </div>
  );
}
