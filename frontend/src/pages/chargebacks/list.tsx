import type React from "react";
import { Typography } from "antd";
import { useEffect, useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { useSearchParams } from "react-router";
import { ChargebackGrid } from "../../components/chargebacks/ChargebackGrid";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { ExportButton } from "../../components/chargebacks/ExportButton";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import { useTenant } from "../../providers/TenantContext";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

const { Text, Title } = Typography;

export function ChargebackListPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, queryParams } = useChargebackFilters();
  const [searchParams] = useSearchParams();
  const [selectedRow, setSelectedRow] = useState<{ id: number; tags: Record<string, string> } | null>(null);
  const gridRef = useRef<AgGridReact>(null);

  // Read `selected` param from URL to open drawer on mount — run once
  useEffect(() => {
    const selected = searchParams.get("selected");
    if (selected !== null) {
      const id = parseInt(selected, 10);
      if (!isNaN(id)) {
        setSelectedRow({ id, tags: {} });
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          margin: "0 0 8px 0",
        }}
      >
        <Title level={3} style={{ margin: 0 }}>
          Chargebacks
        </Title>
        <ExportButton filters={queryParams} tenantName={currentTenant.tenant_name} disabled={isReadOnly} />
      </div>
      <FilterPanel
        filters={filters}
        onChange={setFilter}
        onBatchChange={setFilters}
        onReset={resetFilters}
        onRefresh={() => gridRef.current?.api?.refreshInfiniteCache()}
        tenantName={currentTenant.tenant_name}
      />
      <ChargebackGrid
        key={currentTenant.tenant_name}
        ref={gridRef}
        tenantName={currentTenant.tenant_name}
        filters={queryParams}
        onRowClick={(row) => setSelectedRow({ id: row.dimension_id!, tags: row.tags })}
      />
      <ChargebackDetailDrawer
        dimensionId={selectedRow?.id ?? null}
        inheritedTags={selectedRow?.tags ?? {}}
        onClose={() => setSelectedRow(null)}
      />
    </div>
  );
}
