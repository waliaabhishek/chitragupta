import { Typography } from "antd";
import { useCallback, useEffect, useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { useSearchParams } from "react-router-dom";
import { ChargebackGrid } from "../../components/chargebacks/ChargebackGrid";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { ExportButton } from "../../components/chargebacks/ExportButton";
import { BulkTagModal } from "../../components/chargebacks/BulkTagModal";
import { SelectionToolbar } from "./SelectionToolbar";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import { useTenant } from "../../providers/TenantContext";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

const { Text, Title } = Typography;

export function ChargebackListPage(): JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, toQueryParams } = useChargebackFilters();
  const [searchParams] = useSearchParams();
  const [selectedDimensionId, setSelectedDimensionId] = useState<number | null>(null);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [selectAllFilters, setSelectAllFilters] = useState<Record<string, string> | null>(null);
  const [selectAllTotal, setSelectAllTotal] = useState(0);
  const [bulkModalOpen, setBulkModalOpen] = useState(false);
  const gridRef = useRef<AgGridReact>(null);

  // Read `selected` param from URL to open drawer on mount
  useEffect(() => {
    const selected = searchParams.get("selected");
    if (selected !== null) {
      const id = parseInt(selected, 10);
      if (!isNaN(id)) {
        setSelectedDimensionId(id);
      }
    }
  }, []); // Run once on mount

  const handleTagsChanged = useCallback(() => {
    gridRef.current?.api?.refreshInfiniteCache();
  }, []);

  const handleSelectionChange = useCallback((ids: number[]) => {
    setSelectedIds(ids);
    if (ids.length === 0) {
      setSelectAllFilters(null);
    }
  }, []);

  const handleSelectAll = useCallback(
    (total: number) => {
      setSelectAllFilters(toQueryParams());
      setSelectAllTotal(total);
      setSelectedIds([]);
    },
    [toQueryParams],
  );

  const handleClearSelection = useCallback(() => {
    setSelectedIds([]);
    setSelectAllFilters(null);
  }, []);

  const handleBulkSuccess = useCallback(() => {
    setBulkModalOpen(false);
    setSelectedIds([]);
    setSelectAllFilters(null);
    gridRef.current?.api?.refreshInfiniteCache();
  }, []);

  const totalSelected = selectAllFilters !== null ? selectAllTotal : selectedIds.length;
  const hasSelection = totalSelected > 0;

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
        <ExportButton filters={toQueryParams()} tenantName={currentTenant.tenant_name} />
      </div>
      <FilterPanel filters={filters} onChange={setFilter} onBatchChange={setFilters} onReset={resetFilters} tenantName={currentTenant.tenant_name} />
      {hasSelection && (
        <SelectionToolbar
          selectedCount={totalSelected}
          isSelectAllMode={selectAllFilters !== null}
          totalCount={selectAllTotal}
          onClear={handleClearSelection}
          onAddTags={() => setBulkModalOpen(true)}
        />
      )}
      <ChargebackGrid
        key={currentTenant.tenant_name}
        ref={gridRef}
        tenantName={currentTenant.tenant_name}
        filters={toQueryParams()}
        onRowClick={(dimensionId) => setSelectedDimensionId(dimensionId)}
        onSelectionChange={handleSelectionChange}
        onSelectAll={handleSelectAll}
      />
      <ChargebackDetailDrawer
        dimensionId={selectedDimensionId}
        onClose={() => setSelectedDimensionId(null)}
        onTagsChanged={handleTagsChanged}
      />
      {bulkModalOpen && (
        <BulkTagModal
          tenantName={currentTenant.tenant_name}
          selectedIds={selectAllFilters === null ? selectedIds : null}
          filters={selectAllFilters}
          totalCount={totalSelected}
          onClose={() => setBulkModalOpen(false)}
          onSuccess={handleBulkSuccess}
        />
      )}
    </div>
  );
}
