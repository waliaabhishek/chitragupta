import type React from "react";
import { Typography } from "antd";
import { useCallback, useEffect, useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { useSearchParams } from "react-router";
import { ChargebackGrid } from "../../components/chargebacks/ChargebackGrid";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { ExportButton } from "../../components/chargebacks/ExportButton";
import { BulkTagModal } from "../../components/chargebacks/BulkTagModal";
import { SelectionToolbar } from "./SelectionToolbar";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import { useTenant } from "../../providers/TenantContext";
import { ChargebackDetailDrawer } from "./ChargebackDetailDrawer";

const { Text, Title } = Typography;

export function ChargebackListPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, toQueryParams, queryParams } = useChargebackFilters();
  const [searchParams] = useSearchParams();
  const [selectedDimensionId, setSelectedDimensionId] = useState<number | null>(null);
  const [selectedIds, setSelectedIds] = useState<number[]>([]);
  const [selectAllFilters, setSelectAllFilters] = useState<Record<string, string> | null>(null);
  const [selectAllTotal, setSelectAllTotal] = useState(0);
  const [bulkModalOpen, setBulkModalOpen] = useState(false);
  const gridRef = useRef<AgGridReact>(null);

  // Read `selected` param from URL to open drawer on mount
  // eslint-disable-next-line react-hooks/exhaustive-deps
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
      {hasSelection && (
        <SelectionToolbar
          selectedCount={totalSelected}
          isSelectAllMode={selectAllFilters !== null}
          totalCount={selectAllTotal}
          onClear={handleClearSelection}
          onAddTags={() => setBulkModalOpen(true)}
          disabled={isReadOnly}
        />
      )}
      <ChargebackGrid
        key={currentTenant.tenant_name}
        ref={gridRef}
        tenantName={currentTenant.tenant_name}
        filters={queryParams}
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
          disabled={isReadOnly}
        />
      )}
    </div>
  );
}
