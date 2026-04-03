import type React from "react";
import { Typography, Tabs } from "antd";
import { useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { TopicAttributionGrid } from "../../components/topicAttributions/TopicAttributionGrid";
import { TopicAttributionFilterPanel } from "../../components/topicAttributions/TopicAttributionFilterPanel";
import { TopicAttributionExportButton } from "../../components/topicAttributions/TopicAttributionExportButton";
import { TopicAttributionAnalytics } from "../../components/topicAttributions/TopicAttributionAnalytics";
import { useTopicAttributionFilters } from "../../hooks/useTopicAttributionFilters";
import { useTenant } from "../../providers/TenantContext";

const { Text, Title } = Typography;

export function TopicAttributionPage(): React.JSX.Element {
  const { currentTenant, isReadOnly } = useTenant();
  const { filters, setFilter, setFilters, resetFilters, queryParams } =
    useTopicAttributionFilters();
  const gridRef = useRef<AgGridReact>(null);
  const [activeTab, setActiveTab] = useState<"table" | "analytics">("table");

  if (!currentTenant) {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
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
          Topic Attribution
        </Title>
        <TopicAttributionExportButton
          filters={queryParams}
          tenantName={currentTenant.tenant_name}
          disabled={isReadOnly}
        />
      </div>
      <TopicAttributionFilterPanel
        tenantName={currentTenant.tenant_name}
        filters={filters}
        onChange={setFilter}
        onBatchChange={setFilters}
        onReset={resetFilters}
        activeTab={activeTab}
        onRefresh={
          activeTab === "table"
            ? () => gridRef.current?.api?.refreshInfiniteCache()
            : undefined
        }
      />
      <Tabs
        activeKey={activeTab}
        onChange={(key) => setActiveTab(key as "table" | "analytics")}
        style={{ flex: 1 }}
        items={[
          {
            key: "table",
            label: "Table",
            children: (
              <TopicAttributionGrid
                key={currentTenant.tenant_name}
                ref={gridRef}
                tenantName={currentTenant.tenant_name}
                filters={queryParams}
              />
            ),
          },
          {
            key: "analytics",
            label: "Analytics",
            children: (
              <TopicAttributionAnalytics
                tenantName={currentTenant.tenant_name}
                filters={filters}
              />
            ),
          },
        ]}
      />
    </div>
  );
}
