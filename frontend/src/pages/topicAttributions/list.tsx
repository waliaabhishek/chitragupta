import type React from "react";
import { Alert, Typography, Segmented } from "antd";
import { useRef, useState } from "react";
import type { AgGridReact } from "ag-grid-react";
import { TopicAttributionGrid } from "../../components/topicAttributions/TopicAttributionGrid";
import { TopicAttributionFilterPanel } from "../../components/topicAttributions/TopicAttributionFilterPanel";
import { TopicAttributionExportButton } from "../../components/topicAttributions/TopicAttributionExportButton";
import { TopicAttributionAnalytics } from "../../components/topicAttributions/TopicAttributionAnalytics";
import { useTopicAttributionFilters } from "../../hooks/useTopicAttributionFilters";
import { useTenant } from "../../providers/TenantContext";

const { Text, Title } = Typography;

const TAB_OPTIONS: Array<{ label: string; value: "table" | "analytics" }> = [
  { label: "Table", value: "table" },
  { label: "Analytics", value: "analytics" },
];

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

  if (currentTenant.topic_attribution_status === "config_error") {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
        <Alert
          type="error"
          showIcon
          message="Topic Attribution configuration error"
          description={
            currentTenant.topic_attribution_error ??
            "Configuration validation failed."
          }
        />
      </div>
    );
  }

  if (currentTenant.topic_attribution_status === "disabled") {
    return (
      <div>
        <Title level={3}>Topic Attribution</Title>
        <Alert
          type="info"
          showIcon
          message="Topic Attribution is not configured"
          description={
            <span>
              Topic Attribution overlays Kafka topic-level cost attribution on
              top of chargeback data, enabling per-topic cost breakdowns across
              your Confluent Cloud environment. To enable it, add{" "}
              <code>topic_attribution.enabled: true</code> under your
              tenant&apos;s
              <code>plugin_settings</code> in the YAML config and restart the
              service.
            </span>
          }
        />
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
      <Segmented
        options={TAB_OPTIONS}
        value={activeTab}
        onChange={(value) => setActiveTab(value as "table" | "analytics")}
        style={{ marginBottom: 8, alignSelf: "flex-start" }}
      />
      {activeTab === "table" && (
        <TopicAttributionGrid
          key={currentTenant.tenant_name}
          ref={gridRef}
          tenantName={currentTenant.tenant_name}
          filters={queryParams}
        />
      )}
      {activeTab === "analytics" && (
        <TopicAttributionAnalytics
          tenantName={currentTenant.tenant_name}
          filters={filters}
        />
      )}
    </div>
  );
}
