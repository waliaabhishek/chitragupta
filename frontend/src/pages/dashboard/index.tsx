import { useMemo, useState } from "react";
import { Col, Radio, Row, Typography } from "antd";
import type { TenantStatusSummary } from "../../types/api";
import { useTenant } from "../../providers/TenantContext";
import { useChargebackFilters } from "../../hooks/useChargebackFilters";
import type { UseAggregationParams } from "../../hooks/useAggregation";
import { useAggregation } from "../../hooks/useAggregation";
import { useDataAvailability } from "../../hooks/useDataAvailability";
import { useInventorySummary } from "../../hooks/useInventorySummary";
import { AllocationIssuesTable } from "../../components/dashboard/AllocationIssuesTable";
import { SummaryStatCards } from "../../components/dashboard/SummaryStatCards";
import { InventoryCounters } from "../../components/dashboard/InventoryCounters";
import { FilterPanel } from "../../components/chargebacks/FilterPanel";
import { ChartCard } from "../../components/charts/ChartCard";
import { DataAvailabilityTimeline } from "../../components/charts/DataAvailabilityTimeline";
import { CostTrendChart } from "../../components/charts/CostTrendChart";
import { CostByIdentityChart } from "../../components/charts/CostByIdentityChart";
import { CostByProductChart } from "../../components/charts/CostByProductChart";
import { CostByResourceChart } from "../../components/charts/CostByResourceChart";
import { ProductChartTypeToggle } from "../../components/charts/ProductChartTypeToggle";
import { DimensionPieChart } from "../../components/charts/DimensionPieChart";
import type { ChargebackFilters } from "../../types/filters";

const { Title, Text } = Typography;

type TimeBucket = "day" | "week" | "month";

interface DashboardContentProps {
  tenant: TenantStatusSummary;
  filters: ChargebackFilters;
  timeBucket: TimeBucket;
}

/** Inner component: hooks are unconditionally called here (tenant is always set). */
function DashboardContent({ tenant, filters, timeBucket }: DashboardContentProps): JSX.Element {
  const [productChartType, setProductChartType] = useState<"pie" | "treemap">("pie");

  const sharedParams: Omit<UseAggregationParams, "groupBy"> = useMemo(() => ({
    tenantName: tenant.tenant_name,
    timeBucket,
    startDate: filters.start_date ?? "",
    endDate: filters.end_date ?? "",
    identityId: filters.identity_id,
    productType: filters.product_type,
    resourceId: filters.resource_id,
    costType: filters.cost_type,
  }), [tenant.tenant_name, timeBucket, filters]);

  const trendData = useAggregation({ ...sharedParams, groupBy: ["identity_id"] });
  const productData = useAggregation({ ...sharedParams, groupBy: ["product_type"] });
  const resourceData = useAggregation({ ...sharedParams, groupBy: ["resource_id"] });
  const environmentData = useAggregation({ ...sharedParams, groupBy: ["environment_id"] });
  const productCategoryData = useAggregation({ ...sharedParams, groupBy: ["product_category"] });
  const availabilityData = useDataAvailability({ tenantName: tenant.tenant_name });
  const inventoryData = useInventorySummary({ tenantName: tenant.tenant_name });

  return (
    <Row gutter={[16, 16]}>
      <Col span={24}>
        <SummaryStatCards
          data={trendData.data}
          isLoading={trendData.isLoading}
          error={trendData.error}
        />
      </Col>

      <Col span={24}>
        <InventoryCounters
          data={inventoryData.data}
          isLoading={inventoryData.isLoading}
          error={inventoryData.error}
        />
      </Col>

      <Col span={24}>
        <ChartCard
          title="Data Availability"
          loading={availabilityData.isLoading}
          error={availabilityData.error}
          onRetry={availabilityData.refetch}
        >
          <DataAvailabilityTimeline
            dates={availabilityData.data?.dates ?? []}
            startDate={filters.start_date ?? ""}
            endDate={filters.end_date ?? ""}
          />
        </ChartCard>
      </Col>

      <Col span={24}>
        <ChartCard
          title="Cost Trend Over Time"
          loading={trendData.isLoading}
          error={trendData.error}
          onRetry={trendData.refetch}
        >
          <CostTrendChart data={trendData.data?.buckets ?? []} timeBucket={timeBucket} />
        </ChartCard>
      </Col>

      <Col span={24}>
        <ChartCard
          title="Cost by Identity"
          loading={trendData.isLoading}
          error={trendData.error}
          onRetry={trendData.refetch}
        >
          <CostByIdentityChart data={trendData.data?.buckets ?? []} />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Environment"
          loading={environmentData.isLoading}
          error={environmentData.error}
          onRetry={environmentData.refetch}
        >
          <DimensionPieChart data={environmentData.data?.buckets ?? []} dimension="environment_id" />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Resource"
          loading={resourceData.isLoading}
          error={resourceData.error}
          onRetry={resourceData.refetch}
        >
          <CostByResourceChart data={resourceData.data?.buckets ?? []} />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Product Type"
          loading={productData.isLoading}
          error={productData.error}
          onRetry={productData.refetch}
          extra={<ProductChartTypeToggle value={productChartType} onChange={setProductChartType} />}
        >
          <CostByProductChart data={productData.data?.buckets ?? []} chartType={productChartType} />
        </ChartCard>
      </Col>

      <Col xs={24} sm={12} lg={6}>
        <ChartCard
          title="Cost by Product Category"
          loading={productCategoryData.isLoading}
          error={productCategoryData.error}
          onRetry={productCategoryData.refetch}
        >
          <DimensionPieChart data={productCategoryData.data?.buckets ?? []} dimension="product_category" />
        </ChartCard>
      </Col>

      <Col span={24}>
        <ChartCard title="Allocation Issues">
          <AllocationIssuesTable
            tenantName={tenant.tenant_name}
            filters={filters}
          />
        </ChartCard>
      </Col>
    </Row>
  );
}

/** Top-level page: handles tenant check and filter/time-bucket state. */
export function CostDashboardPage(): JSX.Element {
  const { currentTenant } = useTenant();
  const { filters, setFilter, setFilters, resetFilters } = useChargebackFilters();
  const [timeBucket, setTimeBucket] = useState<TimeBucket>("day");
  const [refreshKey, setRefreshKey] = useState(0);

  return (
    <div>
      <Title level={3}>Cost Dashboard</Title>

      {!currentTenant ? (
        <Text type="secondary">Select a tenant to view cost analytics.</Text>
      ) : (
        <>
          <FilterPanel
            filters={filters}
            onChange={setFilter}
            onBatchChange={setFilters}
            onReset={resetFilters}
            onRefresh={() => setRefreshKey((k) => k + 1)}
            tenantName={currentTenant.tenant_name}
          />

          <div style={{ margin: "12px 0" }}>
            <Radio.Group
              value={timeBucket}
              onChange={(e) => setTimeBucket(e.target.value as TimeBucket)}
            >
              <Radio.Button value="day">Daily</Radio.Button>
              <Radio.Button value="week">Weekly</Radio.Button>
              <Radio.Button value="month">Monthly</Radio.Button>
            </Radio.Group>
          </div>

          <DashboardContent
            key={refreshKey}
            tenant={currentTenant}
            filters={filters}
            timeBucket={timeBucket}
          />
        </>
      )}
    </div>
  );
}
