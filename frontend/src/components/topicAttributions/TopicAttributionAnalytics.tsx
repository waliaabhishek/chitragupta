import type React from "react";
import { useState } from "react";
import { Col, Radio, Row } from "antd";
import { ChartCard } from "../charts/ChartCard";
import { TopTopicsChart } from "./charts/TopTopicsChart";
import { CostCompositionChart } from "./charts/CostCompositionChart";
import { CostVelocityChart } from "./charts/CostVelocityChart";
import { AttributionMethodDonut } from "./charts/AttributionMethodDonut";
import { ZombieTopicsTable } from "./charts/ZombieTopicsTable";
import { EnvironmentCostChart } from "./charts/EnvironmentCostChart";
import { ClusterConcentrationChart } from "./charts/ClusterConcentrationChart";
import { ProductTypeMixChart } from "./charts/ProductTypeMixChart";
import { PivotedCostBreakdown } from "./charts/PivotedCostBreakdown";
import { useTopicAttributionAggregation } from "../../hooks/useTopicAttributionAggregation";
import type { TopicAttributionFilters } from "../../types/filters";

interface TopicAttributionAnalyticsProps {
  tenantName: string;
  filters: TopicAttributionFilters;
}

export function TopicAttributionAnalytics({
  tenantName,
  filters,
}: TopicAttributionAnalyticsProps): React.JSX.Element {
  const [timeBucket, setTimeBucket] = useState<"day" | "week" | "month">("day");

  const sharedParams = {
    tenantName,
    timeBucket,
    startDate: filters.start_date ?? "",
    endDate: filters.end_date ?? "",
    clusterResourceId: filters.cluster_resource_id,
    topicName: filters.topic_name,
    productType: filters.product_type,
    timezone: filters.timezone,
  };

  const topTopicsData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["topic_name"],
  });
  const compositionData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["topic_name", "product_type"],
  });
  const methodData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["attribution_method"],
  });
  const envData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["topic_name", "env_id"],
  });
  const clusterData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["cluster_resource_id", "topic_name"],
  });
  const mixData = useTopicAttributionAggregation({
    ...sharedParams,
    groupBy: ["product_type"],
  });

  return (
    <div>
      <Radio.Group
        value={timeBucket}
        onChange={(e) =>
          setTimeBucket(e.target.value as "day" | "week" | "month")
        }
        style={{ margin: "8px 0 16px 0" }}
      >
        <Radio.Button value="day">Daily</Radio.Button>
        <Radio.Button value="week">Weekly</Radio.Button>
        <Radio.Button value="month">Monthly</Radio.Button>
      </Radio.Group>
      <Row gutter={[16, 16]}>
        <Col span={24}>
          <ChartCard
            title="Top Topics by Cost"
            loading={topTopicsData.isLoading}
            error={topTopicsData.error}
            onRetry={topTopicsData.refetch}
          >
            <TopTopicsChart data={topTopicsData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
        <Col span={24}>
          <ChartCard
            title="Cost Composition by Product Type"
            loading={compositionData.isLoading}
            error={compositionData.error}
            onRetry={compositionData.refetch}
          >
            <CostCompositionChart
              data={compositionData.data?.buckets ?? []}
            />
          </ChartCard>
        </Col>
        <Col span={24}>
          <ChartCard
            title="Cost Velocity (Top Movers)"
            loading={topTopicsData.isLoading}
            error={topTopicsData.error}
            onRetry={topTopicsData.refetch}
          >
            <CostVelocityChart
              data={topTopicsData.data?.buckets ?? []}
            />
          </ChartCard>
        </Col>
        <Col xs={24} lg={12}>
          <ChartCard
            title="Attribution Method Confidence"
            loading={methodData.isLoading}
            error={methodData.error}
            onRetry={methodData.refetch}
          >
            <AttributionMethodDonut data={methodData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
        <Col xs={24} lg={12}>
          <ChartCard
            title="Environment Cost Comparison"
            loading={envData.isLoading}
            error={envData.error}
            onRetry={envData.refetch}
          >
            <EnvironmentCostChart data={envData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
        <Col xs={24} lg={12}>
          <ChartCard
            title="Cluster Concentration"
            loading={clusterData.isLoading}
            error={clusterData.error}
            onRetry={clusterData.refetch}
          >
            <ClusterConcentrationChart data={clusterData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
        <Col xs={24} lg={12}>
          <ChartCard
            title="Product Type Mix (100% Area)"
            loading={mixData.isLoading}
            error={mixData.error}
            onRetry={mixData.refetch}
          >
            <ProductTypeMixChart
              data={mixData.data?.buckets ?? []}
            />
          </ChartCard>
        </Col>
        <Col span={24}>
          <ChartCard
            title="Zombie Topic Candidates"
            loading={compositionData.isLoading}
            error={compositionData.error}
            onRetry={compositionData.refetch}
          >
            <ZombieTopicsTable data={compositionData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
        <Col span={24}>
          <ChartCard
            title="Pivoted Cost Breakdown"
            loading={compositionData.isLoading}
            error={compositionData.error}
            onRetry={compositionData.refetch}
          >
            <PivotedCostBreakdown data={compositionData.data?.buckets ?? []} />
          </ChartCard>
        </Col>
      </Row>
    </div>
  );
}
