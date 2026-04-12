import { useQuery } from "@tanstack/react-query";
import { fetchTopicAttributionAggregation } from "../api/topicAttributions";
import type { TopicAttributionAggregationResponse } from "../types/api";

export interface UseTopicAttributionAggregationParams {
  tenantName: string;
  groupBy: string[];
  timeBucket: "day" | "week" | "month";
  startDate: string;
  endDate: string;
  clusterResourceId?: string | null;
  topicName?: string | null;
  productType?: string | null;
  timezone?: string | null;
  tagFilters?: Record<string, string[]>;
}

export interface UseTopicAttributionAggregationResult {
  data: TopicAttributionAggregationResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useTopicAttributionAggregation(
  params: UseTopicAttributionAggregationParams,
): UseTopicAttributionAggregationResult {
  const {
    tenantName,
    groupBy,
    timeBucket,
    startDate,
    endDate,
    clusterResourceId,
    topicName,
    productType,
    timezone,
    tagFilters,
  } = params;

  const groupByKey = groupBy.join(",");

  const query = useQuery({
    queryKey: [
      "topic-attribution-aggregation",
      tenantName,
      groupByKey,
      timeBucket,
      startDate,
      endDate,
      clusterResourceId ?? null,
      topicName ?? null,
      productType ?? null,
      timezone ?? null,
      JSON.stringify(tagFilters ?? null),
    ],
    queryFn: ({ signal }) =>
      fetchTopicAttributionAggregation(
        tenantName,
        {
          group_by: groupBy,
          time_bucket: timeBucket,
          start_date: startDate,
          end_date: endDate,
          cluster_resource_id: clusterResourceId ?? undefined,
          topic_name: topicName ?? undefined,
          product_type: productType ?? undefined,
          timezone: timezone ?? undefined,
          tag_filters: tagFilters,
        },
        signal,
      ),
    enabled: !!tenantName && !!startDate && !!endDate,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
    refetch: query.refetch,
  };
}
