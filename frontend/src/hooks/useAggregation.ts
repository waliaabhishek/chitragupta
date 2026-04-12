import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";
import type { AggregationResponse } from "../types/api";
import { appendTagFilters } from "../utils/aggregation";

export interface UseAggregationParams {
  tenantName: string;
  groupBy: string[];
  timeBucket: "day" | "week" | "month";
  startDate: string;
  endDate: string;
  identityId?: string | null;
  productType?: string | null;
  resourceId?: string | null;
  costType?: string | null;
  timezone?: string | null;
  tagFilters?: Record<string, string[]>;
}

export interface UseAggregationResult {
  data: AggregationResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useAggregation(
  params: UseAggregationParams,
): UseAggregationResult {
  const {
    tenantName,
    groupBy,
    timeBucket,
    startDate,
    endDate,
    identityId,
    productType,
    resourceId,
    costType,
    timezone,
    tagFilters,
  } = params;

  const groupByKey = groupBy.join(",");

  const query = useQuery({
    queryKey: [
      "aggregation",
      tenantName,
      groupByKey,
      timeBucket,
      startDate,
      endDate,
      identityId ?? null,
      productType ?? null,
      resourceId ?? null,
      costType ?? null,
      timezone ?? null,
      JSON.stringify(tagFilters ?? null),
    ],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      for (const g of groupByKey.split(",").filter(Boolean)) {
        qs.append("group_by", g);
      }
      qs.set("time_bucket", timeBucket);
      qs.set("start_date", startDate);
      qs.set("end_date", endDate);
      if (identityId) qs.set("identity_id", identityId);
      if (productType) qs.set("product_type", productType);
      if (resourceId) qs.set("resource_id", resourceId);
      if (costType) qs.set("cost_type", costType);
      if (timezone) qs.set("timezone", timezone);
      if (tagFilters) appendTagFilters(qs, tagFilters);

      const url = `${API_URL}/tenants/${tenantName}/chargebacks/aggregate?${qs.toString()}`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<AggregationResponse>;
    },
    enabled: !!tenantName && !!startDate && !!endDate,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
    refetch: query.refetch,
  };
}
