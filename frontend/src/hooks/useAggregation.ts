import { useCallback, useEffect, useState } from "react";
import { API_URL } from "../config";
import type { AggregationResponse } from "../types/api";

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
}

export interface UseAggregationResult {
  data: AggregationResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useAggregation(params: UseAggregationParams): UseAggregationResult {
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
  } = params;

  // Serialize array for stable dependency tracking
  const groupByKey = groupBy.join(",");

  const [data, setData] = useState<AggregationResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refetchKey, setRefetchKey] = useState(0);

  const refetch = useCallback(() => {
    setRefetchKey((k) => k + 1);
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setIsLoading(true);
    setError(null);

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

    const url = `${API_URL}/tenants/${tenantName}/chargebacks/aggregate?${qs.toString()}`;

    fetch(url, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return response.json() as Promise<AggregationResponse>;
      })
      .then((result) => {
        setData(result);
        setIsLoading(false);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to fetch aggregation data");
        setIsLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [
    tenantName,
    groupByKey,
    timeBucket,
    startDate,
    endDate,
    identityId,
    productType,
    resourceId,
    costType,
    refetchKey,
  ]);

  return { data, isLoading, error, refetch };
}
