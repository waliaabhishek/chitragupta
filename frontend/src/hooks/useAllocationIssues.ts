import { useCallback, useEffect, useState } from "react";
import { API_URL } from "../config";
import type { AllocationIssueResponse, PaginatedResponse } from "../types/api";
import type { ChargebackFilters } from "../types/filters";

export interface UseAllocationIssuesParams {
  tenantName: string;
  filters: ChargebackFilters;
  page: number;
  pageSize: number;
}

export interface UseAllocationIssuesResult {
  data: PaginatedResponse<AllocationIssueResponse> | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useAllocationIssues(params: UseAllocationIssuesParams): UseAllocationIssuesResult {
  const { tenantName, filters, page, pageSize } = params;

  const [data, setData] = useState<PaginatedResponse<AllocationIssueResponse> | null>(null);
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
    qs.set("page", String(page));
    qs.set("page_size", String(pageSize));
    if (filters.start_date) qs.set("start_date", filters.start_date);
    if (filters.end_date) qs.set("end_date", filters.end_date);
    if (filters.identity_id) qs.set("identity_id", filters.identity_id);
    if (filters.product_type) qs.set("product_type", filters.product_type);
    if (filters.resource_id) qs.set("resource_id", filters.resource_id);

    const url = `${API_URL}/tenants/${tenantName}/chargebacks/allocation-issues?${qs.toString()}`;

    fetch(url, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return response.json() as Promise<PaginatedResponse<AllocationIssueResponse>>;
      })
      .then((result) => {
        setData(result);
        setIsLoading(false);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to fetch allocation issues");
        setIsLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [tenantName, filters, page, pageSize, refetchKey]);

  return { data, isLoading, error, refetch };
}
