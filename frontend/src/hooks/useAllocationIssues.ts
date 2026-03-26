import { useQuery } from "@tanstack/react-query";
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
  const { start_date, end_date, identity_id, product_type, resource_id, timezone } = filters;

  const query = useQuery({
    queryKey: ["allocationIssues", tenantName, start_date, end_date, identity_id, product_type, resource_id, timezone, page, pageSize],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      qs.set("page", String(page));
      qs.set("page_size", String(pageSize));
      if (start_date) qs.set("start_date", start_date);
      if (end_date) qs.set("end_date", end_date);
      if (identity_id) qs.set("identity_id", identity_id);
      if (product_type) qs.set("product_type", product_type);
      if (resource_id) qs.set("resource_id", resource_id);
      if (timezone) qs.set("timezone", timezone);

      const url = `${API_URL}/tenants/${tenantName}/chargebacks/allocation-issues?${qs.toString()}`;
      const response = await fetch(url, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<PaginatedResponse<AllocationIssueResponse>>;
    },
    enabled: !!tenantName,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
    refetch: query.refetch,
  };
}
