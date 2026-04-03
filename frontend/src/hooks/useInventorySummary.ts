import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";
import type { InventorySummaryResponse } from "../types/api";

export interface UseInventorySummaryParams {
  tenantName: string;
}

export interface UseInventorySummaryResult {
  data: InventorySummaryResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useInventorySummary(
  params: UseInventorySummaryParams,
): UseInventorySummaryResult {
  const { tenantName } = params;

  const query = useQuery({
    queryKey: ["inventorySummary", tenantName],
    queryFn: async ({ signal }) => {
      const url = `${API_URL}/tenants/${tenantName}/inventory/summary`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<InventorySummaryResponse>;
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
