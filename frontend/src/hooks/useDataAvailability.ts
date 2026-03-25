import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";
import type { ChargebackDatesResponse } from "../types/api";

export interface UseDataAvailabilityParams {
  tenantName: string;
}

export interface UseDataAvailabilityResult {
  data: ChargebackDatesResponse | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useDataAvailability(params: UseDataAvailabilityParams): UseDataAvailabilityResult {
  const { tenantName } = params;

  const query = useQuery({
    queryKey: ["dataAvailability", tenantName],
    queryFn: async ({ signal }) => {
      const url = `${API_URL}/tenants/${tenantName}/chargebacks/dates`;
      const response = await fetch(url, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<ChargebackDatesResponse>;
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
