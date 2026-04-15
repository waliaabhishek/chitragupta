import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface UseDateRangeParams {
  tenantName: string | null;
}

export interface UseDateRangeResult {
  minDate: string | null;
  maxDate: string | null;
  isLoading: boolean;
}

interface ApiNode {
  created_at: string | null;
  deleted_at: string | null;
  [key: string]: unknown;
}

interface ChargebackDatesResponse {
  dates: string[];
}

export function useDateRange({
  tenantName,
}: UseDateRangeParams): UseDateRangeResult {
  const graphQuery = useQuery({
    queryKey: ["date-range", tenantName],
    queryFn: async ({ signal }) => {
      const url = `${API_URL}/tenants/${tenantName}/graph`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<{ nodes: ApiNode[]; edges: unknown[] }>;
    },
    enabled: !!tenantName,
  });

  const datesQuery = useQuery({
    queryKey: ["chargeback-dates", tenantName],
    queryFn: async ({ signal }) => {
      const url = `${API_URL}/tenants/${tenantName}/chargebacks/dates`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<ChargebackDatesResponse>;
    },
    enabled: !!tenantName,
  });

  if (!graphQuery.data || !datesQuery.data) {
    return { minDate: null, maxDate: null, isLoading: graphQuery.isLoading || datesQuery.isLoading };
  }

  const nodes = graphQuery.data.nodes;
  const createdDates = nodes
    .filter((n) => n.created_at !== null)
    .map((n) => n.created_at!.split("T")[0]);

  if (createdDates.length === 0) {
    return { minDate: null, maxDate: null, isLoading: false };
  }

  const minDate = createdDates.reduce((a, b) => (a < b ? a : b));

  const chargebackDates = datesQuery.data.dates;
  const maxDate = chargebackDates.length > 0
    ? chargebackDates[chargebackDates.length - 1]
    : minDate;

  return { minDate, maxDate, isLoading: false };
}
