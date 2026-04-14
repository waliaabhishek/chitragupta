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

export function useDateRange({
  tenantName,
}: UseDateRangeParams): UseDateRangeResult {
  const query = useQuery({
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

  if (!query.data) {
    return { minDate: null, maxDate: null, isLoading: query.isLoading };
  }

  const nodes = query.data.nodes;
  const createdDates = nodes
    .filter((n) => n.created_at !== null)
    .map((n) => n.created_at!.split("T")[0]);

  if (createdDates.length === 0) {
    return { minDate: null, maxDate: null, isLoading: false };
  }

  const minDate = createdDates.reduce((a, b) => (a < b ? a : b));

  const today = new Date().toISOString().split("T")[0];
  const deletedDates = nodes
    .filter((n) => n.deleted_at !== null)
    .map((n) => n.deleted_at!.split("T")[0]);

  const maxDate = [today, ...deletedDates].reduce((a, b) => (a > b ? a : b));

  return { minDate, maxDate, isLoading: false };
}
