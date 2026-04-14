import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface GraphSearchResult {
  id: string;
  resource_type: string;
  display_name: string | null;
  parent_id: string | null;
  parent_display_name: string | null;
  status: string;
}

interface UseGraphSearchParams {
  tenantName: string | null;
  query: string;
}

interface UseGraphSearchResult {
  results: GraphSearchResult[];
  isLoading: boolean;
  error: string | null;
}

export function useGraphSearch({
  tenantName,
  query,
}: UseGraphSearchParams): UseGraphSearchResult {
  const q = useQuery({
    queryKey: ["graph-search", tenantName, query],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams({ q: query });
      const response = await fetch(
        `${API_URL}/tenants/${tenantName}/graph/search?${qs.toString()}`,
        { signal },
      );
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const json = (await response.json()) as { results: GraphSearchResult[] };
      return json.results;
    },
    enabled: !!tenantName && query.length >= 1,
  });

  return {
    results: q.data ?? [],
    isLoading: q.isLoading,
    error: q.error?.message ?? null,
  };
}
