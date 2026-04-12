import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface UseTagValuesResult {
  data: string[];
  isLoading: boolean;
  error: string | null;
}

export function useTagValues(
  tenantName: string,
  tagKey: string,
  prefix?: string,
): UseTagValuesResult {
  const query = useQuery({
    queryKey: ["tag-values", tenantName, tagKey, prefix ?? ""],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      if (prefix) qs.set("q", prefix);
      const response = await fetch(
        `${API_URL}/tenants/${tenantName}/tags/keys/${tagKey}/values?${qs.toString()}`,
        { signal },
      );
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      const json = (await response.json()) as { values: string[] };
      return json.values;
    },
    enabled: !!tenantName && !!tagKey,
  });

  return {
    data: query.data ?? [],
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
  };
}
