import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface UseTagKeysResult {
  data: string[];
  isLoading: boolean;
  error: string | null;
}

export function useTagKeys(tenantName: string): UseTagKeysResult {
  const query = useQuery({
    queryKey: ["tag-keys", tenantName],
    queryFn: async ({ signal }) => {
      const response = await fetch(
        `${API_URL}/tenants/${tenantName}/tags/keys`,
        { signal },
      );
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      const json = (await response.json()) as { keys: string[] };
      return json.keys;
    },
    enabled: !!tenantName,
  });

  return {
    data: query.data ?? [],
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
  };
}
