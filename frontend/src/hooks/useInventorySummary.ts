import { useCallback, useEffect, useState } from "react";
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

export function useInventorySummary(params: UseInventorySummaryParams): UseInventorySummaryResult {
  const { tenantName } = params;

  const [data, setData] = useState<InventorySummaryResponse | null>(null);
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

    const url = `${API_URL}/tenants/${tenantName}/inventory/summary`;

    fetch(url, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return response.json() as Promise<InventorySummaryResponse>;
      })
      .then((result) => {
        setData(result);
        setIsLoading(false);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to fetch inventory summary");
        setIsLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [tenantName, refetchKey]);

  return { data, isLoading, error, refetch };
}
