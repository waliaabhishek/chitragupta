import { useCallback, useEffect, useState } from "react";
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

  const [data, setData] = useState<ChargebackDatesResponse | null>(null);
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

    const url = `${API_URL}/tenants/${tenantName}/chargebacks/dates`;

    fetch(url, { signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return response.json() as Promise<ChargebackDatesResponse>;
      })
      .then((result) => {
        setData(result);
        setIsLoading(false);
      })
      .catch((err: unknown) => {
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to fetch data availability");
        setIsLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [tenantName, refetchKey]);

  return { data, isLoading, error, refetch };
}
