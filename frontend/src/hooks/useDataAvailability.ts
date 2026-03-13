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
    let cancelled = false;
    setIsLoading(true);
    setError(null);

    const url = `${API_URL}/tenants/${tenantName}/chargebacks/dates`;

    fetch(url)
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        return response.json() as Promise<ChargebackDatesResponse>;
      })
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setIsLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to fetch data availability");
          setIsLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [tenantName, refetchKey]);

  return { data, isLoading, error, refetch };
}
