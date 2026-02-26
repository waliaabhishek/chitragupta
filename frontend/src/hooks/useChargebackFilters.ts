import { useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import type { ChargebackFilters } from "../types/filters";

const FILTER_KEYS: (keyof ChargebackFilters)[] = [
  "start_date",
  "end_date",
  "identity_id",
  "product_type",
  "resource_id",
  "cost_type",
];

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}

function thirtyDaysAgoStr(): string {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return d.toISOString().slice(0, 10);
}

interface UseChargebackFiltersReturn {
  filters: ChargebackFilters;
  setFilter: (key: keyof ChargebackFilters, value: string | null) => void;
  resetFilters: () => void;
  toQueryParams: () => Record<string, string>;
}

export function useChargebackFilters(): UseChargebackFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  const filters: ChargebackFilters = {
    start_date: searchParams.get("start_date") ?? thirtyDaysAgoStr(),
    end_date: searchParams.get("end_date") ?? todayStr(),
    identity_id: searchParams.get("identity_id"),
    product_type: searchParams.get("product_type"),
    resource_id: searchParams.get("resource_id"),
    cost_type: searchParams.get("cost_type"),
  };

  const setFilter = useCallback(
    (key: keyof ChargebackFilters, value: string | null) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (value === null || value === "") {
            next.delete(key);
          } else {
            next.set(key, value);
          }
          return next;
        },
        { replace: true },
      );
    },
    [setSearchParams],
  );

  const resetFilters = useCallback(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        for (const key of FILTER_KEYS) {
          next.delete(key);
        }
        return next;
      },
      { replace: true },
    );
  }, [setSearchParams]);

  const toQueryParams = (): Record<string, string> => {
    const result: Record<string, string> = {};
    for (const key of FILTER_KEYS) {
      const val = filters[key];
      if (val !== null) {
        result[key] = val;
      }
    }
    return result;
  };

  return { filters, setFilter, resetFilters, toQueryParams };
}
