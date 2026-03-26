import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import type { ResourceFilters } from "../types/filters";

const FILTER_KEYS: (keyof ResourceFilters)[] = [
  "search",
  "resource_type",
  "status",
  "tag_key",
  "tag_value",
];

interface UseResourceFiltersReturn {
  filters: ResourceFilters;
  setFilter: (key: keyof ResourceFilters, value: string | null) => void;
  resetFilters: () => void;
  queryParams: Record<string, string>;
}

export function useResourceFilters(): UseResourceFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  const filters: ResourceFilters = useMemo(
    () => ({
      search: searchParams.get("search"),
      resource_type: searchParams.get("resource_type"),
      status: searchParams.get("status"),
      tag_key: searchParams.get("tag_key"),
      tag_value: searchParams.get("tag_value"),
    }),
    [searchParams],
  );

  const setFilter = useCallback(
    (key: keyof ResourceFilters, value: string | null) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (value === null || value === "") next.delete(key);
          else next.set(key, value);
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
        for (const key of FILTER_KEYS) next.delete(key);
        return next;
      },
      { replace: true },
    );
  }, [setSearchParams]);

  const queryParams: Record<string, string> = useMemo(() => {
    const result: Record<string, string> = {};
    for (const key of FILTER_KEYS) {
      const val = filters[key];
      if (val !== null) result[key] = val;
    }
    return result;
  }, [filters]);

  return { filters, setFilter, resetFilters, queryParams };
}
