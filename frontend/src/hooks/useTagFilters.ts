import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import type { TagFilters } from "../types/filters";

const FILTER_KEYS: (keyof TagFilters)[] = ["tag_key", "entity_type"];

interface UseTagFiltersReturn {
  filters: TagFilters;
  setFilter: (key: keyof TagFilters, value: string | null) => void;
  resetFilters: () => void;
  queryParams: Record<string, string>;
}

export function useTagFilters(): UseTagFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  const filters: TagFilters = useMemo(
    () => ({
      tag_key: searchParams.get("tag_key"),
      entity_type: searchParams.get("entity_type"),
    }),
    [searchParams],
  );

  const setFilter = useCallback(
    (key: keyof TagFilters, value: string | null) => {
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
