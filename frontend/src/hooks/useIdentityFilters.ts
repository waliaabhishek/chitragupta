import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import type { IdentityFilters } from "../types/filters";

const FILTER_KEYS: (keyof IdentityFilters)[] = [
  "search",
  "identity_type",
  "tag_key",
  "tag_value",
];

interface UseIdentityFiltersReturn {
  filters: IdentityFilters;
  setFilter: (key: keyof IdentityFilters, value: string | null) => void;
  resetFilters: () => void;
  queryParams: Record<string, string>;
}

export function useIdentityFilters(): UseIdentityFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  const filters: IdentityFilters = useMemo(
    () => ({
      search: searchParams.get("search"),
      identity_type: searchParams.get("identity_type"),
      tag_key: searchParams.get("tag_key"),
      tag_value: searchParams.get("tag_value"),
    }),
    [searchParams],
  );

  const setFilter = useCallback(
    (key: keyof IdentityFilters, value: string | null) => {
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
