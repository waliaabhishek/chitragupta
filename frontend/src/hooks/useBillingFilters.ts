import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import type { BillingFilters } from "../types/filters";
import {
  todayStr,
  thirtyDaysAgoStr,
  loadDatesFromStorage,
  saveDatesToStorage,
  clearDatesFromStorage,
} from "../utils/dateFilterStorage";

const FILTER_KEYS: (keyof BillingFilters)[] = [
  "start_date",
  "end_date",
  "product_type",
  "resource_id",
];

const DATE_STORAGE_KEY = "billing_date_range";
const DATE_FIELDS: (keyof BillingFilters)[] = ["start_date", "end_date"];

interface UseBillingFiltersReturn {
  filters: BillingFilters;
  setFilter: (key: keyof BillingFilters, value: string | null) => void;
  setFilters: (updates: Partial<BillingFilters>) => void;
  resetFilters: () => void;
  toQueryParams: () => Record<string, string>;
  queryParams: Record<string, string>;
}

export function useBillingFilters(): UseBillingFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();
  // eslint-disable-next-line react-hooks/exhaustive-deps -- searchParams triggers re-read of localStorage when URL changes
  const storedDates = useMemo(() => loadDatesFromStorage(DATE_STORAGE_KEY), [searchParams]);
  const spStartDate = searchParams.get("start_date");
  const spEndDate = searchParams.get("end_date");
  const spProductType = searchParams.get("product_type");
  const spResourceId = searchParams.get("resource_id");

  const filters: BillingFilters = useMemo(
    () => ({
      start_date: spStartDate ?? storedDates.start_date ?? thirtyDaysAgoStr(),
      end_date: spEndDate ?? storedDates.end_date ?? todayStr(),
      product_type: spProductType,
      resource_id: spResourceId,
    }),
    [spStartDate, spEndDate, spProductType, spResourceId, storedDates.start_date, storedDates.end_date],
  );

  const setFilter = useCallback(
    (key: keyof BillingFilters, value: string | null) => {
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
      if (DATE_FIELDS.includes(key)) {
        const current = loadDatesFromStorage(DATE_STORAGE_KEY);
        const stored = value === null || value === "" ? null : value;
        const updated = { ...current, [key]: stored };
        saveDatesToStorage(DATE_STORAGE_KEY, updated.start_date, updated.end_date);
      }
    },
    [setSearchParams],
  );

  const setFilters = useCallback(
    (updates: Partial<BillingFilters>) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          for (const [key, value] of Object.entries(updates)) {
            if (value === null || value === undefined || value === "") {
              next.delete(key);
            } else {
              next.set(key, value);
            }
          }
          return next;
        },
        { replace: true },
      );
      const hasDateUpdate = DATE_FIELDS.some((f) => f in updates);
      if (hasDateUpdate) {
        const current = loadDatesFromStorage(DATE_STORAGE_KEY);
        const updated = {
          start_date: "start_date" in updates ? (updates.start_date ?? null) : current.start_date,
          end_date: "end_date" in updates ? (updates.end_date ?? null) : current.end_date,
        };
        saveDatesToStorage(DATE_STORAGE_KEY, updated.start_date, updated.end_date);
      }
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
    clearDatesFromStorage(DATE_STORAGE_KEY);
  }, [setSearchParams]);

  const queryParams: Record<string, string> = useMemo(() => {
    const result: Record<string, string> = {};
    for (const key of FILTER_KEYS) {
      const val = filters[key];
      if (val !== null) {
        result[key] = val;
      }
    }
    return result;
  }, [filters]);

  const toQueryParams = useCallback((): Record<string, string> => queryParams, [queryParams]);

  return { filters, setFilter, setFilters, resetFilters, toQueryParams, queryParams };
}
