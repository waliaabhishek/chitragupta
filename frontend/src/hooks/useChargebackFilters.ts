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

const DATE_STORAGE_KEY = "chargeback_date_range";
const DATE_FIELDS: (keyof ChargebackFilters)[] = ["start_date", "end_date"];

function todayStr(): string {
  return new Date().toISOString().slice(0, 10);
}

function thirtyDaysAgoStr(): string {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return d.toISOString().slice(0, 10);
}

function loadDatesFromStorage(): { start_date: string | null; end_date: string | null } {
  try {
    const raw = localStorage.getItem(DATE_STORAGE_KEY);
    if (!raw) return { start_date: null, end_date: null };
    return JSON.parse(raw) as { start_date: string | null; end_date: string | null };
  } catch {
    return { start_date: null, end_date: null };
  }
}

function saveDatesToStorage(start: string | null, end: string | null): void {
  try {
    localStorage.setItem(DATE_STORAGE_KEY, JSON.stringify({ start_date: start, end_date: end }));
  } catch {
    // localStorage unavailable — silent fail
  }
}

function clearDatesFromStorage(): void {
  try {
    localStorage.removeItem(DATE_STORAGE_KEY);
  } catch {
    // silent
  }
}

interface UseChargebackFiltersReturn {
  filters: ChargebackFilters;
  setFilter: (key: keyof ChargebackFilters, value: string | null) => void;
  setFilters: (updates: Partial<ChargebackFilters>) => void;
  resetFilters: () => void;
  toQueryParams: () => Record<string, string>;
}

export function useChargebackFilters(): UseChargebackFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  // Intentional: called every render — localStorage reads are synchronous & sub-ms;
  // avoids separate useState sync logic since filters derive from searchParams anyway.
  const storedDates = loadDatesFromStorage();

  const filters: ChargebackFilters = {
    start_date:
      searchParams.get("start_date") ?? storedDates.start_date ?? thirtyDaysAgoStr(),
    end_date:
      searchParams.get("end_date") ?? storedDates.end_date ?? todayStr(),
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
      // Persist date fields to localStorage
      if (DATE_FIELDS.includes(key)) {
        const current = loadDatesFromStorage();
        const stored = value === null || value === "" ? null : value;
        const updated = { ...current, [key]: stored };
        saveDatesToStorage(updated.start_date, updated.end_date);
      }
    },
    [setSearchParams],
  );

  const setFilters = useCallback(
    (updates: Partial<ChargebackFilters>) => {
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
      // Persist date fields to localStorage
      const hasDateUpdate = DATE_FIELDS.some((f) => f in updates);
      if (hasDateUpdate) {
        const current = loadDatesFromStorage();
        const updated = {
          start_date:
            "start_date" in updates ? (updates.start_date ?? null) : current.start_date,
          end_date:
            "end_date" in updates ? (updates.end_date ?? null) : current.end_date,
        };
        saveDatesToStorage(updated.start_date, updated.end_date);
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
    clearDatesFromStorage();
  }, [setSearchParams]);

  // eslint-disable-next-line react-hooks/exhaustive-deps -- individual field deps are intentional; `filters` object changes every render
  const toQueryParams = useCallback((): Record<string, string> => {
    const result: Record<string, string> = {};
    for (const key of FILTER_KEYS) {
      const val = filters[key];
      if (val !== null) {
        result[key] = val;
      }
    }
    return result;
  }, [filters.start_date, filters.end_date, filters.identity_id, filters.product_type, filters.resource_id, filters.cost_type]);

  return { filters, setFilter, setFilters, resetFilters, toQueryParams };
}
