import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import type { TopicAttributionFilters } from "../types/filters";
import {
  todayStr,
  thirtyDaysAgoStr,
  loadDatesFromStorage,
  saveDatesToStorage,
  clearDatesFromStorage,
  loadTimezoneFromStorage,
  saveTimezoneToStorage,
  clearTimezoneFromStorage,
} from "../utils/dateFilterStorage";

const FILTER_KEYS: (keyof TopicAttributionFilters)[] = [
  "start_date",
  "end_date",
  "cluster_resource_id",
  "topic_name",
  "product_type",
  "attribution_method",
  "timezone",
];

const DATE_STORAGE_KEY = "topic_attribution_date_range";
const DATE_FIELDS: (keyof TopicAttributionFilters)[] = [
  "start_date",
  "end_date",
];
const BROWSER_TIMEZONE = Intl.DateTimeFormat().resolvedOptions().timeZone;

interface UseTopicAttributionFiltersReturn {
  filters: TopicAttributionFilters;
  setFilter: (key: keyof TopicAttributionFilters, value: string | null) => void;
  setFilters: (updates: Partial<TopicAttributionFilters>) => void;
  resetFilters: () => void;
  queryParams: Record<string, string>;
}

export function useTopicAttributionFilters(): UseTopicAttributionFiltersReturn {
  const [searchParams, setSearchParams] = useSearchParams();

  const storedDates = useMemo(
    () => loadDatesFromStorage(DATE_STORAGE_KEY),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- searchParams triggers re-read of localStorage when URL changes
    [searchParams],
  );
  const storedTimezone = useMemo(
    () => loadTimezoneFromStorage(),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- searchParams triggers re-read of localStorage when URL changes
    [searchParams],
  );

  const filters: TopicAttributionFilters = useMemo(
    () => ({
      start_date:
        searchParams.get("start_date") ??
        storedDates.start_date ??
        thirtyDaysAgoStr(),
      end_date:
        searchParams.get("end_date") ?? storedDates.end_date ?? todayStr(),
      cluster_resource_id: searchParams.get("cluster_resource_id"),
      topic_name: searchParams.get("topic_name"),
      product_type: searchParams.get("product_type"),
      attribution_method: searchParams.get("attribution_method"),
      timezone:
        searchParams.get("timezone") ?? storedTimezone ?? BROWSER_TIMEZONE,
    }),
    [
      searchParams,
      storedDates.start_date,
      storedDates.end_date,
      storedTimezone,
    ],
  );

  const setFilter = useCallback(
    (key: keyof TopicAttributionFilters, value: string | null) => {
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
        saveDatesToStorage(
          DATE_STORAGE_KEY,
          updated.start_date,
          updated.end_date,
        );
      }
      if (key === "timezone") {
        if (value === null || value === "") {
          clearTimezoneFromStorage();
        } else {
          saveTimezoneToStorage(value);
        }
      }
    },
    [setSearchParams],
  );

  const setFilters = useCallback(
    (updates: Partial<TopicAttributionFilters>) => {
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
      if (DATE_FIELDS.some((f) => f in updates)) {
        const current = loadDatesFromStorage(DATE_STORAGE_KEY);
        saveDatesToStorage(
          DATE_STORAGE_KEY,
          "start_date" in updates
            ? (updates.start_date ?? null)
            : current.start_date,
          "end_date" in updates ? (updates.end_date ?? null) : current.end_date,
        );
      }
      if ("timezone" in updates) {
        const tz = updates.timezone;
        if (tz === null || tz === undefined || tz === "") {
          clearTimezoneFromStorage();
        } else {
          saveTimezoneToStorage(tz);
        }
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
    clearTimezoneFromStorage();
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

  return { filters, setFilter, setFilters, resetFilters, queryParams };
}
