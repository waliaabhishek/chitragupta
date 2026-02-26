import type { AggregationBucket } from "../types/api";

/** Aggregate buckets by time_bucket: sum all dimension amounts per time period. */
export function aggregateByTime(
  buckets: AggregationBucket[],
): { time: string; amount: number }[] {
  const map = new Map<string, number>();
  for (const b of buckets) {
    map.set(b.time_bucket, (map.get(b.time_bucket) ?? 0) + parseFloat(b.total_amount));
  }
  return Array.from(map.entries())
    .map(([time, amount]) => ({ time, amount }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

/** Aggregate buckets by a dimension value: sum across time buckets. */
export function aggregateByDimension(
  buckets: AggregationBucket[],
  dimension: string,
): { key: string; amount: number }[] {
  const map = new Map<string, number>();
  for (const b of buckets) {
    const key = b.dimensions[dimension] ?? "Unknown";
    map.set(key, (map.get(key) ?? 0) + parseFloat(b.total_amount));
  }
  return Array.from(map.entries()).map(([key, amount]) => ({ key, amount }));
}

/** Format a number as USD currency string. */
export function formatCurrency(amount: number): string {
  return `$${amount.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}
