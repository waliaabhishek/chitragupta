/** Structural minimum required by aggregation utilities. */
export interface BucketLike {
  dimensions: Record<string, string>;
  time_bucket: string;
  total_amount: string;
  row_count?: number;
}

/** Aggregate buckets by time_bucket: sum all dimension amounts per time period. */
export function aggregateByTime(
  buckets: BucketLike[],
): { time: string; amount: number }[] {
  const map = new Map<string, number>();
  for (const b of buckets) {
    map.set(
      b.time_bucket,
      (map.get(b.time_bucket) ?? 0) + parseFloat(b.total_amount),
    );
  }
  return Array.from(map.entries())
    .map(([time, amount]) => ({ time, amount }))
    .sort((a, b) => a.time.localeCompare(b.time));
}

/** Aggregate buckets by a dimension value: sum across time buckets. */
export function aggregateByDimension(
  buckets: BucketLike[],
  dimension: string,
): { key: string; amount: number }[] {
  const map = new Map<string, number>();
  for (const b of buckets) {
    const key = b.dimensions[dimension] ?? "Unknown";
    map.set(key, (map.get(key) ?? 0) + parseFloat(b.total_amount));
  }
  return Array.from(map.entries()).map(([key, amount]) => ({ key, amount }));
}

/**
 * Sort by amount desc, take top N, sum remainder into an "Other" bucket.
 * If all items fit within topN, no "Other" bucket is added.
 */
export function topNWithOther(
  items: { key: string; amount: number }[],
  n: number,
): { key: string; amount: number }[] {
  const sorted = [...items].sort((a, b) => b.amount - a.amount);
  if (sorted.length <= n) return sorted;
  const top = sorted.slice(0, n);
  const otherAmount = sorted.slice(n).reduce((sum, d) => sum + d.amount, 0);
  return [...top, { key: "Other", amount: otherAmount }];
}

/** Format a number as USD currency string. */
export function formatCurrency(amount: number): string {
  return `$${amount.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function appendTagFilters(
  qs: URLSearchParams,
  tagFilters: Record<string, string[]>,
): void {
  for (const [key, values] of Object.entries(tagFilters)) {
    for (const val of values) {
      qs.append(`tag:${key}`, val);
    }
  }
}
