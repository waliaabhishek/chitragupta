import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface UseGraphDiffParams {
  tenantName: string | null;
  fromStart: string | null;
  fromEnd: string | null;
  toStart: string | null;
  toEnd: string | null;
  focus: string | null;
  depth?: number;
}

export interface GraphDiffNode {
  id: string;
  resource_type: string;
  display_name: string | null;
  parent_id: string | null;
  cost_before: number;
  cost_after: number;
  cost_delta: number;
  pct_change: number | null;
  status: "new" | "deleted" | "changed" | "unchanged";
}

export interface UseGraphDiffResult {
  data: GraphDiffNode[] | null;
  isLoading: boolean;
  error: string | null;
}

interface RawDiffNode {
  id: string;
  resource_type: string;
  display_name: string | null;
  parent_id: string | null;
  cost_before: string;
  cost_after: string;
  cost_delta: string;
  pct_change: string | null;
  status: "new" | "deleted" | "changed" | "unchanged";
}

export function useGraphDiff(params: UseGraphDiffParams): UseGraphDiffResult {
  const {
    tenantName,
    fromStart,
    fromEnd,
    toStart,
    toEnd,
    focus,
    depth = 1,
  } = params;

  const enabled =
    !!tenantName &&
    fromStart !== null &&
    fromEnd !== null &&
    toStart !== null &&
    toEnd !== null;

  const query = useQuery({
    queryKey: [
      "graph-diff",
      tenantName,
      fromStart,
      fromEnd,
      toStart,
      toEnd,
      focus ?? null,
      depth ?? 1,
    ],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      qs.set("from_start", fromStart!);
      qs.set("from_end", fromEnd!);
      qs.set("to_start", toStart!);
      qs.set("to_end", toEnd!);
      qs.set("depth", String(depth));
      if (focus) qs.set("focus", focus);

      const url = `${API_URL}/tenants/${tenantName}/graph/diff?${qs.toString()}`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      const raw = (await response.json()) as RawDiffNode[];
      return raw.map(
        (n): GraphDiffNode => ({
          ...n,
          cost_before: parseFloat(n.cost_before),
          cost_after: parseFloat(n.cost_after),
          cost_delta: parseFloat(n.cost_delta),
          pct_change: n.pct_change !== null ? parseFloat(n.pct_change) : null,
        }),
      );
    },
    enabled,
    placeholderData: keepPreviousData,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
  };
}
