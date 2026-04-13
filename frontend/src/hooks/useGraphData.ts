import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";
import type { GraphNode, GraphEdge } from "../components/explorer/renderers/types";

interface GraphResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface UseGraphDataParams {
  tenantName: string | null;
  focus: string | null;
  depth?: number;
  at?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  timezone?: string | null;
}

export interface UseGraphDataResult {
  data: { nodes: GraphNode[]; edges: GraphEdge[] } | null;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

export function useGraphData(params: UseGraphDataParams): UseGraphDataResult {
  const { tenantName, focus, depth, at, startDate, endDate, timezone } =
    params;

  const query = useQuery({
    queryKey: [
      "graph",
      tenantName,
      focus ?? null,
      depth ?? 1,
      at ?? null,
      startDate ?? null,
      endDate ?? null,
    ],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      if (focus) qs.set("focus", focus);
      if (depth !== undefined) qs.set("depth", String(depth));
      if (at) qs.set("at", at);
      if (startDate) qs.set("start_date", startDate);
      if (endDate) qs.set("end_date", endDate);
      if (timezone) qs.set("timezone", timezone);

      const qsStr = qs.toString();
      const url = `${API_URL}/tenants/${tenantName}/graph${qsStr ? `?${qsStr}` : ""}`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<GraphResponse>;
    },
    enabled: !!tenantName,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
    refetch: query.refetch,
  };
}
