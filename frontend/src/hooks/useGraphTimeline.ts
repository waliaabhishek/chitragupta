import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";

export interface UseGraphTimelineParams {
  tenantName: string | null;
  entityId: string | null;
  startDate: string | null;
  endDate: string | null;
}

export interface TimelinePoint {
  date: string;
  cost: number;
}

export interface UseGraphTimelineResult {
  data: TimelinePoint[] | null;
  isLoading: boolean;
  error: string | null;
}

export function useGraphTimeline(
  params: UseGraphTimelineParams,
): UseGraphTimelineResult {
  const { tenantName, entityId, startDate, endDate } = params;

  const enabled =
    !!tenantName && entityId !== null && startDate !== null && endDate !== null;

  const query = useQuery({
    queryKey: ["graph-timeline", tenantName, entityId, startDate, endDate],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams();
      qs.set("entity_id", entityId!);
      qs.set("start", startDate!);
      qs.set("end", endDate!);

      const url = `${API_URL}/tenants/${tenantName}/graph/timeline?${qs.toString()}`;
      const response = await fetch(url, { signal });
      if (!response.ok)
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      return response.json() as Promise<TimelinePoint[]>;
    },
    enabled,
  });

  return {
    data: query.data ?? null,
    isLoading: query.isLoading,
    error: query.error?.message ?? null,
  };
}
