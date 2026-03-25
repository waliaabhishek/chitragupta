import { useQuery } from "@tanstack/react-query";
import { API_URL } from "../config";
import type {
  AggregationResponse,
  IdentityResponse,
  PaginatedResponse,
  ResourceResponse,
} from "../types/api";

export interface SelectOption {
  label: string;
  value: string;
}

export interface UseFilterOptionsResult {
  identityOptions: SelectOption[];
  resourceOptions: SelectOption[];
  productTypeOptions: SelectOption[];
  isLoading: boolean;
  error: string | null;
}

export function useFilterOptions(
  tenantName: string,
  startDate: string | null,
  endDate: string | null,
): UseFilterOptionsResult {
  const identityResourceQuery = useQuery({
    queryKey: ["filterOptions", "identityResource", tenantName],
    queryFn: async ({ signal }) => {
      const identityUrl = `${API_URL}/tenants/${tenantName}/identities?page_size=1000`;
      const resourceUrl = `${API_URL}/tenants/${tenantName}/resources?page_size=1000`;

      const [identities, resources] = await Promise.all([
        fetch(identityUrl, { signal }).then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json() as Promise<PaginatedResponse<IdentityResponse>>;
        }),
        fetch(resourceUrl, { signal }).then((r) => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return r.json() as Promise<PaginatedResponse<ResourceResponse>>;
        }),
      ]);

      return {
        identityOptions: identities.items.map((i) => ({
          label: i.display_name ? `${i.display_name} (${i.identity_id})` : i.identity_id,
          value: i.identity_id,
        })),
        resourceOptions: resources.items.map((r) => ({
          label: r.display_name ? `${r.display_name} (${r.resource_id})` : r.resource_id,
          value: r.resource_id,
        })),
      };
    },
    enabled: !!tenantName,
  });

  const productTypeQuery = useQuery({
    queryKey: ["filterOptions", "productType", tenantName, startDate, endDate],
    queryFn: async ({ signal }) => {
      const qs = new URLSearchParams({ group_by: "product_type", time_bucket: "day" });
      if (startDate) qs.set("start_date", startDate);
      if (endDate) qs.set("end_date", endDate);
      const productTypeUrl = `${API_URL}/tenants/${tenantName}/chargebacks/aggregate?${qs.toString()}`;

      const response = await fetch(productTypeUrl, { signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const aggregation = (await response.json()) as AggregationResponse;

      const seen = new Set<string>();
      const ptOptions: SelectOption[] = [];
      for (const bucket of aggregation.buckets) {
        const pt = bucket.dimensions["product_type"];
        if (pt && !seen.has(pt)) {
          seen.add(pt);
          ptOptions.push({ label: pt, value: pt });
        }
      }
      return ptOptions;
    },
    enabled: !!tenantName,
  });

  return {
    identityOptions: identityResourceQuery.data?.identityOptions ?? [],
    resourceOptions: identityResourceQuery.data?.resourceOptions ?? [],
    productTypeOptions: productTypeQuery.data ?? [],
    isLoading: identityResourceQuery.isLoading || productTypeQuery.isLoading,
    error: identityResourceQuery.error?.message ?? productTypeQuery.error?.message ?? null,
  };
}
