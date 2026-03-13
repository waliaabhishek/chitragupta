import { useEffect, useState } from "react";
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
  const [identityOptions, setIdentityOptions] = useState<SelectOption[]>([]);
  const [resourceOptions, setResourceOptions] = useState<SelectOption[]>([]);
  const [productTypeOptions, setProductTypeOptions] = useState<SelectOption[]>([]);
  const [identitiesLoading, setIdentitiesLoading] = useState(false);
  const [productTypesLoading, setProductTypesLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Effect 1: identity + resource options — only re-fetch when tenant changes
  useEffect(() => {
    if (!tenantName) return;
    let cancelled = false;
    setIdentitiesLoading(true);

    const identityUrl = `${API_URL}/tenants/${tenantName}/identities?page_size=1000`;
    const resourceUrl = `${API_URL}/tenants/${tenantName}/resources?page_size=1000`;

    Promise.all([
      fetch(identityUrl).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<PaginatedResponse<IdentityResponse>>;
      }),
      fetch(resourceUrl).then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<PaginatedResponse<ResourceResponse>>;
      }),
    ])
      .then(([identities, resources]) => {
        if (cancelled) return;
        setIdentityOptions(
          identities.items.map((i) => ({
            label: i.display_name ? `${i.display_name} (${i.identity_id})` : i.identity_id,
            value: i.identity_id,
          })),
        );
        setResourceOptions(
          resources.items.map((r) => ({
            label: r.display_name ? `${r.display_name} (${r.resource_id})` : r.resource_id,
            value: r.resource_id,
          })),
        );
        setIdentitiesLoading(false);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to fetch options");
          setIdentitiesLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [tenantName]);

  // Effect 2: product type options — re-fetch when tenant or date range changes
  useEffect(() => {
    if (!tenantName) return;
    let cancelled = false;
    setProductTypesLoading(true);

    const qs = new URLSearchParams({ group_by: "product_type", time_bucket: "day" });
    if (startDate) qs.set("start_date", startDate);
    if (endDate) qs.set("end_date", endDate);
    const productTypeUrl = `${API_URL}/tenants/${tenantName}/chargebacks/aggregate?${qs.toString()}`;

    fetch(productTypeUrl)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json() as Promise<AggregationResponse>;
      })
      .then((aggregation) => {
        if (cancelled) return;
        const seen = new Set<string>();
        const ptOptions: SelectOption[] = [];
        for (const bucket of aggregation.buckets) {
          const pt = bucket.dimensions["product_type"];
          if (pt && !seen.has(pt)) {
            seen.add(pt);
            ptOptions.push({ label: pt, value: pt });
          }
        }
        setProductTypeOptions(ptOptions);
        setProductTypesLoading(false);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to fetch options");
          setProductTypesLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [tenantName, startDate, endDate]);

  return {
    identityOptions,
    resourceOptions,
    productTypeOptions,
    isLoading: identitiesLoading || productTypesLoading,
    error,
  };
}
