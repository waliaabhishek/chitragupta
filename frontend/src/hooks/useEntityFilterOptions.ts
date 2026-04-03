import { useMemo } from "react";
import { useInventorySummary } from "./useInventorySummary";
import type { SelectOption } from "./useFilterOptions";

const RESOURCE_STATUS_OPTIONS: SelectOption[] = [
  { label: "Active", value: "active" },
  { label: "Deleted", value: "deleted" },
];

const ENTITY_TYPE_OPTIONS: SelectOption[] = [
  { label: "Identity", value: "identity" },
  { label: "Resource", value: "resource" },
];

interface UseIdentityFilterOptionsResult {
  identityTypeOptions: SelectOption[];
  isLoading: boolean;
}

interface UseResourceFilterOptionsResult {
  resourceTypeOptions: SelectOption[];
  resourceStatusOptions: SelectOption[];
  isLoading: boolean;
}

export function useIdentityFilterOptions(
  tenantName: string,
): UseIdentityFilterOptionsResult {
  const { data, isLoading } = useInventorySummary({ tenantName });
  const identityTypeOptions = useMemo(
    () =>
      Object.keys(data?.identity_counts ?? {}).map((t) => ({
        label: t,
        value: t,
      })),
    [data],
  );
  return { identityTypeOptions, isLoading };
}

export function useResourceFilterOptions(
  tenantName: string,
): UseResourceFilterOptionsResult {
  const { data, isLoading } = useInventorySummary({ tenantName });
  const resourceTypeOptions = useMemo(
    () =>
      Object.keys(data?.resource_counts ?? {}).map((t) => ({
        label: t,
        value: t,
      })),
    [data],
  );
  return {
    resourceTypeOptions,
    resourceStatusOptions: RESOURCE_STATUS_OPTIONS,
    isLoading,
  };
}

export { ENTITY_TYPE_OPTIONS };
