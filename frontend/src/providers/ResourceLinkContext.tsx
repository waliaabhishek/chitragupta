import type React from "react";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { API_URL } from "../config";
import { useTenant } from "./TenantContext";
import {
  environmentUrl,
  clusterUrl,
  schemaRegistryUrl,
  serviceAccountUrl,
  connectorUrl,
} from "../config/confluentCloudUrls";

const STORAGE_KEY = "chargeback_deep_links_enabled";

type ResourceEntry = { resource_type: string; parent_id: string | null };
type ResourceIndex = Record<string, ResourceEntry>;

interface ResourceLinkContextValue {
  resolveUrl: (resourceId: string) => string | null;
  enabled: boolean;
  setEnabled: (enabled: boolean) => void;
  isLoading: boolean;
}

const ResourceLinkContext = createContext<ResourceLinkContextValue | null>(
  null,
);

function getInitialEnabled(): boolean {
  return localStorage.getItem(STORAGE_KEY) === "true";
}

function resolveFromEntry(
  resourceId: string,
  entry: ResourceEntry,
  index: ResourceIndex,
): string | null {
  switch (entry.resource_type) {
    case "environment":
      return environmentUrl(resourceId);
    case "kafka_cluster": {
      if (!entry.parent_id) return null;
      return clusterUrl(entry.parent_id, resourceId);
    }
    case "schema_registry": {
      if (!entry.parent_id) return null;
      return schemaRegistryUrl(entry.parent_id, resourceId);
    }
    case "service_account":
      return serviceAccountUrl();
    case "connector": {
      if (!entry.parent_id) return null;
      const clusterEntry = index[entry.parent_id];
      if (!clusterEntry?.parent_id) return null;
      return connectorUrl(clusterEntry.parent_id, entry.parent_id, resourceId);
    }
    default:
      return null;
  }
}

interface ResourceLinkProviderProps {
  children: ReactNode;
}

export function ResourceLinkProvider({
  children,
}: ResourceLinkProviderProps): React.JSX.Element {
  const { currentTenant } = useTenant();
  const [enabled, setEnabledState] = useState<boolean>(getInitialEnabled);
  const [index, setIndex] = useState<ResourceIndex>({});
  const [isLoading, setIsLoading] = useState(false);

  const setEnabled = useCallback((value: boolean) => {
    localStorage.setItem(STORAGE_KEY, String(value));
    setEnabledState(value);
  }, []);

  useEffect(() => {
    if (!enabled || !currentTenant) return;

    const tenantName = currentTenant.tenant_name;
    const controller = new AbortController();

    async function fetchResources(): Promise<void> {
      setIsLoading(true);
      try {
        const newIndex: ResourceIndex = {};
        let page = 1;
        let hasMore = true;

        while (hasMore) {
          const url = `${API_URL}/tenants/${tenantName}/resources?page=${page}&page_size=100`;
          const res = await fetch(url, { signal: controller.signal });
          if (!res.ok) break;
          const data = (await res.json()) as {
            items: Array<{
              resource_id: string;
              resource_type: string;
              parent_id: string | null;
              deleted_at: string | null;
            }>;
            page: number;
            pages: number;
          };
          for (const r of data.items) {
            if (r.deleted_at) continue;
            newIndex[r.resource_id] = {
              resource_type: r.resource_type,
              parent_id: r.parent_id,
            };
          }
          hasMore = data.page < data.pages;
          page++;
        }

        if (!controller.signal.aborted) {
          setIndex(newIndex);
        }
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") return;
      } finally {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      }
    }

    void fetchResources();
    return () => controller.abort();
  }, [enabled, currentTenant]);

  const resolveUrl = useCallback(
    (resourceId: string): string | null => {
      if (!enabled) return null;

      const entry = index[resourceId];
      if (entry) {
        return resolveFromEntry(resourceId, entry, index);
      }

      // Prefix fallbacks for IDs not in index
      if (resourceId.startsWith("sa-")) return serviceAccountUrl();
      if (resourceId.startsWith("env-")) return environmentUrl(resourceId);
      // lkc-, lsrc- need parent context — cannot resolve without index
      return null;
    },
    [enabled, index],
  );

  const value = useMemo<ResourceLinkContextValue>(
    () => ({ resolveUrl, enabled, setEnabled, isLoading }),
    [resolveUrl, enabled, setEnabled, isLoading],
  );

  return (
    <ResourceLinkContext.Provider value={value}>
      {children}
    </ResourceLinkContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useResourceLinks(): ResourceLinkContextValue {
  const ctx = useContext(ResourceLinkContext);
  if (!ctx) {
    throw new Error(
      "useResourceLinks must be used within ResourceLinkProvider",
    );
  }
  return ctx;
}
