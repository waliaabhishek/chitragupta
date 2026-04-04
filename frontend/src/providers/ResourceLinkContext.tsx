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
  userUrl,
  identityProviderUrl,
  apiKeyUrl,
  flinkComputePoolUrl,
  ksqldbClusterUrl,
} from "../config/confluentCloudUrls";

const STORAGE_KEY = "chargeback_deep_links_enabled";

type ResourceEntry = {
  resource_type: string;
  parent_id: string | null;
  metadata: Record<string, unknown>;
};
type ResourceIndex = Record<string, ResourceEntry>;

type IdentityEntry = { identity_type: string };
type IdentityIndex = Record<string, IdentityEntry>;

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
      return schemaRegistryUrl(entry.parent_id);
    }
    case "service_account":
      return serviceAccountUrl(resourceId);
    case "flink_compute_pool": {
      if (!entry.parent_id) return null;
      return flinkComputePoolUrl(entry.parent_id, resourceId);
    }
    case "ksqldb_cluster": {
      if (!entry.parent_id) return null;
      const kafkaClusterId = entry.metadata.kafka_cluster_id;
      if (typeof kafkaClusterId !== "string" || !kafkaClusterId) return null;
      return ksqldbClusterUrl(entry.parent_id, kafkaClusterId, resourceId);
    }
    default:
      return null;
  }
}

function resolveFromIdentity(
  identityId: string,
  entry: IdentityEntry,
): string | null {
  switch (entry.identity_type) {
    case "service_account":
      return serviceAccountUrl(identityId);
    case "user":
      return userUrl(identityId);
    case "identity_provider":
      return identityProviderUrl(identityId);
    case "api_key":
      return apiKeyUrl(identityId);
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
  const [identityIndex, setIdentityIndex] = useState<IdentityIndex>({});

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
              metadata: Record<string, unknown>;
            }>;
            page: number;
            pages: number;
          };
          for (const r of data.items) {
            if (r.deleted_at) continue;
            newIndex[r.resource_id] = {
              resource_type: r.resource_type,
              parent_id: r.parent_id,
              metadata: r.metadata ?? {},
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

  useEffect(() => {
    if (!enabled || !currentTenant) return;

    const tenantName = currentTenant.tenant_name;
    const controller = new AbortController();

    async function fetchIdentities(): Promise<void> {
      try {
        const newIndex: IdentityIndex = {};
        let page = 1;
        let hasMore = true;

        while (hasMore) {
          const url = `${API_URL}/tenants/${tenantName}/identities?page=${page}&page_size=100`;
          const res = await fetch(url, { signal: controller.signal });
          if (!res.ok) break;
          const data = (await res.json()) as {
            items: Array<{
              identity_id: string;
              identity_type: string;
              deleted_at: string | null;
            }>;
            page: number;
            pages: number;
          };
          for (const r of data.items) {
            if (r.deleted_at) continue;
            newIndex[r.identity_id] = {
              identity_type: r.identity_type,
            };
          }
          hasMore = data.page < data.pages;
          page++;
        }

        if (!controller.signal.aborted) {
          setIdentityIndex(newIndex);
        }
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") return;
      }
    }

    void fetchIdentities();
    return () => controller.abort();
  }, [enabled, currentTenant]);

  const resolveUrl = useCallback(
    (resourceId: string): string | null => {
      if (!enabled) return null;

      const entry = index[resourceId];
      if (entry) {
        return resolveFromEntry(resourceId, entry);
      }

      // Prefix fallbacks for IDs not in resource index
      if (resourceId.startsWith("sa-")) return serviceAccountUrl(resourceId);
      if (resourceId.startsWith("env-")) return environmentUrl(resourceId);
      if (resourceId.startsWith("u-")) return userUrl(resourceId);
      if (resourceId.startsWith("op-")) return identityProviderUrl(resourceId);
      // lkc-, lsrc-, lfcp-, lksqlc- need parent context — cannot resolve without index

      // Identity index fallback — handles api_key (no prefix) and any
      // identities not caught by prefix matching above
      const identityEntry = identityIndex[resourceId];
      if (identityEntry) {
        return resolveFromIdentity(resourceId, identityEntry);
      }

      return null;
    },
    [enabled, index, identityIndex],
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
