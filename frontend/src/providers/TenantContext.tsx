import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { API_URL } from "../config";
import type { TenantStatusSummary } from "../types/api";

const STORAGE_KEY = "chargeback_selected_tenant";

interface TenantContextValue {
  tenants: TenantStatusSummary[];
  currentTenant: TenantStatusSummary | null;
  setCurrentTenant: (tenant: TenantStatusSummary | null) => void;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
}

const TenantContext = createContext<TenantContextValue | null>(null);

interface TenantProviderProps {
  children: ReactNode;
}

export function TenantProvider({ children }: TenantProviderProps): JSX.Element {
  const [tenants, setTenants] = useState<TenantStatusSummary[]>([]);
  const [currentTenant, setCurrentTenantState] =
    useState<TenantStatusSummary | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refetchKey, setRefetchKey] = useState(0);

  const refetch = useCallback(() => {
    setIsLoading(true);
    setError(null);
    setRefetchKey((k) => k + 1);
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function fetchTenants(): Promise<void> {
      try {
        const response = await fetch(`${API_URL}/tenants`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = (await response.json()) as {
          tenants: TenantStatusSummary[];
        };
        if (cancelled) return;

        setTenants(data.tenants);

        // Restore previously selected tenant from localStorage
        const savedName = localStorage.getItem(STORAGE_KEY);
        if (savedName) {
          const found = data.tenants.find((t) => t.tenant_name === savedName);
          if (found) {
            setCurrentTenantState(found);
          } else if (data.tenants.length > 0) {
            setCurrentTenantState(data.tenants[0]);
          }
        } else if (data.tenants.length > 0) {
          setCurrentTenantState(data.tenants[0]);
        }
      } catch (err) {
        if (!cancelled) {
          setError(
            err instanceof Error ? err.message : "Failed to load tenants",
          );
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void fetchTenants();
    return () => {
      cancelled = true;
    };
  }, [refetchKey]);

  const setCurrentTenant = useCallback(
    (tenant: TenantStatusSummary | null) => {
      setCurrentTenantState(tenant);
      if (tenant) {
        localStorage.setItem(STORAGE_KEY, tenant.tenant_name);
      } else {
        localStorage.removeItem(STORAGE_KEY);
      }
    },
    [],
  );

  return (
    <TenantContext.Provider
      value={{
        tenants,
        currentTenant,
        setCurrentTenant,
        isLoading,
        error,
        refetch,
      }}
    >
      {children}
    </TenantContext.Provider>
  );
}

// eslint-disable-next-line react-refresh/only-export-components
export function useTenant(): TenantContextValue {
  const ctx = useContext(TenantContext);
  if (!ctx) {
    throw new Error("useTenant must be used within TenantProvider");
  }
  return ctx;
}
