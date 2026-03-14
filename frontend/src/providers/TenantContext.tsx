import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { API_URL } from "../config";
import type {
  ReadinessResponse,
  TenantStatusSummary,
} from "../types/api";

const STORAGE_KEY = "chargeback_selected_tenant";

export type AppStatus = "loading" | "initializing" | "no_data" | "ready" | "error";

interface TenantContextValue {
  tenants: TenantStatusSummary[];
  currentTenant: TenantStatusSummary | null;
  setCurrentTenant: (tenant: TenantStatusSummary | null) => void;
  isLoading: boolean;
  error: string | null;
  refetch: () => void;
  appStatus: AppStatus;
  readiness: ReadinessResponse | null;
  isReadOnly: boolean;
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
  const [appStatus, setAppStatus] = useState<AppStatus>("loading");
  const [readiness, setReadiness] = useState<ReadinessResponse | null>(null);
  const [tenantsLoaded, setTenantsLoaded] = useState(false);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchReadiness = useCallback(async (): Promise<ReadinessResponse | null> => {
    try {
      const res = await fetch(`${API_URL}/readiness`);
      if (!res.ok) return null;
      return (await res.json()) as ReadinessResponse;
    } catch {
      return null;
    }
  }, []);

  const fetchTenants = useCallback(async (): Promise<void> => {
    try {
      const response = await fetch(`${API_URL}/tenants`);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      const data = (await response.json()) as {
        tenants: TenantStatusSummary[];
      };

      setTenants(data.tenants);
      setTenantsLoaded(true);

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
      setError(
        err instanceof Error ? err.message : "Failed to load tenants",
      );
    }
  }, []);

  // Main readiness polling loop
  useEffect(() => {
    let cancelled = false;

    async function poll(): Promise<void> {
      const data = await fetchReadiness();
      if (cancelled) return;

      if (data === null) {
        // Backend not reachable
        setAppStatus("loading");
        setIsLoading(true);
        pollRef.current = setTimeout(() => { void poll(); }, 5000);
        return;
      }

      setReadiness(data);

      if (data.status === "initializing" || data.status === "no_data") {
        setAppStatus(data.status);
        setIsLoading(false);
        // Fast poll while waiting for data
        pollRef.current = setTimeout(() => { void poll(); }, 5000);
      } else if (data.status === "error") {
        setAppStatus("error");
        const failures = data.tenants
          .filter((t) => t.permanent_failure)
          .map((t) => `${t.tenant_name}: ${t.permanent_failure}`);
        setError(failures.join("; ") || "All tenants permanently failed");
        setIsLoading(false);
      } else {
        // ready
        setAppStatus("ready");
        setIsLoading(false);
        // Fast poll during active pipeline; slow poll when idle
        const anyRunning = data.tenants.some((t) => t.pipeline_running);
        const interval = anyRunning ? 5000 : 15000;
        pollRef.current = setTimeout(() => { void poll(); }, interval);
      }

      // Fetch tenant list once readiness is established (not loading)
      if (!tenantsLoaded && data.status !== "error") {
        void fetchTenants();
      }
    }

    void poll();
    return () => {
      cancelled = true;
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [fetchReadiness, fetchTenants, tenantsLoaded]);

  const refetch = useCallback(() => {
    setIsLoading(true);
    setError(null);
    setTenantsLoaded(false);
  }, []);

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

  // Read-only when any tenant for the current selection is running pipeline
  const isReadOnly =
    readiness?.tenants.some(
      (t) =>
        t.tenant_name === currentTenant?.tenant_name && t.pipeline_running,
    ) ?? false;

  return (
    <TenantContext.Provider
      value={{
        tenants,
        currentTenant,
        setCurrentTenant,
        isLoading,
        error,
        refetch,
        appStatus,
        readiness,
        isReadOnly,
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
