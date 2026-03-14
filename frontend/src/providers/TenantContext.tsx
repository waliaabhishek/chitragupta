import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
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

/**
 * Full JSON fingerprint — PipelineStatusBanner reads pipeline_stage,
 * pipeline_current_date, permanent_failure, and mode from readiness directly,
 * so any field change must propagate.
 */
function readinessFingerprint(data: ReadinessResponse): string {
  return JSON.stringify(data);
}

export function TenantProvider({ children }: TenantProviderProps): JSX.Element {
  const [tenants, setTenants] = useState<TenantStatusSummary[]>([]);
  const [currentTenant, setCurrentTenantState] =
    useState<TenantStatusSummary | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [appStatus, setAppStatus] = useState<AppStatus>("loading");
  const [readiness, setReadiness] = useState<ReadinessResponse | null>(null);
  // useRef instead of useState — does not appear in effect deps, no re-render on set
  const tenantsLoadedRef = useRef(false);
  const readinessFingerprintRef = useRef<string | null>(null);
  // restartKey: the only way to restart the poll loop after an error (incremented by refetch())
  const [restartKey, setRestartKey] = useState(0);
  const pollRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const fetchReadiness = useCallback(
    async (signal: AbortSignal): Promise<ReadinessResponse | null> => {
      try {
        const res = await fetch(`${API_URL}/readiness`, { signal });
        if (!res.ok) return null;
        return (await res.json()) as ReadinessResponse;
      } catch (err) {
        if (err instanceof Error && err.name === "AbortError") return null;
        return null;
      }
    },
    [],
  );

  const fetchTenants = useCallback(
    async (signal: AbortSignal): Promise<void> => {
      try {
        const response = await fetch(`${API_URL}/tenants`, { signal });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        const data = (await response.json()) as { tenants: TenantStatusSummary[] };
        setTenants(data.tenants);
        tenantsLoadedRef.current = true;  // ref write — no re-render, no effect re-run
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
        if (err instanceof Error && err.name === "AbortError") return;
        setError(err instanceof Error ? err.message : "Failed to load tenants");
      }
    },
    [],
  );

  // Main readiness polling loop — stable deps, never restarts due to tenant load
  useEffect(() => {
    const controller = new AbortController();

    async function poll(): Promise<void> {
      const data = await fetchReadiness(controller.signal);
      if (controller.signal.aborted) return;

      if (data === null) {
        setAppStatus("loading");
        setIsLoading(true);
        pollRef.current = setTimeout(() => { void poll(); }, 5000);
        return;
      }

      // Only update readiness state when material fields actually changed
      const fp = readinessFingerprint(data);
      if (fp !== readinessFingerprintRef.current) {
        readinessFingerprintRef.current = fp;
        setReadiness(data);
      }

      if (data.status === "initializing" || data.status === "no_data") {
        setAppStatus(data.status);
        setIsLoading(false);
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
        const anyRunning = data.tenants.some((t) => t.pipeline_running);
        const interval = anyRunning ? 5000 : 15000;
        pollRef.current = setTimeout(() => { void poll(); }, interval);
      }

      // Fetch tenant list once — ref check never triggers effect re-run
      if (!tenantsLoadedRef.current && data.status !== "error") {
        void fetchTenants(controller.signal);
      }
    }

    void poll();
    return () => {
      controller.abort();
      if (pollRef.current) clearTimeout(pollRef.current);
    };
  }, [fetchReadiness, fetchTenants, restartKey]);  // restartKey restarts poll after error; tenantsLoaded removed

  const refetch = useCallback(() => {
    setIsLoading(true);
    setError(null);
    tenantsLoadedRef.current = false;         // re-fetch tenants on next poll
    readinessFingerprintRef.current = null;   // force readiness state update on next poll
    setRestartKey((k) => k + 1);             // restart poll loop (only mechanism after error branch stops it)
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

  const isReadOnly =
    readiness?.tenants.some(
      (t) =>
        t.tenant_name === currentTenant?.tenant_name && t.pipeline_running,
    ) ?? false;

  // Memoize context value — consumers only re-render when deps actually change
  const contextValue = useMemo<TenantContextValue>(
    () => ({
      tenants,
      currentTenant,
      setCurrentTenant,
      isLoading,
      error,
      refetch,
      appStatus,
      readiness,
      isReadOnly,
    }),
    [tenants, currentTenant, setCurrentTenant, isLoading, error, refetch, appStatus, readiness, isReadOnly],
  );

  return (
    <TenantContext.Provider value={contextValue}>
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
