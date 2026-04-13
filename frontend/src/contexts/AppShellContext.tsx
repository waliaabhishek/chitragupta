import { createContext, useContext } from "react";

interface AppShellContextValue {
  isDark: boolean;
  setSidebarCollapsed: (collapsed: boolean) => void;
}

export const AppShellContext = createContext<AppShellContextValue>({
  isDark: false,
  setSidebarCollapsed: () => {},
});

export const useAppShell = () => useContext(AppShellContext);
