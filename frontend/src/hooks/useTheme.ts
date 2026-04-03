import { useState, useCallback, useEffect } from "react";
import { theme as antTheme } from "antd";

type ThemeMode = "dark" | "light";

const STORAGE_KEY = "chargeback-theme";

function getInitialTheme(): ThemeMode {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  return "dark"; // default
}

interface UseThemeReturn {
  isDark: boolean;
  algorithm: typeof antTheme.darkAlgorithm;
  toggleTheme: () => void;
}

export function useTheme(): UseThemeReturn {
  const [mode, setMode] = useState<ThemeMode>(getInitialTheme);

  useEffect(() => {
    document.body.setAttribute("data-theme", mode);
    document.body.setAttribute("data-ag-theme-mode", mode);
  }, [mode]);

  const toggleTheme = useCallback(() => {
    setMode((prev) => {
      const next: ThemeMode = prev === "dark" ? "light" : "dark";
      localStorage.setItem(STORAGE_KEY, next);
      return next;
    });
  }, []);

  return {
    isDark: mode === "dark",
    algorithm:
      mode === "dark" ? antTheme.darkAlgorithm : antTheme.defaultAlgorithm,
    toggleTheme,
  };
}
