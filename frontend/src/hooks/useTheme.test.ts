import { act, renderHook } from "@testing-library/react";
import { theme as antTheme } from "antd";
import { beforeEach, describe, expect, it } from "vitest";
import { useTheme } from "./useTheme";

describe("useTheme", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("defaults to dark mode when localStorage is empty", () => {
    const { result } = renderHook(() => useTheme());

    expect(result.current.isDark).toBe(true);
    expect(result.current.algorithm).toBe(antTheme.darkAlgorithm);
  });

  it("toggles theme and persists to localStorage", () => {
    const { result } = renderHook(() => useTheme());

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.isDark).toBe(false);
    expect(localStorage.getItem("chargeback-theme")).toBe("light");

    act(() => {
      result.current.toggleTheme();
    });

    expect(result.current.isDark).toBe(true);
    expect(localStorage.getItem("chargeback-theme")).toBe("dark");
  });

  it("reads theme preference from localStorage on init", () => {
    localStorage.setItem("chargeback-theme", "light");

    const { result } = renderHook(() => useTheme());

    expect(result.current.isDark).toBe(false);
  });
});
