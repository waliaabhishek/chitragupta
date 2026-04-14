import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useDebouncedValue } from "./useDebouncedValue";

describe("useDebouncedValue", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("returns initial value immediately without delay", () => {
    const { result } = renderHook(() => useDebouncedValue("initial", 200));

    expect(result.current).toBe("initial");
  });

  it("does not update immediately when value changes", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: string }) => useDebouncedValue(value, 200),
      { initialProps: { value: "initial" } },
    );

    act(() => {
      rerender({ value: "updated" });
    });

    // Should still be the initial value before the delay
    expect(result.current).toBe("initial");
  });

  it("updates after the specified delay", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: string }) => useDebouncedValue(value, 200),
      { initialProps: { value: "initial" } },
    );

    act(() => {
      rerender({ value: "updated" });
    });

    act(() => {
      vi.advanceTimersByTime(200);
    });

    expect(result.current).toBe("updated");
  });

  it("resets timer on rapid changes — only last value fires", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: string }) => useDebouncedValue(value, 200),
      { initialProps: { value: "a" } },
    );

    act(() => {
      rerender({ value: "b" });
    });

    act(() => {
      vi.advanceTimersByTime(100);
    });

    act(() => {
      rerender({ value: "c" });
    });

    act(() => {
      vi.advanceTimersByTime(100);
    });

    // Timer was reset — not enough time has passed since "c" was set
    expect(result.current).toBe("a");

    act(() => {
      vi.advanceTimersByTime(200);
    });

    // Now the full delay has elapsed since the last update
    expect(result.current).toBe("c");
  });

  it("intermediate rapid values do not fire — only last value after delay", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: string }) => useDebouncedValue(value, 200),
      { initialProps: { value: "first" } },
    );

    // Rapid succession of updates
    act(() => {
      rerender({ value: "second" });
    });
    act(() => {
      rerender({ value: "third" });
    });
    act(() => {
      rerender({ value: "fourth" });
    });

    // Before delay: still original
    expect(result.current).toBe("first");

    act(() => {
      vi.advanceTimersByTime(200);
    });

    // After delay: only the last value
    expect(result.current).toBe("fourth");
  });

  it("does not update after unmount (cleanup on unmount)", () => {
    const { result, rerender, unmount } = renderHook(
      ({ value }: { value: string }) => useDebouncedValue(value, 200),
      { initialProps: { value: "initial" } },
    );

    act(() => {
      rerender({ value: "updated" });
    });

    unmount();

    // Advancing timers after unmount should not cause issues
    act(() => {
      vi.advanceTimersByTime(200);
    });

    // Value stays at whatever it was at unmount time (initial)
    expect(result.current).toBe("initial");
  });

  it("works with number values", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: number }) => useDebouncedValue(value, 200),
      { initialProps: { value: 0 } },
    );

    act(() => {
      rerender({ value: 42 });
    });

    expect(result.current).toBe(0);

    act(() => {
      vi.advanceTimersByTime(200);
    });

    expect(result.current).toBe(42);
  });

  it("works with null values", () => {
    const { result, rerender } = renderHook(
      ({ value }: { value: string | null }) => useDebouncedValue(value, 200),
      { initialProps: { value: "hello" as string | null } },
    );

    act(() => {
      rerender({ value: null });
    });

    expect(result.current).toBe("hello");

    act(() => {
      vi.advanceTimersByTime(200);
    });

    expect(result.current).toBeNull();
  });
});
