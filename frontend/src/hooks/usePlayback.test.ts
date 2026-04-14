import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { usePlayback } from "./usePlayback";

describe("usePlayback", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts paused with currentDate=null when minDate is null", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: null, maxDate: null }),
    );

    expect(result.current.state.isPlaying).toBe(false);
    expect(result.current.state.currentDate).toBeNull();
  });

  it("currentDate initializes to minDate when minDate transitions from null to a value", () => {
    const { result, rerender } = renderHook(
      ({ minDate }: { minDate: string | null }) =>
        usePlayback({ minDate, maxDate: "2026-12-31" }),
      { initialProps: { minDate: null as string | null } },
    );

    expect(result.current.state.currentDate).toBeNull();

    act(() => {
      rerender({ minDate: "2026-01-01" });
    });

    expect(result.current.state.currentDate).toBe("2026-01-01");
  });

  it("currentDate stays null when minDate remains null", () => {
    const { result, rerender } = renderHook(
      ({ minDate }: { minDate: string | null }) =>
        usePlayback({ minDate, maxDate: "2026-12-31" }),
      { initialProps: { minDate: null as string | null } },
    );

    act(() => {
      rerender({ minDate: null });
    });

    expect(result.current.state.currentDate).toBeNull();
  });

  it("starts paused with default speed=1 and stepDays=3", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    expect(result.current.state.isPlaying).toBe(false);
    expect(result.current.state.speed).toBe(1);
    expect(result.current.state.stepDays).toBe(3);
  });

  it("play() sets isPlaying to true", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    expect(result.current.state.isPlaying).toBe(true);
  });

  it("play() advances currentDate by stepDays on each 300ms tick", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(result.current.state.currentDate).toBe("2026-01-04");
  });

  it("play() advances date by stepDays on each subsequent tick", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(600);
    });

    expect(result.current.state.currentDate).toBe("2026-01-07");
  });

  it("pause() stops isPlaying and freezes date advancement", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    const frozenDate = result.current.state.currentDate;

    act(() => {
      result.current.pause();
    });

    act(() => {
      vi.advanceTimersByTime(900);
    });

    expect(result.current.state.isPlaying).toBe(false);
    expect(result.current.state.currentDate).toBe(frozenDate);
  });

  it("setSpeed(2) doubles tick rate — 2 ticks fire per 300ms interval", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.setSpeed(2);
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    // speed=2 → interval = 300/2 = 150ms per tick → 2 ticks × 3 days = 6 days
    expect(result.current.state.currentDate).toBe("2026-01-07");
    expect(result.current.state.speed).toBe(2);
  });

  it("setStepDays(1) changes step to 1 day per tick", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.setStepDays(1);
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(result.current.state.currentDate).toBe("2026-01-02");
    expect(result.current.state.stepDays).toBe(1);
  });

  it("auto-pauses at maxDate and sets isAtEnd=true", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-01-04" }),
    );

    act(() => {
      result.current.play();
    });

    act(() => {
      vi.advanceTimersByTime(300);
    });

    expect(result.current.state.currentDate).toBe("2026-01-04");
    expect(result.current.state.isPlaying).toBe(false);
    expect(result.current.isAtEnd).toBe(true);
  });

  it("isAtEnd=false when not at boundary", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    expect(result.current.isAtEnd).toBe(false);
  });

  it("setDate() updates currentDate immediately without advancing via interval", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.setDate("2026-06-15");
    });

    expect(result.current.state.currentDate).toBe("2026-06-15");
  });

  it("setDate() during play pauses playback", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    expect(result.current.state.isPlaying).toBe(true);

    act(() => {
      result.current.setDate("2026-06-15");
    });

    expect(result.current.state.isPlaying).toBe(false);
    expect(result.current.state.currentDate).toBe("2026-06-15");
  });

  it("setDate() while paused does not start playback", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.setDate("2026-06-15");
    });

    expect(result.current.state.isPlaying).toBe(false);
  });

  it("clears interval on unmount", () => {
    const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");
    const { result, unmount } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    unmount();

    expect(clearIntervalSpy).toHaveBeenCalled();
    clearIntervalSpy.mockRestore();
  });

  // GIT-001: initialDate param
  it("uses initialDate as starting currentDate when provided", () => {
    const { result } = renderHook(() =>
      usePlayback({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
        initialDate: "2026-06-01",
      }),
    );

    expect(result.current.state.currentDate).toBe("2026-06-01");
  });

  it("initialDate takes precedence over minDate on mount", () => {
    const { result } = renderHook(() =>
      usePlayback({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
        initialDate: "2026-03-15",
      }),
    );

    expect(result.current.state.currentDate).toBe("2026-03-15");
    expect(result.current.state.currentDate).not.toBe("2026-01-01");
  });

  it("initialDate=null falls back to minDate initialization", () => {
    const { result } = renderHook(() =>
      usePlayback({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
        initialDate: undefined,
      }),
    );

    expect(result.current.state.currentDate).toBe("2026-01-01");
  });

  // GIT-005: single-day range edge case
  it("when minDate === maxDate, play() immediately sets isAtEnd=true and pauses", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-04-13", maxDate: "2026-04-13" }),
    );

    act(() => {
      result.current.play();
    });

    expect(result.current.isAtEnd).toBe(true);
    expect(result.current.state.isPlaying).toBe(false);
  });

  it("when minDate === maxDate, currentDate is set to that single date", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-04-13", maxDate: "2026-04-13" }),
    );

    expect(result.current.state.currentDate).toBe("2026-04-13");
  });

  // GIT-R3: interval callback null-guard — !prev.currentDate true branch (line 60)
  // When currentDate is null and a tick fires, the callback returns state unchanged.
  // This covers the right side of the `||` guard: `!prev.currentDate === true`.
  it("interval tick with null currentDate is a no-op — state unchanged", () => {
    const { result } = renderHook(() =>
      usePlayback({ minDate: null, maxDate: "2026-12-31" }),
    );

    // minDate=null means currentDate stays null (initialization effect skips)
    expect(result.current.state.currentDate).toBeNull();

    act(() => {
      result.current.play();
    });

    expect(result.current.state.isPlaying).toBe(true);

    // Advance one full tick (speed=1 → tickInterval = 300ms)
    act(() => {
      vi.advanceTimersByTime(300);
    });

    // currentDate must still be null — the tick callback hit the null guard and returned prev
    expect(result.current.state.currentDate).toBeNull();
    expect(result.current.state.isPlaying).toBe(true);
  });

  // GIT-R2-005: clearInterval path in interval effect (lines 51-52)
  // The effect re-runs when speed changes while playing, clearing the old interval.
  it("changing speed while playing clears the old interval before creating a new one", () => {
    const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");

    const { result } = renderHook(() =>
      usePlayback({ minDate: "2026-01-01", maxDate: "2026-12-31" }),
    );

    act(() => {
      result.current.play();
    });

    clearIntervalSpy.mockClear();

    // Changing speed while playing triggers the interval effect to re-run.
    // The effect clears the existing interval (lines 51-52) before setting a new one.
    act(() => {
      result.current.setSpeed(2);
    });

    expect(clearIntervalSpy).toHaveBeenCalled();

    clearIntervalSpy.mockRestore();
  });
});
