import { useCallback, useEffect, useRef, useState } from "react";
import { addDays } from "../utils/dateUtils";

export interface PlaybackState {
  isPlaying: boolean;
  speed: number;
  currentDate: string | null;
  stepDays: number;
}

export interface UsePlaybackParams {
  minDate: string | null;
  maxDate: string | null;
  initialDate?: string;
}

export interface UsePlaybackResult {
  state: PlaybackState;
  play: () => void;
  pause: () => void;
  setSpeed: (speed: number) => void;
  setStepDays: (days: number) => void;
  setDate: (date: string) => void;
  isAtEnd: boolean;
}

export function usePlayback({
  maxDate,
  initialDate,
}: UsePlaybackParams): UsePlaybackResult {
  const [state, setState] = useState<PlaybackState>({
    isPlaying: false,
    speed: 1,
    currentDate: initialDate ?? null,
    stepDays: 3,
  });

  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Initialize currentDate to latest date when bounds become available
  useEffect(() => {
    if (maxDate && state.currentDate === null) {
      setState((prev) => ({ ...prev, currentDate: maxDate }));
    }
  }, [maxDate]); // eslint-disable-line react-hooks/exhaustive-deps

  // Interval management — restart when isPlaying or speed changes
  useEffect(() => {
    if (intervalRef.current !== null) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }

    if (!state.isPlaying) return;

    const tickInterval = Math.round(300 / state.speed);
    intervalRef.current = setInterval(() => {
      setState((prev) => {
        if (!prev.isPlaying || !prev.currentDate) return prev;
        const next = addDays(prev.currentDate, prev.stepDays);
        if (maxDate && next >= maxDate) {
          return { ...prev, isPlaying: false, currentDate: maxDate };
        }
        return { ...prev, currentDate: next };
      });
    }, tickInterval);

    return () => {
      if (intervalRef.current !== null) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [state.isPlaying, state.speed, maxDate]);

  const play = useCallback(() => {
    setState((prev) => {
      const atEnd =
        prev.currentDate !== null &&
        maxDate !== null &&
        prev.currentDate >= maxDate;
      if (atEnd) return prev;
      return { ...prev, isPlaying: true };
    });
  }, [maxDate]);

  const pause = useCallback(() => {
    setState((prev) => ({ ...prev, isPlaying: false }));
  }, []);

  const setSpeed = useCallback((speed: number) => {
    setState((prev) => ({ ...prev, speed }));
  }, []);

  const setStepDays = useCallback((days: number) => {
    setState((prev) => ({ ...prev, stepDays: days }));
  }, []);

  const setDate = useCallback((date: string) => {
    setState((prev) => ({ ...prev, currentDate: date, isPlaying: false }));
  }, []);

  const isAtEnd =
    state.currentDate !== null &&
    maxDate !== null &&
    state.currentDate >= maxDate;

  return { state, play, pause, setSpeed, setStepDays, setDate, isAtEnd };
}
