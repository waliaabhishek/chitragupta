import type React from "react";
import { useRef, useState } from "react";
import type { TimelinePoint } from "../../hooks/useGraphTimeline";

interface TimelineScrubberProps {
  minDate: string;
  maxDate: string;
  currentDate: string | null;
  onDateChange: (date: string) => void;
  isPlaying: boolean;
  onPlay: () => void;
  onPause: () => void;
  isAtEnd: boolean;
  speed: number;
  onSpeedChange: (speed: number) => void;
  stepDays: number;
  onStepChange: (days: number) => void;
  timelineData: TimelinePoint[] | null;
  isLoading: boolean;
  disabled: boolean;
  isDark: boolean;
}

function computeDateFromPosition(
  clientX: number,
  rect: { left: number; width: number },
  minDate: string,
  maxDate: string,
): string {
  const fraction = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
  const minMs = new Date(minDate + "T00:00:00Z").getTime();
  const maxMs = new Date(maxDate + "T00:00:00Z").getTime();
  const targetMs = minMs + fraction * (maxMs - minMs);
  return new Date(targetMs).toISOString().split("T")[0];
}

function findNearestPoint(
  timelineData: TimelinePoint[],
  date: string,
): TimelinePoint | null {
  if (timelineData.length === 0) return null;
  let lo = 0;
  let hi = timelineData.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (timelineData[mid].date < date) lo = mid + 1;
    else hi = mid;
  }
  if (lo === 0) return timelineData[0];
  const prev = timelineData[lo - 1];
  const curr = timelineData[lo];
  const currDist = Math.abs(
    new Date(curr.date).getTime() - new Date(date).getTime(),
  );
  const prevDist = Math.abs(
    new Date(prev.date).getTime() - new Date(date).getTime(),
  );
  return currDist <= prevDist ? curr : prev;
}

const formatCost = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

export function TimelineScrubber({
  minDate,
  maxDate,
  currentDate,
  onDateChange,
  isPlaying,
  onPlay,
  onPause,
  isAtEnd,
  speed,
  onSpeedChange,
  stepDays,
  onStepChange,
  timelineData,
  isLoading,
  disabled,
  isDark,
}: TimelineScrubberProps): React.JSX.Element {
  const isDraggingRef = useRef(false);
  const [hoverTooltip, setHoverTooltip] = useState<{
    x: number;
    date: string;
    cost: number | null;
  } | null>(null);

  function handlePointerDown(e: React.PointerEvent<HTMLDivElement>) {
    if (disabled) return;
    const rect = e.currentTarget.getBoundingClientRect();
    if (rect.width === 0) return; // zero-width null guard
    if (minDate === maxDate) return; // single-day range — no navigation
    isDraggingRef.current = true;
    e.currentTarget.setPointerCapture(e.pointerId);
    const date = computeDateFromPosition(e.clientX, rect, minDate, maxDate);
    onDateChange(date);
  }

  function handlePointerMove(e: React.PointerEvent<HTMLDivElement>) {
    if (disabled) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const date = computeDateFromPosition(e.clientX, rect, minDate, maxDate);

    // Update hover tooltip
    const nearest = timelineData ? findNearestPoint(timelineData, date) : null;
    setHoverTooltip({
      x: e.clientX - rect.left,
      date,
      cost: nearest?.cost ?? null,
    });

    if (isDraggingRef.current) {
      onDateChange(date);
    }
  }

  function handlePointerUp(e: React.PointerEvent<HTMLDivElement>) {
    if (isDraggingRef.current) {
      e.currentTarget.releasePointerCapture(e.pointerId);
      isDraggingRef.current = false;
    }
  }

  function handlePointerLeave() {
    setHoverTooltip(null);
    isDraggingRef.current = false;
  }

  function handleReplay() {
    onDateChange(minDate);
    onPlay();
  }

  const isSingleDay = minDate === maxDate;

  let playPauseButton: React.JSX.Element;
  if (isAtEnd) {
    playPauseButton = (
      <button aria-label="Replay" onClick={handleReplay}>
        ↺
      </button>
    );
  } else if (isPlaying) {
    playPauseButton = (
      <button aria-label="Pause" onClick={onPause}>
        ⏸
      </button>
    );
  } else {
    playPauseButton = (
      <button
        aria-label="Play"
        onClick={onPlay}
        disabled={isSingleDay}
        aria-disabled={isSingleDay ? "true" : undefined}
      >
        ▶
      </button>
    );
  }

  // Compute thumb position
  const thumbPct =
    currentDate && minDate && maxDate
      ? (() => {
          const minMs = new Date(minDate + "T00:00:00Z").getTime();
          const maxMs = new Date(maxDate + "T00:00:00Z").getTime();
          const curMs = new Date(currentDate + "T00:00:00Z").getTime();
          return Math.max(
            0,
            Math.min(100, ((curMs - minMs) / (maxMs - minMs)) * 100),
          );
        })()
      : 0;

  const bg = isDark ? "rgba(0,0,0,0.3)" : "rgba(0,0,0,0.05)";
  const borderColor = isDark ? "rgba(255,255,255,0.1)" : "rgba(0,0,0,0.1)";
  const textColor = isDark ? "#e0e0e0" : undefined;

  return (
    <div
      data-testid="timeline-scrubber"
      data-disabled={disabled ? "true" : undefined}
      aria-disabled={disabled ? "true" : undefined}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "0 16px",
        height: 64,
        background: bg,
        borderTop: `1px solid ${borderColor}`,
        pointerEvents: disabled ? "none" : undefined,
        opacity: disabled ? 0.5 : 1,
        position: "relative",
        color: textColor,
      }}
    >
      {playPauseButton}

      <span style={{ fontSize: 12, minWidth: 80 }}>
        {currentDate ?? minDate}
      </span>

      <div
        data-testid="scrubber-track"
        onPointerDown={handlePointerDown}
        onPointerMove={handlePointerMove}
        onPointerUp={handlePointerUp}
        onPointerLeave={handlePointerLeave}
        style={{
          flex: 1,
          height: 8,
          background: isDark ? "rgba(255,255,255,0.15)" : "rgba(0,0,0,0.15)",
          borderRadius: 4,
          position: "relative",
          cursor: disabled ? "default" : "pointer",
        }}
      >
        <div
          style={{
            position: "absolute",
            left: `${thumbPct}%`,
            top: "50%",
            transform: "translate(-50%, -50%)",
            width: 14,
            height: 14,
            borderRadius: "50%",
            background: "#1890ff",
            pointerEvents: "none",
          }}
        />
        {hoverTooltip && (
          <div
            style={{
              position: "absolute",
              left: hoverTooltip.x,
              bottom: "100%",
              transform: "translateX(-50%)",
              background: "rgba(0,0,0,0.8)",
              color: "#fff",
              padding: "2px 6px",
              borderRadius: 4,
              fontSize: 11,
              pointerEvents: "none",
              whiteSpace: "nowrap",
              marginBottom: 4,
            }}
          >
            {hoverTooltip.date}
            {hoverTooltip.cost !== null
              ? `: ${formatCost.format(hoverTooltip.cost)}`
              : ""}
          </div>
        )}
      </div>

      <select
        data-testid="speed-selector"
        aria-label="Speed"
        value={speed}
        onChange={(e) => onSpeedChange(Number(e.target.value))}
        style={{ fontSize: 12 }}
      >
        <option value="0.5">0.5x</option>
        <option value="1">1x</option>
        <option value="2">2x</option>
        <option value="4">4x</option>
      </select>

      <select
        data-testid="step-selector"
        aria-label="Step size"
        value={stepDays}
        onChange={(e) => onStepChange(Number(e.target.value))}
        style={{ fontSize: 12 }}
      >
        <option value="1">1 day</option>
        <option value="3">3 days</option>
      </select>

      {isLoading && (
        <div
          style={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            height: 2,
            background: "#1890ff",
          }}
        />
      )}

      {timelineData && timelineData.length > 0 && (
        <div
          data-testid="timeline-data"
          style={{ width: 60, fontSize: 10, opacity: 0.6 }}
        >
          {timelineData.length} pts
        </div>
      )}
    </div>
  );
}
