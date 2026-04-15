import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { TimelinePoint } from "../../hooks/useGraphTimeline";
import { TimelineScrubber } from "./TimelineScrubber";

// jsdom does not implement pointer capture — define no-ops at module level.
// The implementation calls setPointerCapture/releasePointerCapture on the
// track element; without this they throw and break all tests in the file.
if (!("setPointerCapture" in HTMLElement.prototype)) {
  Object.defineProperty(HTMLElement.prototype, "setPointerCapture", {
    writable: true,
    configurable: true,
    value: () => {},
  });
}
if (!("releasePointerCapture" in HTMLElement.prototype)) {
  Object.defineProperty(HTMLElement.prototype, "releasePointerCapture", {
    writable: true,
    configurable: true,
    value: () => {},
  });
}

const DEFAULT_PROPS = {
  minDate: "2026-01-01",
  maxDate: "2026-12-31",
  currentDate: "2026-06-15",
  onDateChange: vi.fn(),
  isPlaying: false,
  onPlay: vi.fn(),
  onPause: vi.fn(),
  isAtEnd: false,
  speed: 1,
  onSpeedChange: vi.fn(),
  stepDays: 3,
  onStepChange: vi.fn(),
  timelineData: null,
  isLoading: false,
  disabled: false,
  isDark: false,
};

describe("TimelineScrubber", () => {
  it("renders the current date as a label", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} currentDate="2026-06-15" />);

    expect(screen.getByText(/2026-06-15/)).toBeInTheDocument();
  });

  it("renders null currentDate gracefully — mounts without throwing", () => {
    const { container } = render(
      <TimelineScrubber {...DEFAULT_PROPS} currentDate={null} />,
    );

    // Component must mount
    expect(container.firstChild).not.toBeNull();

    // Date label should not contain "undefined" or "NaN"
    const dateLabel = container.querySelector("[data-testid='date-label']");
    if (dateLabel) {
      expect(dateLabel.textContent).not.toContain("undefined");
      expect(dateLabel.textContent).not.toContain("NaN");
    }
  });

  it("renders play button when isPlaying=false", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} isPlaying={false} />);

    const playBtn = screen.getByRole("button", { name: /play/i });
    expect(playBtn).toBeInTheDocument();
  });

  it("calls onPlay when play button is clicked", () => {
    const onPlay = vi.fn();
    render(
      <TimelineScrubber {...DEFAULT_PROPS} isPlaying={false} onPlay={onPlay} />,
    );

    fireEvent.click(screen.getByRole("button", { name: /play/i }));

    expect(onPlay).toHaveBeenCalledTimes(1);
  });

  it("renders pause button when isPlaying=true", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} isPlaying={true} />);

    const pauseBtn = screen.getByRole("button", { name: /pause/i });
    expect(pauseBtn).toBeInTheDocument();
  });

  it("calls onPause when pause button is clicked", () => {
    const onPause = vi.fn();
    render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        isPlaying={true}
        onPause={onPause}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /pause/i }));

    expect(onPause).toHaveBeenCalledTimes(1);
  });

  it("renders replay/restart button when isAtEnd=true", () => {
    render(
      <TimelineScrubber {...DEFAULT_PROPS} isAtEnd={true} isPlaying={false} />,
    );

    // When at end, shows replay icon instead of play
    const replayBtn = screen.getByRole("button", {
      name: /replay|restart|again/i,
    });
    expect(replayBtn).toBeInTheDocument();
  });

  it("renders speed selector", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} speed={1} />);

    // Speed control should be present (select or buttons)
    expect(
      screen.getByTestId("speed-selector") ||
        screen.getByLabelText(/speed/i) ||
        screen.getByText(/1x/i),
    ).toBeInTheDocument();
  });

  it("calls onSpeedChange when speed changes", () => {
    const onSpeedChange = vi.fn();
    render(
      <TimelineScrubber {...DEFAULT_PROPS} onSpeedChange={onSpeedChange} />,
    );

    const speedSelect = screen.getByTestId("speed-selector");
    fireEvent.change(speedSelect, { target: { value: "2" } });

    expect(onSpeedChange).toHaveBeenCalledWith(2);
  });

  it("renders step selector with 1-day option", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} stepDays={3} />);

    const stepSelect = screen.getByTestId("step-selector");
    expect(stepSelect).toBeInTheDocument();
    expect(
      stepSelect.querySelector('option[value="1"]') ??
        screen.queryByText(/1.?day/i),
    ).not.toBeNull();
  });

  it("renders step selector with 3-day option", () => {
    render(<TimelineScrubber {...DEFAULT_PROPS} stepDays={3} />);

    const stepSelect = screen.getByTestId("step-selector");
    expect(
      stepSelect.querySelector('option[value="3"]') ??
        screen.queryByText(/3.?day/i),
    ).not.toBeNull();
  });

  it("calls onStepChange when step changes", () => {
    const onStepChange = vi.fn();
    render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        stepDays={3}
        onStepChange={onStepChange}
      />,
    );

    const stepSelect = screen.getByTestId("step-selector");
    fireEvent.change(stepSelect, { target: { value: "1" } });

    expect(onStepChange).toHaveBeenCalledWith(1);
  });

  it("is visually disabled when disabled=true", () => {
    const { container } = render(
      <TimelineScrubber {...DEFAULT_PROPS} disabled={true} />,
    );

    const scrubber = container.firstChild as HTMLElement;
    expect(
      scrubber.classList.contains("disabled") ||
        scrubber.getAttribute("aria-disabled") === "true" ||
        scrubber.style.pointerEvents === "none" ||
        scrubber.getAttribute("data-disabled") === "true",
    ).toBe(true);
  });

  it("does not call onDateChange when disabled and pointerdown fires", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        disabled={true}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    if (track) {
      fireEvent.pointerDown(track, { clientX: 50 });
    }

    expect(onDateChange).not.toHaveBeenCalled();
  });

  it("calls onDateChange on pointerdown in track area", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    expect(track).not.toBeNull();

    // Mock getBoundingClientRect so position math works
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 0 });

    expect(onDateChange).toHaveBeenCalled();
  });

  it("calls onDateChange on pointermove after pointerdown", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    expect(track).not.toBeNull();

    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 0 });
    fireEvent.pointerMove(track!, { clientX: 50 });

    expect(onDateChange).toHaveBeenCalledTimes(2);
  });

  it("date computed from pointerdown at leftmost position equals minDate", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 0 });

    expect(onDateChange).toHaveBeenCalledWith("2026-01-01");
  });

  it("date computed from pointerdown at rightmost position equals maxDate", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 100 });

    expect(onDateChange).toHaveBeenCalledWith("2026-12-31");
  });

  it("shows timeline tooltip area when timelineData is provided", () => {
    const timelineData: TimelinePoint[] = [
      { date: "2026-01-01", cost: 100 },
      { date: "2026-06-15", cost: 250 },
    ];

    const { container } = render(
      <TimelineScrubber {...DEFAULT_PROPS} timelineData={timelineData} />,
    );

    // Timeline data area should be present when data is provided
    expect(
      container.querySelector("[data-testid='timeline-data']") ||
        container.querySelector("[data-testid='scrubber-chart']"),
    ).not.toBeNull();
  });

  // GIT-003: pointerUp handler
  it("pointerUp ends drag — pointermove after pointerup does not call onDateChange", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    expect(track).not.toBeNull();
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 0 });
    fireEvent.pointerUp(track!);
    onDateChange.mockClear();

    // After pointerUp, pointermove should be a no-op
    fireEvent.pointerMove(track!, { clientX: 50 });

    expect(onDateChange).not.toHaveBeenCalled();
  });

  // GIT-003: null guard early return — zero-width track
  it("does not call onDateChange when track has zero width — null guard", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    expect(track).not.toBeNull();
    // jsdom default: getBoundingClientRect returns { width: 0, ... }
    // No mock — zero-width triggers the null guard early return

    fireEvent.pointerDown(track!, { clientX: 50 });

    expect(onDateChange).not.toHaveBeenCalled();
  });

  // GIT-003: cleanup on unmount
  it("unmounting mid-drag does not throw", () => {
    const { container, unmount } = render(
      <TimelineScrubber {...DEFAULT_PROPS} disabled={false} />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 0 });

    expect(() => unmount()).not.toThrow();
  });

  // GIT-005: single-day range edge cases
  it("when minDate === maxDate, pointerdown does not call onDateChange", () => {
    const onDateChange = vi.fn();
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-04-13"
        maxDate="2026-04-13"
        disabled={false}
        onDateChange={onDateChange}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerDown(track!, { clientX: 50 });

    expect(onDateChange).not.toHaveBeenCalled();
  });

  it("when minDate === maxDate, play button is disabled", () => {
    render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-04-13"
        maxDate="2026-04-13"
        isPlaying={false}
      />,
    );

    const playBtn = screen.getByRole("button", { name: /play/i });
    expect(
      playBtn.hasAttribute("disabled") ||
        playBtn.getAttribute("aria-disabled") === "true",
    ).toBe(true);
  });

  // GIT-R3: isLoading indicator branch (line 286)
  // The loading bar is a full-width 2px-high strip; the thumb is a 14px circle.
  // Distinguish them by height.
  it("shows loading indicator (height 2px) when isLoading=true", () => {
    const { container } = render(
      <TimelineScrubber {...DEFAULT_PROPS} isLoading={true} />,
    );

    const loadingBar = Array.from(container.querySelectorAll("div")).find(
      (el) => el.style.height === "2px",
    );
    expect(loadingBar).not.toBeUndefined();
  });

  it("does not show loading indicator when isLoading=false", () => {
    const { container } = render(
      <TimelineScrubber {...DEFAULT_PROPS} isLoading={false} />,
    );

    const loadingBar = Array.from(container.querySelectorAll("div")).find(
      (el) => el.style.height === "2px",
    );
    expect(loadingBar).toBeUndefined();
  });

  // GIT-R3: findNearestPoint with lo > 0 — binary search body (lines 50-58)
  // A hover position between data points forces the binary search past the
  // lo===0 early-return, exercising lines 50-58 and both branches of
  // the ternary `currDist <= prevDist ? curr : prev`.

  it("findNearestPoint returns prev when prev is closer than curr (line 58 false branch)", () => {
    // timelineData: Jan 1 (100), Jun 15 (250), Dec 31 (300)
    // Hover near start (~8%): computed date ≈ late Jan → closer to Jan 1 than Jun 15
    // Binary search lands at lo=1 (> 0), then picks prev (Jan 1, cost $100)
    const timelineData: TimelinePoint[] = [
      { date: "2026-01-01", cost: 100 },
      { date: "2026-06-15", cost: 250 },
      { date: "2026-12-31", cost: 300 },
    ];

    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={timelineData}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    // clientX=8 → fraction 0.08 → date ≈ 2026-01-29 (29 days from Jan 1)
    // Nearest: Jan 1 (29d) vs Jun 15 (137d) → Jan 1 wins (prev)
    fireEvent.pointerMove(track!, { clientX: 8 });

    expect(screen.getByText(/\$100\.00/)).toBeInTheDocument();
  });

  it("findNearestPoint returns curr when curr is closer than prev (line 58 true branch)", () => {
    const timelineData: TimelinePoint[] = [
      { date: "2026-01-01", cost: 100 },
      { date: "2026-06-15", cost: 250 },
      { date: "2026-12-31", cost: 300 },
    ];

    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={timelineData}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    // clientX=90 → fraction 0.9 → date ≈ 2026-12-03 (327 days from Jan 1)
    // Nearest: Jun 15 (171d away) vs Dec 31 (28d away) → Dec 31 wins (curr)
    fireEvent.pointerMove(track!, { clientX: 90 });

    expect(screen.getByText(/\$300\.00/)).toBeInTheDocument();
  });

  it("findNearestPoint with empty timelineData array returns no cost in tooltip", () => {
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={[]}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerMove(track!, { clientX: 50 });

    // Empty array → findNearestPoint returns null → no cost displayed
    expect(screen.queryByText(/\$/)).toBeNull();
  });

  // GIT-R2-002: hover tooltip via findNearestPoint
  it("pointermove with timelineData shows tooltip with nearest date cost", () => {
    const timelineData: TimelinePoint[] = [
      { date: "2026-01-01", cost: 100 },
      { date: "2026-06-15", cost: 250 },
      { date: "2026-12-31", cost: 300 },
    ];

    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={timelineData}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    expect(track).not.toBeNull();
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    // Hover at leftmost position → date = "2026-01-01" → nearest = 100
    fireEvent.pointerMove(track!, { clientX: 0 });

    // Tooltip should appear with date and cost
    expect(screen.getByText(/2026-01-01/)).toBeInTheDocument();
    expect(screen.getByText(/\$100\.00/)).toBeInTheDocument();
  });

  it("pointermove without timelineData shows tooltip with date only — no cost", () => {
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={null}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    fireEvent.pointerMove(track!, { clientX: 0 });

    // Date should appear in tooltip
    expect(screen.getByText(/2026-01-01/)).toBeInTheDocument();
    // Cost portion should NOT appear (no timelineData)
    expect(screen.queryByText(/\$/)).toBeNull();
  });

  // GIT-R2-002: pointerLeave hides tooltip (handlePointerLeave)
  it("pointerLeave hides the hover tooltip", () => {
    const timelineData: TimelinePoint[] = [{ date: "2026-01-01", cost: 100 }];
    const { container } = render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        minDate="2026-01-01"
        maxDate="2026-12-31"
        timelineData={timelineData}
        disabled={false}
      />,
    );

    const track = container.querySelector("[data-testid='scrubber-track']");
    Object.defineProperty(track, "getBoundingClientRect", {
      value: () => ({
        left: 0,
        right: 100,
        width: 100,
        top: 0,
        bottom: 10,
        height: 10,
      }),
    });

    // Show tooltip
    fireEvent.pointerMove(track!, { clientX: 0 });
    expect(screen.getByText(/\$100\.00/)).toBeInTheDocument();

    // Hide tooltip via pointerLeave
    fireEvent.pointerLeave(track!);
    expect(screen.queryByText(/\$100\.00/)).toBeNull();
  });

  // GIT-R2-002: handleReplay calls onDateChange(minDate) then onPlay()
  it("clicking Replay button calls onDateChange(minDate) and onPlay()", () => {
    const onDateChange = vi.fn();
    const onPlay = vi.fn();
    render(
      <TimelineScrubber
        {...DEFAULT_PROPS}
        isAtEnd={true}
        isPlaying={false}
        minDate="2026-01-01"
        onDateChange={onDateChange}
        onPlay={onPlay}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /replay/i }));

    expect(onDateChange).toHaveBeenCalledWith("2026-01-01");
    expect(onPlay).toHaveBeenCalledTimes(1);
  });
});
