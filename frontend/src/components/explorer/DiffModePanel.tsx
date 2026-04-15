import type React from "react";
import { useState } from "react";
import { DatePicker } from "antd";
import dayjs from "dayjs";
import { addDays } from "../../utils/dateUtils";

interface DiffModePanelProps {
  isActive: boolean;
  onToggle: () => void;
  fromRange: [string, string] | null;
  toRange: [string, string] | null;
  onRangesChange: (from: [string, string], to: [string, string]) => void;
  minDate: string | null;
  maxDate: string | null;
  isDark: boolean;
}

type DateRange = [string, string];

function presetWeekOverWeek(ref: string): { from: DateRange; to: DateRange } {
  return {
    to: [addDays(ref, -6), ref],
    from: [addDays(ref, -13), addDays(ref, -7)],
  };
}

function presetMonthOverMonth(ref: string): { from: DateRange; to: DateRange } {
  return {
    to: [addDays(ref, -29), ref],
    from: [addDays(ref, -59), addDays(ref, -30)],
  };
}

function presetLast30d(ref: string): { from: DateRange; to: DateRange } {
  const toEnd = ref;
  const toStart = addDays(ref, -29);
  const fromEnd = addDays(toStart, -1);
  const fromStart = addDays(fromEnd, -29);
  return {
    to: [toStart, toEnd],
    from: [fromStart, fromEnd],
  };
}

export function DiffModePanel({
  isActive,
  onToggle,
  fromRange,
  toRange,
  onRangesChange,
  minDate,
  maxDate,
  isDark,
}: DiffModePanelProps): React.JSX.Element {
  const [showCustom, setShowCustom] = useState(false);
  const disabled = minDate === null || maxDate === null;
  const refDate = maxDate ?? new Date().toISOString().split("T")[0];

  function applyPreset(preset: { from: DateRange; to: DateRange }) {
    onRangesChange(preset.from, preset.to);
  }

  const panelBg = isDark ? "#1f1f2e" : "#fff";
  const panelBorder = isDark ? "#333" : "#d9d9d9";
  const panelColor = isDark ? "#e0e0e0" : undefined;

  return (
    <div style={{ position: "relative" }}>
      <button
        className={isActive ? "diff-toggle diff-toggle--active" : "diff-toggle"}
        aria-label={isActive ? "Disable diff compare" : "Enable diff compare"}
        onClick={onToggle}
        style={{
          background: isActive ? "#1890ff" : "rgba(0,0,0,0.06)",
          color: isActive ? "#fff" : undefined,
          border: isActive ? "1px solid #1890ff" : "1px solid #d9d9d9",
          borderRadius: 4,
          padding: "4px 8px",
          cursor: "pointer",
          fontSize: 12,
        }}
      >
        {isActive ? "Compare: On" : "Compare"}
      </button>

      {isActive && (
        <div
          data-testid="diff-mode-panel"
          style={{
            position: "absolute",
            top: "100%",
            left: 0,
            background: panelBg,
            border: `1px solid ${panelBorder}`,
            borderRadius: 6,
            padding: 12,
            zIndex: 300,
            minWidth: 280,
            boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
            color: panelColor,
          }}
        >
          <div
            style={{
              display: "flex",
              gap: 8,
              marginBottom: 8,
              flexWrap: "wrap",
            }}
          >
            <button
              disabled={disabled}
              aria-disabled={disabled ? "true" : undefined}
              onClick={() => {
                setShowCustom(false);
                applyPreset(presetWeekOverWeek(refDate));
              }}
              style={{ fontSize: 12, cursor: disabled ? "default" : "pointer" }}
            >
              Week over week
            </button>
            <button
              disabled={disabled}
              aria-disabled={disabled ? "true" : undefined}
              onClick={() => {
                setShowCustom(false);
                applyPreset(presetMonthOverMonth(refDate));
              }}
              style={{ fontSize: 12, cursor: disabled ? "default" : "pointer" }}
            >
              Month over month
            </button>
            <button
              disabled={disabled}
              aria-disabled={disabled ? "true" : undefined}
              onClick={() => {
                setShowCustom(false);
                applyPreset(presetLast30d(refDate));
              }}
              style={{ fontSize: 12, cursor: disabled ? "default" : "pointer" }}
            >
              Last 30d vs previous 30d
            </button>
            <button
              disabled={disabled}
              aria-disabled={disabled ? "true" : undefined}
              onClick={() => setShowCustom((v) => !v)}
              style={{ fontSize: 12, cursor: disabled ? "default" : "pointer" }}
            >
              Custom
            </button>
          </div>

          {showCustom && (
            <div style={{ display: "flex", gap: 8, flexDirection: "column" }}>
              <div>
                <label style={{ fontSize: 11 }}>From range</label>
                <div data-testid="from-range-picker" style={{ marginTop: 4 }}>
                  <DatePicker.RangePicker
                    disabled={disabled}
                    value={
                      fromRange
                        ? [dayjs(fromRange[0]), dayjs(fromRange[1])]
                        : null
                    }
                    disabledDate={(current) => {
                      if (!minDate || !maxDate) return false;
                      return (
                        current.isBefore(dayjs(minDate), "day") ||
                        current.isAfter(dayjs(maxDate), "day")
                      );
                    }}
                    onChange={(_, dateStrings) => {
                      const [start, end] = dateStrings;
                      if (start && end) {
                        onRangesChange(
                          [start, end],
                          toRange ?? [start, start],
                        );
                      }
                    }}
                  />
                </div>
              </div>
              <div>
                <label style={{ fontSize: 11 }}>To range</label>
                <div data-testid="to-range-picker" style={{ marginTop: 4 }}>
                  <DatePicker.RangePicker
                    disabled={disabled}
                    value={
                      toRange
                        ? [dayjs(toRange[0]), dayjs(toRange[1])]
                        : null
                    }
                    disabledDate={(current) => {
                      if (!minDate || !maxDate) return false;
                      return (
                        current.isBefore(dayjs(minDate), "day") ||
                        current.isAfter(dayjs(maxDate), "day")
                      );
                    }}
                    onChange={(_, dateStrings) => {
                      const [start, end] = dateStrings;
                      if (start && end) {
                        onRangesChange(
                          fromRange ?? [start, start],
                          [start, end],
                        );
                      }
                    }}
                  />
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
