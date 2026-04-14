import type React from "react";
import { Select, Tooltip } from "antd";

interface TagOverlayPanelProps {
  availableKeys: string[];
  isLoadingKeys: boolean;
  activeKey: string | null;
  onKeyChange: (key: string | null) => void;
  colorMap: Record<string, string>;
  selectedValue: string | null;
  onValueClick: (value: string | null) => void;
  isDark: boolean;
}

export function TagOverlayPanel({
  availableKeys,
  isLoadingKeys,
  activeKey,
  onKeyChange,
  colorMap,
  selectedValue,
  onValueClick,
  isDark,
}: TagOverlayPanelProps): React.JSX.Element {
  const noKeys = availableKeys.length === 0;

  const containerStyle: React.CSSProperties = {
    background: isDark ? "rgba(0,0,0,0.7)" : "rgba(255,255,255,0.92)",
    border: isDark ? "1px solid #444" : "1px solid #d9d9d9",
    borderRadius: 6,
    padding: "8px 10px",
    minWidth: 180,
  };

  return (
    <div style={containerStyle}>
      <Select
        value={activeKey}
        onChange={onKeyChange}
        allowClear
        disabled={noKeys || isLoadingKeys}
        placeholder={noKeys ? "No tags available" : "Select tag key"}
        options={availableKeys.map((k) => ({ label: k, value: k }))}
        style={{ width: "100%" }}
      />
      {activeKey && Object.keys(colorMap).length > 0 && (
        <div style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 4 }}>
          {Object.entries(colorMap).map(([value, color]) => (
            <Tooltip key={value} title={value}>
              <div
                onClick={() => onValueClick(selectedValue === value ? null : value)}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  cursor: "pointer",
                  padding: "2px 4px",
                  borderRadius: 3,
                  background:
                    selectedValue === value
                      ? isDark
                        ? "rgba(255,255,255,0.1)"
                        : "rgba(0,0,0,0.06)"
                      : "transparent",
                }}
              >
                <span
                  style={{
                    display: "inline-block",
                    width: 10,
                    height: 10,
                    borderRadius: "50%",
                    background: color,
                    flexShrink: 0,
                  }}
                />
                <span style={{ fontSize: 12, opacity: 0.85 }}>{value}</span>
              </div>
            </Tooltip>
          ))}
        </div>
      )}
    </div>
  );
}
