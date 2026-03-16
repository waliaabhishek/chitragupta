import type React from "react";
import { Button, Space, Tooltip, Typography } from "antd";

interface SelectionToolbarProps {
  selectedCount: number;
  isSelectAllMode: boolean;
  totalCount: number;
  onClear: () => void;
  onAddTags: () => void;
  disabled?: boolean;
}

export function SelectionToolbar({
  selectedCount,
  isSelectAllMode,
  totalCount,
  onClear,
  onAddTags,
  disabled,
}: SelectionToolbarProps): React.JSX.Element {
  const label = isSelectAllMode
    ? `All ${totalCount} matching rows selected`
    : `${selectedCount} selected`;

  return (
    <div
      style={{
        padding: "8px 12px",
        background: "#e6f4ff",
        borderRadius: 4,
        marginBottom: 8,
        display: "flex",
        alignItems: "center",
        gap: 12,
      }}
    >
      <Typography.Text strong>{label}</Typography.Text>
      <Space>
        <Button size="small" onClick={onClear}>
          Clear Selection
        </Button>
        <Tooltip title={disabled ? "Read-only while pipeline is running" : undefined}>
          <Button size="small" type="primary" onClick={onAddTags} disabled={disabled}>
            Add Tags
          </Button>
        </Tooltip>
      </Space>
    </div>
  );
}
