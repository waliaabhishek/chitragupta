import { Button, Space, Typography } from "antd";

interface SelectionToolbarProps {
  selectedCount: number;
  isSelectAllMode: boolean;
  totalCount: number;
  onClear: () => void;
  onAddTags: () => void;
}

export function SelectionToolbar({
  selectedCount,
  isSelectAllMode,
  totalCount,
  onClear,
  onAddTags,
}: SelectionToolbarProps): JSX.Element {
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
        <Button size="small" type="primary" onClick={onAddTags}>
          Add Tags
        </Button>
      </Space>
    </div>
  );
}
