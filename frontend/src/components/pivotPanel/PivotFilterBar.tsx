import type React from "react";
import { Tag } from "antd";
import { AddFilterPopover } from "./AddFilterPopover";

interface PivotFilterBarProps {
  tenantName: string;
  tagKey: string;
  activeFilters: string[];
  onFilterAdd: (value: string) => void;
  onRemove: (value: string) => void;
}

export function PivotFilterBar({
  tenantName,
  tagKey,
  activeFilters,
  onFilterAdd,
  onRemove,
}: PivotFilterBarProps): React.JSX.Element {
  return (
    <div
      style={{
        padding: "4px 0 8px 0",
        display: "flex",
        flexWrap: "wrap",
        gap: 4,
        alignItems: "center",
      }}
    >
      {activeFilters.map((val) => (
        <Tag key={val} closable color="blue" onClose={() => onRemove(val)}>
          {tagKey}={val}
        </Tag>
      ))}
      <AddFilterPopover
        tenantName={tenantName}
        tagKey={tagKey}
        activeTagFilters={activeFilters}
        onFilterAdd={onFilterAdd}
      />
    </div>
  );
}
