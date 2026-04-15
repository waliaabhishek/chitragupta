import type React from "react";
import type { GraphNodeWithDiff } from "./renderers/types";

interface GraphTooltipProps {
  hoveredNodeId: string | null;
  nodes: GraphNodeWithDiff[];
}

const formatCost = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

export function GraphTooltip({
  hoveredNodeId,
  nodes,
}: GraphTooltipProps): React.JSX.Element | null {
  if (!hoveredNodeId) return null;
  const node = nodes.find((n) => n.id === hoveredNodeId);
  if (!node) return null;

  const cost = formatCost.format(node.cost);
  const tagEntries = Object.entries(node.tags);
  const diff = node.diff;

  return (
    <div
      style={{
        background: "rgba(0,0,0,0.8)",
        color: "#fff",
        padding: "8px 12px",
        borderRadius: 6,
        fontSize: 12,
        pointerEvents: "none",
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 4 }}>
        {node.display_name ?? node.id}
      </div>
      <div style={{ opacity: 0.7, marginBottom: 4 }}>{node.resource_type}</div>
      <div style={{ marginBottom: diff || tagEntries.length > 0 ? 4 : 0 }}>
        Cost: {cost}
      </div>
      {diff && (
        <div style={{ marginBottom: tagEntries.length > 0 ? 4 : 0 }}>
          <div>Before: {formatCost.format(diff.cost_before)}</div>
          <div>After: {formatCost.format(diff.cost_after)}</div>
          <div
            style={{
              color: diff.cost_delta > 0 ? "#ff7875" : "#95de64",
            }}
          >
            Delta: {diff.cost_delta > 0 ? "+" : ""}
            {formatCost.format(diff.cost_delta)}
            {diff.pct_change !== null
              ? ` (${diff.cost_delta > 0 ? "+" : ""}${diff.pct_change.toFixed(1)}%)`
              : " (New)"}
          </div>
        </div>
      )}
      {tagEntries.length > 0 && (
        <div>
          {tagEntries.map(([k, v]) => (
            <div key={k} style={{ opacity: 0.8, fontSize: 11 }}>
              {k}: {v}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
