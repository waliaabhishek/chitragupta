import type React from "react";
import type { GraphNode } from "./renderers/types";

interface GraphTooltipProps {
  hoveredNodeId: string | null;
  nodes: GraphNode[];
}

export function GraphTooltip({ hoveredNodeId, nodes }: GraphTooltipProps): React.JSX.Element | null {
  if (!hoveredNodeId) return null;
  const node = nodes.find((n) => n.id === hoveredNodeId);
  if (!node) return null;

  const cost = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
  }).format(node.cost);

  const tagEntries = Object.entries(node.tags);

  return (
    <div
      style={{
        position: "absolute",
        top: 16,
        right: 16,
        background: "rgba(0,0,0,0.8)",
        color: "#fff",
        padding: "8px 12px",
        borderRadius: 6,
        fontSize: 12,
        maxWidth: 240,
        zIndex: 100,
        pointerEvents: "none",
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 4 }}>
        {node.display_name ?? node.id}
      </div>
      <div style={{ opacity: 0.7, marginBottom: 4 }}>{node.resource_type}</div>
      <div style={{ marginBottom: tagEntries.length > 0 ? 4 : 0 }}>
        Cost: {cost}
      </div>
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
