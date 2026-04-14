export interface GraphNode {
  id: string;
  resource_type: string;
  display_name: string | null;
  cost: number;
  created_at: string | null;
  deleted_at: string | null;
  tags: Record<string, string>;
  parent_id: string | null;
  cloud: string | null;
  region: string | null;
  status: string;
  cross_references: string[];
}

export interface GraphEdge {
  source: string;
  target: string;
  relationship_type: "parent" | "charge" | "attribution";
  cost: number | null;
}

/** Diff overlay data merged onto a GraphNode */
export interface DiffOverlay {
  cost_before: number;
  cost_after: number;
  cost_delta: number;
  pct_change: number | null;
  diff_status: "new" | "deleted" | "changed" | "unchanged";
}

/** GraphNode extended with optional diff data */
export interface GraphNodeWithDiff extends GraphNode {
  diff?: DiffOverlay;
}

export interface GraphRendererProps {
  nodes: GraphNodeWithDiff[];
  edges: GraphEdge[];
  focusId: string | null;
  fadedNodeIds: Set<string>;
  onNodeClick: (nodeId: string, resourceType: string) => void;
  onNodeHover: (nodeId: string | null) => void;
  isDark: boolean;
  width: number;
  height: number;
}
