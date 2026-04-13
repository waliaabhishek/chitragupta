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

export interface GraphRendererProps {
  nodes: GraphNode[];
  edges: GraphEdge[];
  focusId: string | null;
  fadedNodeIds: Set<string>;
  onNodeClick: (nodeId: string, resourceType: string) => void;
  onNodeHover: (nodeId: string | null) => void;
  isLoading: boolean;
  isDark: boolean;
  width: number;
  height: number;
}
