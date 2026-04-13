import type React from "react";
import { useEffect, useRef, useState } from "react";
import { Typography } from "antd";
import { useTenant } from "../../providers/TenantContext";
import { useAppShell } from "../../contexts/AppShellContext";
import { useGraphData } from "../../hooks/useGraphData";
import { useGraphNavigation } from "../../hooks/useGraphNavigation";
import { GraphContainer } from "./GraphContainer";
import { GraphTooltip } from "./GraphTooltip";
import { BreadcrumbTrail } from "./BreadcrumbTrail";
import type { GraphNode, GraphEdge } from "./renderers/types";

const { Text } = Typography;

function enrichWithPhantomNodes(
  nodes: GraphNode[],
  edges: GraphEdge[],
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const existingIds = new Set(nodes.map((n) => n.id));
  const phantomNodes: GraphNode[] = [];
  const phantomEdges: GraphEdge[] = [];

  for (const node of nodes) {
    for (const refId of node.cross_references) {
      if (!existingIds.has(refId)) {
        existingIds.add(refId);
        phantomNodes.push({
          id: refId,
          resource_type: "kafka_cluster",
          display_name: null,
          cost: 0,
          created_at: null,
          deleted_at: null,
          tags: {},
          parent_id: null,
          cloud: null,
          region: null,
          status: "phantom",
          cross_references: [],
        });
        phantomEdges.push({
          source: refId,
          target: node.id,
          relationship_type: "charge",
          cost: null,
        });
      }
    }
  }

  return {
    nodes: [...nodes, ...phantomNodes],
    edges: [...edges, ...phantomEdges],
  };
}

export function ExplorerPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { isDark, setSidebarCollapsed } = useAppShell();
  const { state, navigate, goBack, goToRoot, goToBreadcrumb } =
    useGraphNavigation();
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);

  const tenantName = currentTenant?.tenant_name ?? null;

  const { data, isLoading, error } = useGraphData({
    tenantName,
    focus: state.focusId,
  });

  // Collapse sidebar on enter, restore on leave — collapseRef avoids exhaustive-deps warning
  const collapseRef = useRef(setSidebarCollapsed);
  useEffect(() => {
    collapseRef.current = setSidebarCollapsed;
  });
  useEffect(() => {
    collapseRef.current(true);
    return () => {
      collapseRef.current(false);
    };
  }, []);

  const rawNodes = data?.nodes ?? [];
  const rawEdges = data?.edges ?? [];

  const { nodes: enrichedNodes, edges: enrichedEdges } = enrichWithPhantomNodes(
    rawNodes,
    rawEdges,
  );

  const fadedNodeIds = new Set<string>();

  function handleNodeClick(nodeId: string, resourceType: string) {
    const node = enrichedNodes.find((n) => n.id === nodeId);
    navigate(nodeId, resourceType, node?.display_name ?? null);
  }

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        padding: 0,
        margin: 0,
        overflow: "hidden",
        position: "relative",
      }}
    >
      <BreadcrumbTrail
        breadcrumbs={state.breadcrumbs}
        onNavigate={goToBreadcrumb}
        onGoBack={goBack}
        onGoToRoot={goToRoot}
      />
      <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
        <GraphContainer
          nodes={enrichedNodes}
          edges={enrichedEdges}
          focusId={state.focusId}
          fadedNodeIds={fadedNodeIds}
          onNodeClick={handleNodeClick}
          onNodeHover={setHoveredNodeId}
          isLoading={isLoading}
          isDark={isDark}
        />
        <GraphTooltip hoveredNodeId={hoveredNodeId} nodes={enrichedNodes} />
        {isLoading && (
          <div
            data-testid="graph-loading"
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              background: "rgba(0,0,0,0.2)",
            }}
          >
            <div role="progressbar" style={{ fontSize: 24 }}>
              ⟳
            </div>
          </div>
        )}
        {!tenantName && !isLoading && !error && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 24,
            }}
          >
            <Text type="secondary">Select a tenant to explore the cost graph.</Text>
          </div>
        )}
        {error && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 24,
            }}
          >
            <Text type="danger">{error}</Text>
          </div>
        )}
        {tenantName && !isLoading && !error && enrichedNodes.length === 0 && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 24,
            }}
          >
            <Text type="secondary">No resources found for this time period.</Text>
          </div>
        )}
      </div>
    </div>
  );
}
