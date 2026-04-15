import type React from "react";
import { useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import coseBilkent from "cytoscape-cose-bilkent";
import type { GraphRendererProps } from "./types";
import { getStylesheet } from "./graphStyles";
import { getNodeShape, getNodeSize } from "./nodeShapes";

// Register layout extension once at module load time — idempotent.
cytoscape.use(coseBilkent);

function computeNodeLabel(node: GraphRendererProps["nodes"][number]): string {
  const { resource_type, child_count, child_total_cost, display_name, id } = node;
  if (resource_type === "topic_group") {
    const count = child_count ?? "?";
    const cost = child_total_cost != null ? `$${child_total_cost.toFixed(2)}` : "";
    return cost ? `${count} topics\n${cost} total` : `${count} topics`;
  }
  if (resource_type === "identity_group") {
    const count = child_count ?? "?";
    const cost = child_total_cost != null ? `$${child_total_cost.toFixed(2)}` : "";
    return cost ? `${count} users\n${cost} total` : `${count} users`;
  }
  if (resource_type === "zero_cost_summary") {
    const count = child_count ?? "?";
    return `${count} others at $0`;
  }
  if (resource_type === "capped_summary") {
    const count = child_count ?? "?";
    return `${count} more (capped)`;
  }
  if (resource_type === "resource_group") {
    const count = child_count ?? "?";
    const cost = child_total_cost != null ? `$${child_total_cost.toFixed(2)}` : "";
    return cost ? `${count} resources\n${cost} total` : `${count} resources`;
  }
  if (resource_type === "cluster_group") {
    const count = child_count ?? "?";
    const cost = child_total_cost != null ? `$${child_total_cost.toFixed(2)}` : "";
    return cost ? `${count} clusters\n${cost} total` : `${count} clusters`;
  }
  if (resource_type === "xref_group") {
    const count = child_count ?? "?";
    return `${count} more`;
  }
  return display_name ?? id;
}

interface PulseOverlay {
  id: string;
  x: number;
  y: number;
  size: number;
}

export function CytoscapeRenderer({
  nodes,
  edges,
  fadedNodeIds,
  onNodeClick,
  onNodeHover,
  isDark,
  width,
  height,
  activeTagKey,
  tagSelectedValue,
}: GraphRendererProps): React.JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);
  const [pulseOverlays, setPulseOverlays] = useState<PulseOverlay[]>([]);

  // Stable refs for callbacks — prevent stale closures in mount-once useEffect
  const onClickRef = useRef(onNodeClick);
  const onHoverRef = useRef(onNodeHover);

  useEffect(() => {
    onClickRef.current = onNodeClick;
  });
  useEffect(() => {
    onHoverRef.current = onNodeHover;
  });

  // Track edge IDs for manual management
  const edgeIdsRef = useRef<Set<string>>(new Set());

  // Ref to clean up exiting-node timeout on unmount
  const exitTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Mount once — create cy instance
  useEffect(() => {
    if (!containerRef.current) return;
    let cy: cytoscape.Core | null = null;
    try {
      cy = cytoscape({
        container: containerRef.current,
        style: getStylesheet(isDark),
        layout: { name: "preset" },
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false,
      });
    } catch {
      // jsdom doesn't support canvas — skip in test/non-browser environments
    }

    if (!cy) return;
    cyRef.current = cy;

    const cyInstance = cy;
    cyInstance.on("tap", "node", (evt) => {
      const node = evt.target;
      onClickRef.current(node.id(), node.data("resource_type"));
    });
    cyInstance.on("mouseover", "node", (evt) =>
      onHoverRef.current(evt.target.id()),
    );
    cyInstance.on("mouseout", "node", () => onHoverRef.current(null));

    return () => {
      if (exitTimeoutRef.current !== null) {
        clearTimeout(exitTimeoutRef.current);
        exitTimeoutRef.current = null;
      }
      cyInstance.destroy();
      cyRef.current = null;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Update stylesheet when dark mode changes
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.style().fromJson(getStylesheet(isDark)).update();
  }, [isDark]);

  // Resize container when dimensions change
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy || typeof cy.resize !== "function") return;
    cy.resize();
  }, [width, height]);

  // Constrained re-layout when tag value selection changes
  const isFirstTagEffect = useRef(true);
  useEffect(() => {
    if (isFirstTagEffect.current) {
      isFirstTagEffect.current = false;
      return;
    }
    const cy = cyRef.current;
    if (!cy || cy.nodes().length === 0) return;

    if (!activeTagKey || !tagSelectedValue) {
      cy.layout({
        name: "cose-bilkent",
        animate: true,
        animationDuration: 400,
        fit: true,
        padding: 40,
        nodeRepulsion: 8000,
        idealEdgeLength: 120,
      } as Parameters<typeof cy.layout>[0]).run();
      return;
    }

    cy.layout({
      name: "cose",
      animate: true,
      animationDuration: 500,
      fit: true,
      padding: 40,
      nodeRepulsion: () => 6000,
      idealEdgeLength: (edge: cytoscape.EdgeSingular) => {
        const srcTags = (edge.source().data("tags") as Record<string, string>) ?? {};
        const tgtTags = (edge.target().data("tags") as Record<string, string>) ?? {};
        const srcMatch = srcTags[activeTagKey] === tagSelectedValue;
        const tgtMatch = tgtTags[activeTagKey] === tagSelectedValue;
        if (srcMatch && tgtMatch) return 50;
        if (!srcMatch && !tgtMatch) return 200;
        return 120;
      },
    } as Parameters<typeof cy.layout>[0]).run();
  }, [activeTagKey, tagSelectedValue]); // scalars — reference-stable, no spurious re-runs

  // Data diffing — animate transitions on node/edge changes
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    const costs = nodes.map((n) => n.cost);
    const minCost = Math.min(...costs, 0);
    const maxCost = Math.max(...costs, 0);

    const prevIds = new Set(cy.nodes().map((n) => n.id()));
    const nextIds = new Set(nodes.map((n) => n.id));

    const exiting = [...prevIds].filter((id) => !nextIds.has(id));
    const exitingSet = new Set(exiting);
    const entering = nodes.filter((n) => !prevIds.has(n.id));
    const persisting = nodes.filter((n) => prevIds.has(n.id));

    if (exiting.length > 0) {
      const exitingEles = cy.nodes().filter((n) => exitingSet.has(n.id()));
      exitingEles.animate({ style: { opacity: 0 } }, { duration: 300 });
      if (exitTimeoutRef.current !== null) {
        clearTimeout(exitTimeoutRef.current);
      }
      exitTimeoutRef.current = setTimeout(() => {
        exitingEles.remove();
        exitTimeoutRef.current = null;
      }, 320);
    }

    for (const node of persisting) {
      const cyNode = cy.getElementById(node.id);
      cyNode.data({
        ...node,
        size: getNodeSize(node.resource_type, node.cost, minCost, maxCost),
        shape: getNodeShape(node.resource_type),
        label: computeNodeLabel(node),
      });
    }

    for (const node of entering) {
      cy.add({
        group: "nodes",
        data: {
          ...node,
          size: getNodeSize(node.resource_type, node.cost, minCost, maxCost),
          shape: getNodeShape(node.resource_type),
          label: computeNodeLabel(node),
        },
        position: { x: 0, y: 0 },
      });
    }

    // Remove old edges, add new ones
    for (const edgeId of edgeIdsRef.current) {
      const el = cy.getElementById(edgeId);
      if (el && typeof el.remove === "function") el.remove();
    }
    edgeIdsRef.current = new Set();

    for (const edge of edges) {
      const edgeId = `edge-${edge.source}-${edge.target}-${edge.relationship_type}`;
      cy.add({
        group: "edges",
        data: {
          id: edgeId,
          source: edge.source,
          target: edge.target,
          relationship_type: edge.relationship_type,
          cost: edge.cost,
        },
      });
      edgeIdsRef.current.add(edgeId);
    }

    // Apply fading
    cy.nodes().removeClass("faded");
    if (fadedNodeIds.size > 0) {
      cy.nodes()
        .filter((n) => fadedNodeIds.has(n.id()))
        .addClass("faded");
    }

    // Apply diff classes from node data
    cy.nodes().removeClass("diff-increase diff-decrease diff-new diff-deleted");
    for (const node of [...persisting, ...entering]) {
      const cyNode = cy.getElementById(node.id);
      const diffStatus = node.diff?.diff_status;
      if (!diffStatus || diffStatus === "unchanged") continue;
      if (diffStatus === "changed") {
        cyNode.addClass(
          node.diff!.cost_delta > 0 ? "diff-increase" : "diff-decrease",
        );
      } else if (diffStatus === "new") {
        cyNode.addClass("diff-new");
      } else if (diffStatus === "deleted") {
        cyNode.addClass("diff-deleted");
      }
    }

    // Clear old pulse overlays before layout runs
    setPulseOverlays([]);

    if (nodes.length > 0) {
      const layout = cy.layout({
        name: "cose-bilkent",
        animate: true,
        animationDuration: 500,
        fit: true,
        padding: 40,
        nodeRepulsion: 8000,
        idealEdgeLength: 120,
      } as Parameters<typeof cy.layout>[0]);

      // After layout completes, compute pulse overlay positions for diff-new nodes
      // Runtime guard needed: test mock cy does not implement .one()
      if (typeof cy.one === "function") {
        cy.one("layoutstop", () => {
          const newNodes = cy.nodes(".diff-new");
          const overlays: PulseOverlay[] = [];
          newNodes.forEach((n) => {
            const pos = n.renderedPosition();
            const size = n.renderedStyle("width") as unknown as number;
            overlays.push({
              id: n.id(),
              x: pos.x,
              y: pos.y,
              size: typeof size === "number" ? size : 30,
            });
          });
          setPulseOverlays(overlays);
        });
      }

      layout.run();
    }
  }, [nodes, edges]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div style={{ position: "relative", width, height }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
      {pulseOverlays.map((o) => (
        <div
          key={o.id}
          style={{
            position: "absolute",
            left: o.x,
            top: o.y,
            width: o.size,
            height: o.size,
            borderRadius: "50%",
            border: "2px solid #1890ff",
            transform: "translate(-50%, -50%)",
            pointerEvents: "none",
            animation: "diff-new-pulse 1.2s ease-out infinite",
          }}
        />
      ))}
      <style>{`
        @keyframes diff-new-pulse {
          0% { transform: translate(-50%, -50%) scale(1); opacity: 0.8; }
          70% { transform: translate(-50%, -50%) scale(1.15); opacity: 0.3; }
          100% { transform: translate(-50%, -50%) scale(1); opacity: 0; }
        }
      `}</style>
    </div>
  );
}
