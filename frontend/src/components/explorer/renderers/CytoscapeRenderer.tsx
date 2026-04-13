import type React from "react";
import { useEffect, useRef } from "react";
import cytoscape from "cytoscape";
import coseBilkent from "cytoscape-cose-bilkent";
import type { GraphRendererProps } from "./types";
import { getStylesheet } from "./graphStyles";
import { getNodeShape, costToSize } from "./nodeShapes";

// Register layout extension once at module load time — idempotent.
cytoscape.use(coseBilkent);

export function CytoscapeRenderer({
  nodes,
  edges,
  fadedNodeIds,
  onNodeClick,
  onNodeHover,
  isLoading,
  isDark,
  width,
  height,
}: GraphRendererProps): React.JSX.Element {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

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
      exitingEles.animate(
        { style: { opacity: 0 } },
        { duration: 300 },
      );
      setTimeout(() => exitingEles.remove(), 320);
    }

    for (const node of persisting) {
      const cyNode = cy.getElementById(node.id);
      cyNode.data({
        ...node,
        size: costToSize(node.cost, minCost, maxCost),
        shape: getNodeShape(node.resource_type),
      });
    }

    for (const node of entering) {
      cy.add({
        group: "nodes",
        data: {
          ...node,
          size: costToSize(node.cost, minCost, maxCost),
          shape: getNodeShape(node.resource_type),
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
      const edgeId = `edge-${edge.source}-${edge.target}`;
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

    if (nodes.length > 0) {
      cy.layout({
        name: "cose-bilkent",
        animate: true,
        animationDuration: 500,
        fit: true,
        padding: 40,
        nodeRepulsion: 8000,
        idealEdgeLength: 120,
      } as Parameters<typeof cy.layout>[0]).run();
    }
  }, [nodes, edges]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div style={{ position: "relative", width, height }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
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
    </div>
  );
}
