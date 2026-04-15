import type React from "react";
import { useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import d3Force from "cytoscape-d3-force";
import type { GraphRendererProps } from "./types";
import { getStylesheet } from "./graphStyles";
import { getNodeShape, getNodeSize } from "./nodeShapes";

// Register layout extension once at module load time — idempotent.
cytoscape.use(d3Force);

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

// d3-force node type — after simulation init, source/target on links become
// node objects with the cytoscape data merged in via Object.assign.
interface D3Node {
  id: string;
  size?: number;
}
interface D3Link {
  source: D3Node;
  target: D3Node;
}

// ---------------------------------------------------------------------------
// Force parameters — tuned for our hub-and-spoke topology (~10-20 nodes,
// one focal node with radiating neighbors, cost-scaled 10-30px sizes).
//
//   linkDistance 150 → ring ~150px from focus, ~78px spacing per neighbor
//   manyBody -120    → enough to spread 15 nodes with room for labels
//   linkStrength 0.5 → moderate — keeps structure without rigid propagation
//   no collision     → repel handles separation at this node count
//   velocityDecay 0.4, alphaDecay 0.0228 → smooth ~300-iteration settle
//   fit: false       → fitToFocus handles viewport after settle
// ---------------------------------------------------------------------------

/** Shared d3-force layout options for the standard (non-tag-filtered) view. */
function standardLayoutOptions() {
  return {
    name: "d3-force" as const,
    animate: true,
    infinite: true,
    randomize: false, // we always pre-position nodes (radial or preserved)
    fit: false,
    padding: 40,
    linkId: (d: D3Node) => d.id,
    linkDistance: 150,
    linkStrength: 0.5,
    manyBodyStrength: -120,
    collideRadius: 1,
    collideStrength: 0.001,
    collideIterations: 1,
    alphaDecay: 0.0228,
    velocityDecay: 0.4,
    // Kill the default forceX/forceY toward (0,0) — the adapter always creates
    // them, and without explicit xX/yY they pull toward the top-left corner at
    // strength 0.1, competing with forceCenter at (w/2,h/2) and biasing the
    // layout into a semicircle.  Can't use 0 (falsy → default 0.1 stays).
    xStrength: 0.001,
    yStrength: 0.001,
  };
}

/** Tag-filtered layout: matching nodes pull together, non-matching push apart. */
function tagFilteredLayoutOptions(
  activeTagKey: string,
  tagSelectedValue: string,
  cy: cytoscape.Core,
) {
  return {
    name: "d3-force" as const,
    animate: true,
    infinite: true,
    randomize: false,
    fit: false,
    padding: 40,
    linkId: (d: D3Node) => d.id,
    linkDistance: (d: D3Link) => {
      const srcNode = cy.getElementById(d.source.id ?? (d.source as unknown as string));
      const tgtNode = cy.getElementById(d.target.id ?? (d.target as unknown as string));
      const srcTags = (srcNode.data("tags") as Record<string, string>) ?? {};
      const tgtTags = (tgtNode.data("tags") as Record<string, string>) ?? {};
      const srcMatch = srcTags[activeTagKey] === tagSelectedValue;
      const tgtMatch = tgtTags[activeTagKey] === tagSelectedValue;
      if (srcMatch && tgtMatch) return 80;
      if (!srcMatch && !tgtMatch) return 280;
      return 180;
    },
    linkStrength: 0.5,
    manyBodyStrength: -120,
    collideRadius: 1,
    collideStrength: 0.001,
    collideIterations: 1,
    alphaDecay: 0.0228,
    velocityDecay: 0.4,
    xStrength: 0.001,
    yStrength: 0.001,
  };
}

/** Smoothly fit all elements into view, centering on the focus node if present. */
function fitToFocus(cy: cytoscape.Core, focusId: string | null) {
  if (typeof cy.elements !== "function" || cy.nodes().length === 0) return;
  cy.animate({
    fit: { eles: cy.elements(), padding: 40 },
    duration: 400,
    easing: "ease-out",
    complete: () => {
      if (!focusId) return;
      const focusNode = cy.getElementById(focusId);
      if (focusNode && focusNode.isNode()) {
        cy.animate({
          center: { eles: focusNode },
          duration: 200,
          easing: "ease-out",
        });
      }
    },
  });
}

/** Compute pulse overlay positions for diff-new nodes. */
function computePulseOverlays(cy: cytoscape.Core): PulseOverlay[] {
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
  return overlays;
}

export function CytoscapeRenderer({
  nodes,
  edges,
  focusId,
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

  // Stable refs for callbacks and focusId — prevent stale closures
  const onClickRef = useRef(onNodeClick);
  const onHoverRef = useRef(onNodeHover);
  const focusIdRef = useRef(focusId);

  useEffect(() => {
    onClickRef.current = onNodeClick;
  });
  useEffect(() => {
    onHoverRef.current = onNodeHover;
  });
  useEffect(() => {
    focusIdRef.current = focusId;
  });

  // Track edge IDs for manual management
  const edgeIdsRef = useRef<Set<string>>(new Set());

  // Ref to clean up exiting-node timeout on unmount
  const exitTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Track active layout so we can stop it before starting a new one
  const layoutRef = useRef<cytoscape.Layouts | null>(null);

  // Track whether initial layout has been performed — subsequent updates
  // preserve positions instead of re-randomizing.
  const hasInitialLayoutRef = useRef(false);

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
      if (layoutRef.current) {
        layoutRef.current.stop();
        layoutRef.current = null;
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

    // Stop any running simulation before starting a new one
    if (layoutRef.current) {
      layoutRef.current.stop();
    }

    if (!activeTagKey || !tagSelectedValue) {
      const layout = cy.layout(
        standardLayoutOptions() as Parameters<typeof cy.layout>[0],
      );
      layoutRef.current = layout;
      layout.run();
      return;
    }

    const layout = cy.layout(
      tagFilteredLayoutOptions(activeTagKey, tagSelectedValue, cy) as Parameters<
        typeof cy.layout
      >[0],
    );
    layoutRef.current = layout;
    layout.run();
  }, [activeTagKey, tagSelectedValue]); // scalars — reference-stable, no spurious re-runs

  // Data diffing — surgically update nodes/edges, only restart layout when
  // the topology (set of node IDs) actually changes.  During timelapse or
  // cost-only refreshes the simulation keeps running undisturbed.
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

    const topologyChanged = entering.length > 0 || exiting.length > 0;

    // --- Exiting nodes: fade out then remove ---
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

    // --- Persisting nodes: update data in place ---
    for (const node of persisting) {
      const cyNode = cy.getElementById(node.id);
      cyNode.data({
        ...node,
        size: getNodeSize(node.resource_type, node.cost, minCost, maxCost),
        shape: getNodeShape(node.resource_type),
        label: computeNodeLabel(node),

      });
    }

    // --- Entering nodes: position near a connected neighbor, fade in ---
    for (const node of entering) {
      let pos = { x: 0, y: 0 };

      // Try to find a persisting neighbor to cluster near
      const connectedEdge = edges.find(
        (e) =>
          (e.source === node.id &&
            prevIds.has(e.target) &&
            nextIds.has(e.target)) ||
          (e.target === node.id &&
            prevIds.has(e.source) &&
            nextIds.has(e.source)),
      );
      if (connectedEdge) {
        const neighborId =
          connectedEdge.source === node.id
            ? connectedEdge.target
            : connectedEdge.source;
        const neighborNode = cy.getElementById(neighborId);
        if (neighborNode && neighborNode.isNode()) {
          const np = neighborNode.position();
          pos = {
            x: np.x + (Math.random() - 0.5) * 80,
            y: np.y + (Math.random() - 0.5) * 80,
          };
        }
      } else if (focusIdRef.current) {
        // Fallback: position near focused node
        const focusNode = cy.getElementById(focusIdRef.current);
        if (focusNode && focusNode.isNode()) {
          const fp = focusNode.position();
          pos = {
            x: fp.x + (Math.random() - 0.5) * 120,
            y: fp.y + (Math.random() - 0.5) * 120,
          };
        }
      }

      const cyNode = cy.add({
        group: "nodes",
        data: {
          ...node,
          size: getNodeSize(node.resource_type, node.cost, minCost, maxCost),
          shape: getNodeShape(node.resource_type),
          label: computeNodeLabel(node),
  
        },
        position: pos,
      });
      // Fade in — guard for test environments where cy.add returns undefined
      if (cyNode && typeof cyNode.style === "function") {
        cyNode.style("opacity", 0);
        cyNode.animate({ style: { opacity: 1 } }, { duration: 400 });
      }
    }

    // --- Edges: remove old, add new ---
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

    // --- Apply fading ---
    cy.nodes().removeClass("faded");
    if (fadedNodeIds.size > 0) {
      cy.nodes()
        .filter((n) => fadedNodeIds.has(n.id()))
        .addClass("faded");
    }

    // --- Apply diff classes ---
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

    setPulseOverlays([]);

    if (nodes.length === 0) return;

    // --- Layout decision ---
    const isFirstLayout = !hasInitialLayoutRef.current;
    const isFullReset = isFirstLayout || persisting.length === 0;

    if (isFullReset) {
      // Pre-position nodes radially so the simulation starts from a meaningful
      // layout.  With randomize: false the simulation uses these positions,
      // letting us call fitToFocus immediately (no blank canvas).
      const cx = typeof cy.width === "function" ? cy.width() / 2 : 400;
      const ch = typeof cy.height === "function" ? cy.height() / 2 : 300;
      const focusCyNode = focusIdRef.current
        ? cy.getElementById(focusIdRef.current)
        : null;
      const hasFocus = focusCyNode && focusCyNode.isNode();
      if (hasFocus) {
        focusCyNode.position({ x: cx, y: ch });
        const others = cy.nodes().filter((n) => n.id() !== focusIdRef.current);
        const count = others.length;
        if (count > 0) {
          const radius = 150; // matches linkDistance
          others.forEach((node, i) => {
            const angle = (2 * Math.PI * i) / count - Math.PI / 2;
            node.position({
              x: cx + radius * Math.cos(angle),
              y: ch + radius * Math.sin(angle),
            });
          });
        }
      } else {
        // No focus — arrange all in a circle around center
        const allCyNodes = cy.nodes();
        const count = allCyNodes.length;
        if (count > 0) {
          const radius = 150;
          allCyNodes.forEach((node, i) => {
            const angle = (2 * Math.PI * i) / count - Math.PI / 2;
            node.position({
              x: cx + radius * Math.cos(angle),
              y: ch + radius * Math.sin(angle),
            });
          });
        }
      }

      hasInitialLayoutRef.current = true;
      if (layoutRef.current) layoutRef.current.stop();
      let settled = false;
      const opts = {
        ...standardLayoutOptions(),
        tick: (progress: number) => {
          if (settled || progress < 0.8) return;
          settled = true;
          setPulseOverlays(computePulseOverlays(cy));
        },
      };
      const layout = cy.layout(opts as Parameters<typeof cy.layout>[0]);
      layoutRef.current = layout;
      layout.run();
      fitToFocus(cy, focusIdRef.current);
    } else if (topologyChanged) {
      // Partial topology change — positions already set (persisting + near-neighbor)
      if (layoutRef.current) layoutRef.current.stop();
      let settled = false;
      const opts = {
        ...standardLayoutOptions(),
        tick: (progress: number) => {
          if (settled || progress < 0.8) return;
          settled = true;
          setPulseOverlays(computePulseOverlays(cy));
        },
      };
      const layout = cy.layout(opts as Parameters<typeof cy.layout>[0]);
      layoutRef.current = layout;
      layout.run();
      fitToFocus(cy, focusIdRef.current);
    } else {
      // Topology unchanged (timelapse cost update) — no layout restart,
      // just re-center on the focused node.
      fitToFocus(cy, focusIdRef.current);
      setPulseOverlays(computePulseOverlays(cy));
    }
  }, [nodes, edges, isDark]); // eslint-disable-line react-hooks/exhaustive-deps

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
