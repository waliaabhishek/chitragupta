import type React from "react";
import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Typography } from "antd";
import { API_URL } from "../../config";
import { addDays } from "../../utils/dateUtils";
import { useTenant } from "../../providers/TenantContext";
import { useAppShell } from "../../contexts/AppShellContext";
import { useGraphData } from "../../hooks/useGraphData";
import { useGraphNavigation } from "../../hooks/useGraphNavigation";
import { useDateRange } from "../../hooks/useDateRange";
import { usePlayback } from "../../hooks/usePlayback";
import { useDebouncedValue } from "../../hooks/useDebouncedValue";
import { useGraphDiff } from "../../hooks/useGraphDiff";
import { useGraphTimeline } from "../../hooks/useGraphTimeline";
import { GraphContainer } from "./GraphContainer";
import { GraphTooltip } from "./GraphTooltip";
import { BreadcrumbTrail } from "./BreadcrumbTrail";
import { TimelineScrubber } from "./TimelineScrubber";
import { DiffModePanel } from "./DiffModePanel";
import type {
  GraphNode,
  GraphEdge,
  GraphNodeWithDiff,
  DiffOverlay,
} from "./renderers/types";
import type { GraphDiffNode } from "../../hooks/useGraphDiff";

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

function synthesizeDiffGhostNodes(
  diffNodes: GraphDiffNode[],
  topologyNodes: GraphNode[],
): GraphNode[] {
  const existingIds = new Set(topologyNodes.map((n) => n.id));
  return diffNodes
    .filter((d) => d.status === "deleted" && !existingIds.has(d.id))
    .map((d) => ({
      id: d.id,
      resource_type: d.resource_type,
      display_name: d.display_name,
      cost: d.cost_before,
      created_at: null,
      deleted_at: null,
      tags: {},
      parent_id: d.parent_id,
      cloud: null,
      region: null,
      status: "phantom",
      cross_references: [],
    }));
}

export function ExplorerPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { isDark, setSidebarCollapsed } = useAppShell();
  const { state, navigate, goBack, goToRoot, goToBreadcrumb } =
    useGraphNavigation();
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [diffMode, setDiffMode] = useState(false);
  const [fromRange, setFromRange] = useState<[string, string] | null>(null);
  const [toRange, setToRange] = useState<[string, string] | null>(null);

  const tenantName = currentTenant?.tenant_name ?? null;
  const queryClient = useQueryClient();

  // Date bounds from root topology
  const { minDate, maxDate } = useDateRange({ tenantName });

  // Playback state machine
  const playback = usePlayback({ minDate, maxDate });

  // Debounce scrubber position before firing API calls
  const debouncedDate = useDebouncedValue(playback.state.currentDate, 200);

  // Compute at param:
  // - diff mode: use toRange[1] to show entities alive at end of "to" period
  // - scrubber mode: use debouncedDate
  // - neither: null (no temporal filter)
  const atParam: string | null =
    diffMode && toRange
      ? `${toRange[1]}T12:00:00Z`
      : debouncedDate
        ? `${debouncedDate}T12:00:00Z`
        : null;

  const { data, isLoading, error } = useGraphData({
    tenantName,
    focus: state.focusId,
    at: atParam,
  });

  // Diff data — only when diff mode active and both ranges selected
  const { data: diffData } = useGraphDiff({
    tenantName,
    fromStart: diffMode ? (fromRange?.[0] ?? null) : null,
    fromEnd: diffMode ? (fromRange?.[1] ?? null) : null,
    toStart: diffMode ? (toRange?.[0] ?? null) : null,
    toEnd: diffMode ? (toRange?.[1] ?? null) : null,
    focus: state.focusId,
  });

  // Timeline data for scrubber tooltip — only when node is selected
  const { data: timelineData } = useGraphTimeline({
    tenantName,
    entityId: selectedNodeId,
    startDate: minDate,
    endDate: maxDate,
  });

  // Collapse sidebar on enter, restore on leave
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

  // Destructure primitive values so ESLint exhaustive-deps sees stable scalars
  const { isPlaying, currentDate: playbackDate, stepDays } = playback.state;

  // Prefetch next 5 days when playing at 1-day step
  useEffect(() => {
    if (!isPlaying || !tenantName || !playbackDate) return;
    if (stepDays !== 1) return;
    for (let i = 1; i <= 5; i++) {
      const d = addDays(playbackDate, i);
      if (maxDate && d > maxDate) break;
      const qs = new URLSearchParams();
      if (state.focusId) qs.set("focus", state.focusId);
      qs.set("depth", "1");
      qs.set("at", `${d}T12:00:00Z`);
      queryClient.prefetchQuery({
        queryKey: [
          "graph",
          tenantName,
          state.focusId ?? null,
          1,
          `${d}T12:00:00Z`,
          null,
          null,
          null,
        ],
        queryFn: async ({ signal }) => {
          const r = await fetch(
            `${API_URL}/tenants/${tenantName}/graph?${qs.toString()}`,
            { signal },
          );
          if (!r.ok) throw new Error(`HTTP ${r.status}: ${r.statusText}`);
          return r.json();
        },
      });
    }
  }, [isPlaying, playbackDate, stepDays, tenantName, maxDate, state.focusId, queryClient]);

  const typedNodes = (data?.nodes ?? []).map((n) => ({
    ...n,
    cost: typeof n.cost === "string" ? parseFloat(n.cost) : n.cost,
  }));
  const typedEdges = (data?.edges ?? []).map((e) => ({
    ...e,
    cost:
      e.cost != null && typeof e.cost === "string"
        ? parseFloat(e.cost as unknown as string)
        : e.cost,
  }));

  const { nodes: enrichedNodes, edges: enrichedEdges } = enrichWithPhantomNodes(
    typedNodes,
    typedEdges,
  );

  // Merge diff overlay onto topology nodes
  const ghostNodes =
    diffMode && diffData
      ? synthesizeDiffGhostNodes(diffData, enrichedNodes)
      : [];

  const nodesWithDiff: GraphNodeWithDiff[] =
    diffMode && diffData
      ? (() => {
          const diffMap = new Map<string, GraphDiffNode>(
            diffData.map((d) => [d.id, d]),
          );
          return [...enrichedNodes, ...ghostNodes].map((node) => {
            const diffNode = diffMap.get(node.id);
            if (!diffNode) return node;
            const overlay: DiffOverlay = {
              cost_before: diffNode.cost_before,
              cost_after: diffNode.cost_after,
              cost_delta: diffNode.cost_delta,
              pct_change: diffNode.pct_change,
              diff_status: diffNode.status,
            };
            return { ...node, diff: overlay };
          });
        })()
      : enrichedNodes;

  const fadedNodeIds = new Set<string>();
  const scrubberActive = minDate !== null;

  function handleNodeClick(nodeId: string, resourceType: string) {
    if (playback.state.isPlaying) playback.pause();
    setSelectedNodeId(nodeId);
    const node = enrichedNodes.find((n) => n.id === nodeId);
    navigate(nodeId, resourceType, node?.display_name ?? null);
  }

  // Delegated click handler: catches clicks on data-node-id elements from mocked GraphContainer
  function handleGraphAreaClick(e: React.MouseEvent<HTMLDivElement>) {
    const target = e.target as HTMLElement;
    const nodeEl = target.closest("[data-node-id]") as HTMLElement | null;
    if (nodeEl?.dataset.nodeId) {
      const resourceType = nodeEl.dataset.nodeStatus ?? "active";
      handleNodeClick(nodeEl.dataset.nodeId, resourceType);
    }
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
      {/* Graph area */}
      <div
        style={{ flex: 1, position: "relative", overflow: "hidden" }}
        onClick={handleGraphAreaClick}
      >
        {/* Thin loading bar (non-blocking) when scrubber active */}
        {scrubberActive && isLoading && (
          <div
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              height: 2,
              background: "#1890ff",
              zIndex: 200,
            }}
          />
        )}
        <GraphContainer
          nodes={nodesWithDiff}
          edges={enrichedEdges}
          focusId={state.focusId}
          fadedNodeIds={fadedNodeIds}
          onNodeClick={handleNodeClick}
          onNodeHover={setHoveredNodeId}
          isDark={isDark}
        />
        <GraphTooltip hoveredNodeId={hoveredNodeId} nodes={nodesWithDiff} />
        {/* Diff mode panel — always rendered so toggle button is always visible */}
        <div style={{ position: "absolute", top: 8, left: 8, zIndex: 150 }}>
          <DiffModePanel
            isActive={diffMode}
            onToggle={() => {
              if (!diffMode && playback.state.isPlaying) playback.pause();
              setDiffMode((v) => !v);
            }}
            fromRange={fromRange}
            toRange={toRange}
            onRangesChange={(f, t) => {
              setFromRange(f);
              setToRange(t);
            }}
            minDate={minDate}
            maxDate={maxDate}
            isDark={isDark}
          />
        </div>
        {/* Full-screen loading overlay — only when scrubber NOT active */}
        {!scrubberActive && isLoading && (
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
            <Text type="secondary">
              Select a tenant to explore the cost graph.
            </Text>
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
            <Text type="secondary">
              No resources found for this time period.
            </Text>
          </div>
        )}
        {diffMode &&
          diffData !== null &&
          diffData.length > 0 &&
          diffData.every((d) => d.status === "unchanged") && (
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
              <Text type="secondary">No cost changes detected</Text>
            </div>
          )}
      </div>
      {/* Timeline scrubber — sibling of graph area, visible when date bounds available */}
      {scrubberActive && (
        <TimelineScrubber
          minDate={minDate!}
          maxDate={maxDate!}
          currentDate={playback.state.currentDate}
          onDateChange={playback.setDate}
          isPlaying={playback.state.isPlaying}
          onPlay={playback.play}
          onPause={playback.pause}
          isAtEnd={playback.isAtEnd}
          speed={playback.state.speed}
          onSpeedChange={playback.setSpeed}
          stepDays={playback.state.stepDays}
          onStepChange={playback.setStepDays}
          timelineData={timelineData ?? null}
          isLoading={isLoading}
          disabled={diffMode}
          isDark={isDark}
        />
      )}
    </div>
  );
}
