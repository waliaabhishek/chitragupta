import type React from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Button, Typography } from "antd";
import { API_URL } from "../../config";
import { addDays } from "../../utils/dateUtils";
import { useTenant } from "../../providers/TenantContext";
import { useAppShell } from "../../contexts/AppShellContext";
import { useGraphData } from "../../hooks/useGraphData";
import { useGraphNavigation } from "../../hooks/useGraphNavigation";
import { useExplorerParams } from "../../hooks/useExplorerParams";
import { useDateRange } from "../../hooks/useDateRange";
import { usePlayback } from "../../hooks/usePlayback";
import { useDebouncedValue } from "../../hooks/useDebouncedValue";
import { useGraphDiff } from "../../hooks/useGraphDiff";
import { useGraphTimeline } from "../../hooks/useGraphTimeline";
import { useTagOverlay } from "../../hooks/useTagOverlay";
import { GraphContainer } from "./GraphContainer";
import { GraphTooltip } from "./GraphTooltip";
import { BreadcrumbTrail } from "./BreadcrumbTrail";
import { TimelineScrubber } from "./TimelineScrubber";
import { DiffModePanel } from "./DiffModePanel";
import { SearchBar } from "./SearchBar";
import { TagOverlayPanel } from "./TagOverlayPanel";
import { CopyLinkButton } from "./CopyLinkButton";
import { isExpandableGroup, isGroupNode } from "./renderers/nodeShapes";

const NEAR_ZERO_THRESHOLD = 0.005;

/**
 * Fold nodes that display as "$0.00" into the zero_cost_summary node.
 * Runs on the typed API response before phantom enrichment.
 */
function collapseNearZeroNodes(
  nodes: GraphNode[],
  edges: GraphEdge[],
  focusId: string | null,
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nearZeroIds = new Set<string>();
  for (const n of nodes) {
    if (n.id === focusId) continue;
    if (isGroupNode(n.resource_type)) continue;
    if (n.status === "phantom") continue;
    if (n.cost < NEAR_ZERO_THRESHOLD) nearZeroIds.add(n.id);
  }
  if (nearZeroIds.size === 0) return { nodes, edges };

  // Find existing zero_cost_summary node
  let summary = nodes.find((n) => n.resource_type === "zero_cost_summary") ?? null;
  const existingCount = summary?.child_count ?? 0;
  const newCount = existingCount + nearZeroIds.size;

  if (summary) {
    // Update existing summary
    summary = {
      ...summary,
      child_count: newCount,
      display_name: `${newCount} others at $0`,
    };
  } else {
    // Determine edge target from one of the near-zero nodes' edges
    const sampleEdge = edges.find(
      (e) => nearZeroIds.has(e.source) || nearZeroIds.has(e.target),
    );
    const target = sampleEdge
      ? nearZeroIds.has(sampleEdge.source)
        ? sampleEdge.target
        : sampleEdge.source
      : focusId;
    const summaryId = `${target}:zero_cost_ui`;
    summary = {
      id: summaryId,
      resource_type: "zero_cost_summary",
      display_name: `${newCount} others at $0`,
      cost: 0,
      created_at: null,
      deleted_at: null,
      tags: {},
      parent_id: null,
      cloud: null,
      region: null,
      status: "active",
      cross_references: [],
      child_count: newCount,
      child_total_cost: 0,
    };
    edges = [
      ...edges,
      {
        source: summaryId,
        target: target!,
        relationship_type: "charge",
        cost: null,
      },
    ];
  }

  const outNodes = nodes
    .filter((n) => !nearZeroIds.has(n.id) && n.resource_type !== "zero_cost_summary")
    .concat(summary);
  const outEdges = edges.filter(
    (e) => !nearZeroIds.has(e.source) && !nearZeroIds.has(e.target),
  );

  return { nodes: outNodes, edges: outEdges };
}
import type {
  GraphNode,
  GraphEdge,
  GraphNodeWithDiff,
  DiffOverlay,
} from "./renderers/types";
import type { GraphDiffNode } from "../../hooks/useGraphDiff";

const { Text } = Typography;

// UI binding: maps expandable group types to the API expand= value
const GROUP_EXPAND_MAP: Record<string, string> = {
  topic_group: "topics",
  identity_group: "identities",
  resource_group: "resources",
  cluster_group: "clusters",
};

function enrichWithPhantomNodes(
  nodes: GraphNode[],
  edges: GraphEdge[],
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const existingIds = new Set(nodes.map((n) => n.id));
  const phantomNodes: GraphNode[] = [];
  const phantomEdges: GraphEdge[] = [];

  for (const node of nodes) {
    for (const group of node.cross_references) {
      // Individual phantom nodes from top N items
      for (const item of group.items) {
        if (existingIds.has(item.id)) continue;
        existingIds.add(item.id);
        phantomNodes.push({
          id: item.id,
          resource_type: item.resource_type,
          display_name: item.display_name,
          cost: item.cost,
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
          source: item.id,
          target: node.id,
          relationship_type: "charge",
          cost: null,
        });
      }

      // Group summary node if there are more items than shown
      const remaining = group.total_count - group.items.length;
      if (remaining > 0) {
        const groupId = `${node.id}:xref_group:${group.resource_type}`;
        if (!existingIds.has(groupId)) {
          existingIds.add(groupId);
          phantomNodes.push({
            id: groupId,
            resource_type: "xref_group",
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
            child_count: remaining,
          });
          phantomEdges.push({
            source: groupId,
            target: node.id,
            relationship_type: "charge",
            cost: null,
          });
        }
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

function enrichWithTagColor(
  nodes: GraphNodeWithDiff[],
  activeKey: string | null,
  colorMap: Record<string, string>,
): GraphNodeWithDiff[] {
  if (!activeKey) return nodes;
  return nodes.map((n) => ({
    ...n,
    tagColor:
      n.status === "phantom"
        ? undefined
        : (colorMap[n.tags[activeKey] ?? "UNTAGGED"] ?? "#d9d9d9"),
  }));
}

export function ExplorerPage(): React.JSX.Element {
  const { currentTenant } = useTenant();
  const { isDark } = useAppShell();
  const { params, pushParam, pushParams, replaceParam } = useExplorerParams();
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const fromRange: [string, string] | null =
    params.from_start && params.from_end
      ? [params.from_start, params.from_end]
      : null;
  const toRange: [string, string] | null =
    params.to_start && params.to_end
      ? [params.to_start, params.to_end]
      : null;

  const tenantName = currentTenant?.tenant_name ?? null;
  const tenantId = currentTenant?.tenant_id ?? null;
  const queryClient = useQueryClient();

  // The tenant root node is synthetic (no DB row) — the API only serves it
  // via the root view (focus=null).  Strip tenant-self-focus so that
  // URL-driven loads like ?focus=ccloud-prod don't 404.
  const effectiveFocus = params.focus === tenantId ? null : params.focus;

  const setFocus = useCallback((id: string | null) => pushParam("focus", id), [pushParam]);

  // Date bounds from root topology
  const { minDate, maxDate } = useDateRange({ tenantName });

  // Playback state machine
  const playback = usePlayback({ minDate, maxDate, initialDate: params.at ?? undefined });

  // Sync scrubber position to URL on pause (replace — no history entry).
  // Skip initial mount so a pre-populated currentDate does not clobber the URL.
  const isFirstScrubberEffect = useRef(true);
  useEffect(() => {
    if (isFirstScrubberEffect.current) {
      isFirstScrubberEffect.current = false;
      return;
    }
    if (!playback.state.isPlaying && playback.state.currentDate) {
      replaceParam("at", playback.state.currentDate);
    }
  }, [playback.state.isPlaying, playback.state.currentDate, replaceParam]);

  // Debounce scrubber position before firing API calls
  const debouncedDate = useDebouncedValue(playback.state.currentDate, 200);

  // Compute at param:
  // - diff mode: use toRange[1] to show entities alive at end of "to" period
  // - scrubber mode: use debouncedDate
  // - neither: null (no temporal filter)
  const atParam: string | null =
    params.diff && toRange
      ? `${toRange[1]}T12:00:00Z`
      : debouncedDate
        ? `${debouncedDate}T12:00:00Z`
        : null;

  const { data, isLoading, error } = useGraphData({
    tenantName,
    focus: effectiveFocus,
    at: atParam,
    startDate: debouncedDate ?? undefined,
    endDate: debouncedDate ?? undefined,
    expand: params.expand,
  });

  // Diff data — only when diff mode active and both ranges selected
  const { data: diffData } = useGraphDiff({
    tenantName,
    fromStart: params.diff ? (fromRange?.[0] ?? null) : null,
    fromEnd: params.diff ? (fromRange?.[1] ?? null) : null,
    toStart: params.diff ? (toRange?.[0] ?? null) : null,
    toEnd: params.diff ? (toRange?.[1] ?? null) : null,
    focus: effectiveFocus,
  });

  // Timeline data for scrubber tooltip — only when node is selected
  const { data: timelineData } = useGraphTimeline({
    tenantName,
    entityId: selectedNodeId,
    startDate: minDate,
    endDate: maxDate,
  });


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
      if (effectiveFocus) qs.set("focus", effectiveFocus);
      qs.set("depth", "1");
      qs.set("at", `${d}T12:00:00Z`);
      if (params.expand) qs.set("expand", params.expand);
      queryClient.prefetchQuery({
        queryKey: [
          "graph",
          tenantName,
          effectiveFocus ?? null,
          1,
          `${d}T12:00:00Z`,
          null,
          null,
          null,
          params.expand ?? null,
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
  }, [isPlaying, playbackDate, stepDays, tenantName, maxDate, effectiveFocus, params.expand, queryClient]);

  const typedNodes = useMemo(
    () =>
      (data?.nodes ?? []).map((n) => ({
        ...n,
        cost: typeof n.cost === "string" ? parseFloat(n.cost) : n.cost,
        child_total_cost:
          n.child_total_cost != null && typeof n.child_total_cost === "string"
            ? parseFloat(n.child_total_cost as unknown as string)
            : n.child_total_cost,
        cross_references: (n.cross_references ?? []).map((g) => ({
          ...g,
          items: g.items.map((item) => ({
            ...item,
            cost: typeof item.cost === "string" ? parseFloat(item.cost) : item.cost,
          })),
        })),
      })),
    [data?.nodes],
  );
  const typedEdges = useMemo(
    () =>
      (data?.edges ?? []).map((e) => ({
        ...e,
        cost:
          e.cost != null && typeof e.cost === "string"
            ? parseFloat(e.cost as unknown as string)
            : e.cost,
      })),
    [data?.edges],
  );

  const { nodes: collapsedNodes, edges: collapsedEdges } = useMemo(
    () => collapseNearZeroNodes(typedNodes, typedEdges, effectiveFocus),
    [typedNodes, typedEdges, effectiveFocus],
  );

  const { nodes: enrichedNodes, edges: enrichedEdges } = useMemo(
    () => enrichWithPhantomNodes(collapsedNodes, collapsedEdges),
    [collapsedNodes, collapsedEdges],
  );

  // URL-driven navigation — called after enrichedNodes so currentNodes can be passed
  const { state, navigate, goBack, goToRoot, goToBreadcrumb } = useGraphNavigation({
    focusFromUrl: effectiveFocus,
    setFocus,
    currentNodes: enrichedNodes.length > 0 ? enrichedNodes : null,
  });

  // Tag overlay — stable clear callback, then overlay hook
  const onClearTagValue = useCallback(() => pushParam("tag_value", null), [pushParam]);
  const tagOverlay = useTagOverlay({
    tenantName,
    nodes: enrichedNodes,
    activeKey: params.tag,
    onClearValue: onClearTagValue,
  });

  // Merge diff overlay onto topology nodes
  const ghostNodes = useMemo(
    () =>
      params.diff && diffData ? synthesizeDiffGhostNodes(diffData, enrichedNodes) : [],
    [params.diff, diffData, enrichedNodes],
  );

  const nodesWithDiff: GraphNodeWithDiff[] = useMemo(() => {
    if (params.diff && diffData) {
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
    }
    return enrichedNodes;
  }, [params.diff, diffData, enrichedNodes, ghostNodes]);

  const fadedNodeIds = useMemo(() => {
    if (!params.tag || !params.tag_value) return new Set<string>();
    const result = new Set<string>();
    for (const node of nodesWithDiff) {
      if (node.status === "phantom") continue;
      const tags = node.tags as Record<string, string>;
      const val = tags[params.tag] ?? "UNTAGGED";
      if (val !== params.tag_value) result.add(node.id);
    }
    return result;
  }, [nodesWithDiff, params.tag, params.tag_value]);

  const nodesForRenderer = useMemo(
    () => enrichWithTagColor(nodesWithDiff, params.tag, tagOverlay.colorMap),
    [nodesWithDiff, params.tag, tagOverlay.colorMap],
  );

  const scrubberActive = minDate !== null;

  function handleNodeClick(nodeId: string, resourceType: string) {
    if (playback.state.isPlaying) playback.pause();

    // Already focused on this node — don't duplicate breadcrumb
    if (nodeId === params.focus) return;

    // Non-interactive summary nodes: no-op
    if (isGroupNode(resourceType) && !isExpandableGroup(resourceType)) return;

    // Expandable group nodes: set expand param, don't navigate
    if (isExpandableGroup(resourceType)) {
      const expandValue = GROUP_EXPAND_MAP[resourceType];
      if (params.expand !== expandValue) pushParam("expand", expandValue);
      return;
    }

    // Tenant node: clicking the synthetic root is equivalent to "go to root"
    if (resourceType === "tenant") {
      goToRoot();
      if (params.expand) replaceParam("expand", null);
      return;
    }

    // Regular nodes: navigate to focus, clear expand
    setSelectedNodeId(nodeId);
    const node = enrichedNodes.find((n) => n.id === nodeId);
    navigate(nodeId, resourceType, node?.display_name ?? null);
    if (params.expand) replaceParam("expand", null);
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
        onNavigate={(crumb) => {
          goToBreadcrumb(crumb);
          if (params.expand) replaceParam("expand", null);
        }}
        onGoBack={() => {
          goBack();
          if (params.expand) replaceParam("expand", null);
        }}
        onGoToRoot={() => {
          goToRoot();
          if (params.expand) replaceParam("expand", null);
        }}
        copyLinkButton={<CopyLinkButton isDark={isDark} />}
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
          nodes={nodesForRenderer}
          edges={enrichedEdges}
          focusId={effectiveFocus}
          fadedNodeIds={fadedNodeIds}
          onNodeClick={handleNodeClick}
          onNodeHover={setHoveredNodeId}
          isDark={isDark}
          activeTagKey={params.tag}
          tagSelectedValue={params.tag_value}
        />
        {/* Diff mode panel — always rendered so toggle button is always visible */}
        <div style={{ position: "absolute", top: 8, left: 8, zIndex: 150 }}>
          <DiffModePanel
            isActive={params.diff}
            onToggle={() => {
              if (!params.diff && playback.state.isPlaying) playback.pause();
              if (params.diff) {
                pushParams({
                  diff: false,
                  from_start: null,
                  from_end: null,
                  to_start: null,
                  to_end: null,
                });
              } else {
                pushParam("diff", true);
              }
            }}
            fromRange={fromRange}
            toRange={toRange}
            onRangesChange={(f, t) => {
              pushParams({
                from_start: f?.[0] ?? null,
                from_end: f?.[1] ?? null,
                to_start: t?.[0] ?? null,
                to_end: t?.[1] ?? null,
              });
            }}
            minDate={minDate}
            maxDate={maxDate}
            isDark={isDark}
          />
        </div>
        {/* Collapse button — visible when expand is active */}
        {params.expand && (
          <div style={{ position: "absolute", top: 48, left: 8, zIndex: 150 }}>
            <Button
              size="small"
              onClick={() => pushParam("expand", null)}
            >
              Collapse {params.expand}
            </Button>
          </div>
        )}
        {/* Right-side controls: search, tag overlay, tooltip */}
        <div style={{ position: "absolute", top: 8, right: 8, zIndex: 150, display: "flex", flexDirection: "column", gap: 8, maxWidth: 240 }}>
          <SearchBar
            tenantName={tenantName}
            onSelect={(entityId, resourceType, displayName) => {
              if (playback.state.isPlaying) playback.pause();
              setSelectedNodeId(entityId);
              navigate(entityId, resourceType, displayName);
              if (params.expand) replaceParam("expand", null);
            }}
            isDark={isDark}
          />
          <TagOverlayPanel
            availableKeys={tagOverlay.availableKeys}
            isLoadingKeys={tagOverlay.isLoadingKeys}
            activeKey={params.tag}
            onKeyChange={(key) => {
              const k = key ?? null;
              if (k === null) {
                pushParams({ tag: null, tag_value: null });
              } else {
                pushParam("tag", k);
              }
            }}
            colorMap={tagOverlay.colorMap}
            selectedValue={params.tag_value}
            onValueClick={(value) => pushParam("tag_value", value ?? null)}
            isDark={isDark}
          />
          <GraphTooltip hoveredNodeId={hoveredNodeId} nodes={nodesWithDiff} />
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
        {params.diff &&
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
          disabled={params.diff}
          isDark={isDark}
        />
      )}
    </div>
  );
}
