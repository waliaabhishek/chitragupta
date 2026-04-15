import { useEffect, useRef, useState } from "react";
import type { GraphNode } from "../components/explorer/renderers/types";

interface NavigationState {
  focusId: string | null;
  focusType: string | null;
  breadcrumbs: Array<{ id: string; label: string; type: string }>;
}

interface UseGraphNavigationResult {
  state: NavigationState;
  navigate: (
    nodeId: string,
    resourceType: string,
    displayName: string | null,
  ) => void;
  goBack: () => void;
  goToRoot: () => void;
  goToBreadcrumb: (index: number) => void;
}

interface UseGraphNavigationOptions {
  focusFromUrl: string | null;
  setFocus: (id: string | null) => void;
  currentNodes: GraphNode[] | null;
}

const INITIAL_STATE: NavigationState = {
  focusId: null,
  focusType: null,
  breadcrumbs: [],
};

export function useGraphNavigation(
  options?: UseGraphNavigationOptions,
): UseGraphNavigationResult {
  const urlDriven = options !== undefined;
  const [internalState, setInternalState] = useState<NavigationState>(INITIAL_STATE);

  // In URL-driven mode, focusId comes from props; otherwise from internal state.
  const state: NavigationState = urlDriven
    ? { ...internalState, focusId: options.focusFromUrl }
    : internalState;

  // Breadcrumb reconstruction for URL-driven mode.
  // Fires once when focusFromUrl is set, breadcrumbs empty, and nodes are available.
  const breadcrumbsRef = useRef(internalState.breadcrumbs);
  useEffect(() => {
    breadcrumbsRef.current = internalState.breadcrumbs;
  });

  const focusFromUrl = options?.focusFromUrl ?? null;
  const currentNodes = options?.currentNodes ?? null;

  useEffect(() => {
    if (!urlDriven || !focusFromUrl) return;
    if (breadcrumbsRef.current.length > 0) return;
    if (!currentNodes || currentNodes.length === 0) return;

    const nodeMap = new Map(currentNodes.map((n) => [n.id, n]));
    const chain: Array<{ id: string; label: string; type: string }> = [];
    const visited = new Set<string>();

    let current: GraphNode | undefined = nodeMap.get(focusFromUrl);
    while (current) {
      if (visited.has(current.id)) break;
      visited.add(current.id);
      chain.unshift({
        id: current.id,
        label: current.display_name ?? current.id,
        type: current.resource_type,
      });
      if (!current.parent_id) break;
      current = nodeMap.get(current.parent_id);
    }

    if (chain.length > 0) {
      setInternalState((prev) => ({ ...prev, breadcrumbs: chain }));
    }
  }, [urlDriven, focusFromUrl, currentNodes]);

  function navigate(nodeId: string, resourceType: string, displayName: string | null) {
    if (urlDriven) {
      setInternalState((prev) => ({
        ...prev,
        focusType: resourceType,
        breadcrumbs: [
          ...prev.breadcrumbs,
          { id: nodeId, label: displayName ?? nodeId, type: resourceType },
        ],
      }));
      options!.setFocus(nodeId);
    } else {
      setInternalState((prev) => ({
        focusId: nodeId,
        focusType: resourceType,
        breadcrumbs: [
          ...prev.breadcrumbs,
          { id: nodeId, label: displayName ?? nodeId, type: resourceType },
        ],
      }));
    }
  }

  function goBack() {
    if (internalState.breadcrumbs.length === 0) return;
    const newBreadcrumbs = internalState.breadcrumbs.slice(0, -1);
    const last = newBreadcrumbs[newBreadcrumbs.length - 1] ?? null;

    if (urlDriven) {
      setInternalState((prev) => ({
        ...prev,
        focusType: last?.type ?? null,
        breadcrumbs: newBreadcrumbs,
      }));
      options!.setFocus(last?.id ?? null);
    } else {
      setInternalState({
        focusId: last?.id ?? null,
        focusType: last?.type ?? null,
        breadcrumbs: newBreadcrumbs,
      });
    }
  }

  function goToRoot() {
    if (urlDriven) {
      setInternalState(INITIAL_STATE);
      options!.setFocus(null);
    } else {
      setInternalState(INITIAL_STATE);
    }
  }

  function goToBreadcrumb(index: number) {
    if (index < 0) return;
    if (index >= internalState.breadcrumbs.length - 1) return;
    const newBreadcrumbs = internalState.breadcrumbs.slice(0, index + 1);
    const target = newBreadcrumbs[index];

    if (urlDriven) {
      setInternalState((prev) => ({
        ...prev,
        focusType: target.type,
        breadcrumbs: newBreadcrumbs,
      }));
      options!.setFocus(target.id);
    } else {
      setInternalState({
        focusId: target.id,
        focusType: target.type,
        breadcrumbs: newBreadcrumbs,
      });
    }
  }

  return { state, navigate, goBack, goToRoot, goToBreadcrumb };
}
