import { useState } from "react";

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

const INITIAL_STATE: NavigationState = {
  focusId: null,
  focusType: null,
  breadcrumbs: [],
};

export function useGraphNavigation(): UseGraphNavigationResult {
  const [state, setState] = useState<NavigationState>(INITIAL_STATE);

  function navigate(
    nodeId: string,
    resourceType: string,
    displayName: string | null,
  ) {
    setState((prev) => ({
      focusId: nodeId,
      focusType: resourceType,
      breadcrumbs: [
        ...prev.breadcrumbs,
        { id: nodeId, label: displayName ?? nodeId, type: resourceType },
      ],
    }));
  }

  function goBack() {
    if (state.breadcrumbs.length === 0) return;
    const newBreadcrumbs = state.breadcrumbs.slice(0, -1);
    const last = newBreadcrumbs[newBreadcrumbs.length - 1] ?? null;
    setState({
      focusId: last?.id ?? null,
      focusType: last?.type ?? null,
      breadcrumbs: newBreadcrumbs,
    });
  }

  function goToRoot() {
    setState(INITIAL_STATE);
  }

  function goToBreadcrumb(index: number) {
    if (index < 0) return;
    if (index >= state.breadcrumbs.length - 1) return;
    const newBreadcrumbs = state.breadcrumbs.slice(0, index + 1);
    const target = newBreadcrumbs[index];
    setState({
      focusId: target.id,
      focusType: target.type,
      breadcrumbs: newBreadcrumbs,
    });
  }

  return { state, navigate, goBack, goToRoot, goToBreadcrumb };
}
