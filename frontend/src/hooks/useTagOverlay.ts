import { useEffect, useMemo, useRef } from "react";
import type { GraphNode } from "../components/explorer/renderers/types";
import { useTagKeys } from "./useTagKeys";

const TAG_PALETTE = [
  "#1677ff",
  "#52c41a",
  "#faad14",
  "#ff4d4f",
  "#722ed1",
  "#13c2c2",
  "#eb2f96",
  "#fa8c16",
  "#a0d911",
  "#ff6e33",
];

const UNTAGGED_COLOR = "#d9d9d9";

interface UseTagOverlayParams {
  tenantName: string | null;
  nodes: GraphNode[];
  activeKey: string | null;
  onClearValue: () => void;
}

interface UseTagOverlayResult {
  availableKeys: string[];
  isLoadingKeys: boolean;
  colorMap: Record<string, string>;
}

export function useTagOverlay({
  tenantName,
  nodes,
  activeKey,
  onClearValue,
}: UseTagOverlayParams): UseTagOverlayResult {
  const { data: availableKeys, isLoading: isLoadingKeys } = useTagKeys(tenantName);

  const colorMap = useMemo<Record<string, string>>(() => {
    if (!activeKey) return {};

    const costByValue: Record<string, number> = {};
    let hasUntagged = false;

    for (const node of nodes) {
      const tags = node.tags as Record<string, string>;
      const value = tags[activeKey] ?? null;
      if (value === null || value === undefined) {
        hasUntagged = true;
      } else {
        costByValue[value] = (costByValue[value] ?? 0) + node.cost;
      }
    }

    const sorted = Object.entries(costByValue).sort(([, a], [, b]) => b - a);
    const result: Record<string, string> = {};

    sorted.forEach(([value], i) => {
      result[value] = i < TAG_PALETTE.length ? TAG_PALETTE[i] : "#8c8c8c";
    });

    if (hasUntagged) {
      result["UNTAGGED"] = UNTAGGED_COLOR;
    }

    return result;
  }, [nodes, activeKey]);

  // Call onClearValue when activeKey changes — but NOT on initial mount.
  const isFirstKeyEffect = useRef(true);
  useEffect(() => {
    if (isFirstKeyEffect.current) {
      isFirstKeyEffect.current = false;
      return;
    }
    onClearValue();
  }, [activeKey, onClearValue]);

  return { availableKeys, isLoadingKeys, colorMap };
}
