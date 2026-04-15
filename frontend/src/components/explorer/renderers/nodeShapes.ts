export function costToSize(
  cost: number,
  minCost: number,
  maxCost: number,
): number {
  if (maxCost <= minCost) return 40;
  const MIN_SIZE = 20;
  const MAX_SIZE = 80;
  if (cost <= 0) return MIN_SIZE;
  const logMin = Math.log1p(minCost);
  const logMax = Math.log1p(maxCost);
  const logCost = Math.log1p(cost);
  const t = (logCost - logMin) / (logMax - logMin);
  return MIN_SIZE + t * (MAX_SIZE - MIN_SIZE);
}

const SHAPE_MAP: Record<string, string> = {
  tenant: "ellipse",
  environment: "ellipse",
  kafka_cluster: "ellipse",
  dedicated_cluster: "ellipse",
  kafka_topic: "ellipse",
  service_account: "ellipse",
  api_key: "ellipse", // pragma: allowlist secret
  identity: "ellipse",
  connector: "ellipse",
  flink_compute_pool: "ellipse",
  schema_registry: "ellipse",
  ksqldb_cluster: "ellipse",
  // Synthetic group nodes (TASK-243)
  topic_group: "round-rectangle",
  identity_group: "round-rectangle",
  zero_cost_summary: "round-rectangle",
  capped_summary: "round-rectangle",
  // Synthetic group nodes (TASK-245)
  resource_group: "round-rectangle",
  cluster_group: "round-rectangle",
  // Synthetic cross-reference overflow nodes (TASK-246)
  xref_group: "round-rectangle",
};

const GROUP_TYPES = new Set([
  "topic_group",
  "identity_group",
  "zero_cost_summary",
  "capped_summary",
  "resource_group",
  "cluster_group",
  "xref_group",
]);

export function getNodeShape(resourceType: string): string {
  return SHAPE_MAP[resourceType] ?? "ellipse";
}

export function getNodeSize(
  _resourceType: string,
  cost: number,
  minCost: number,
  maxCost: number,
): number {
  return costToSize(cost, minCost, maxCost);
}

export function isGroupNode(resourceType: string): boolean {
  return GROUP_TYPES.has(resourceType);
}

export function isExpandableGroup(resourceType: string): boolean {
  return (
    resourceType === "topic_group" ||
    resourceType === "identity_group" ||
    resourceType === "resource_group" ||
    resourceType === "cluster_group"
  );
}
