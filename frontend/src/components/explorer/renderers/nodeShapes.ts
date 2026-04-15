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
  tenant: "round-rectangle",
  environment: "hexagon",
  kafka_cluster: "ellipse",
  dedicated_cluster: "ellipse",
  kafka_topic: "ellipse",
  service_account: "diamond",
  api_key: "diamond", // pragma: allowlist secret
  identity: "diamond",
  connector: "rectangle",
  flink_compute_pool: "triangle",
  schema_registry: "pentagon",
  ksqldb_cluster: "round-rectangle",
  // Synthetic group nodes (TASK-243)
  topic_group: "round-rectangle",
  identity_group: "round-rectangle",
  zero_cost_summary: "round-rectangle",
  capped_summary: "round-rectangle",
  // Synthetic group nodes (TASK-245)
  resource_group: "round-rectangle",
  cluster_group: "round-rectangle",
};

const GROUP_TYPES = new Set([
  "topic_group",
  "identity_group",
  "zero_cost_summary",
  "capped_summary",
  "resource_group",
  "cluster_group",
]);

const GROUP_NODE_SIZE = 100;

export function getNodeShape(resourceType: string): string {
  return SHAPE_MAP[resourceType] ?? "ellipse";
}

export function getNodeSize(
  resourceType: string,
  cost: number,
  minCost: number,
  maxCost: number,
): number {
  if (GROUP_TYPES.has(resourceType)) return GROUP_NODE_SIZE;
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
