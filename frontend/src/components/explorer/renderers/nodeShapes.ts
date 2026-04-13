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
};

export function getNodeShape(resourceType: string): string {
  return SHAPE_MAP[resourceType] ?? "ellipse";
}
