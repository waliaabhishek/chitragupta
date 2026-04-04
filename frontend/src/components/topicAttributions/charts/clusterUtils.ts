import type { TopicAttributionAggregationBucket } from "../../../types/api";
import { formatCurrency } from "../../../utils/aggregation";

export interface ClusterAggregation {
  clusterTopic: Record<string, Record<string, number>>;
  clusterTotals: Record<string, number>;
}

export function aggregateByCluster(
  buckets: TopicAttributionAggregationBucket[],
): ClusterAggregation {
  const clusterTopic: Record<string, Record<string, number>> = {};
  const clusterTotals: Record<string, number> = {};
  for (const b of buckets) {
    const c = b.dimensions.cluster_resource_id ?? "Unknown";
    const t = b.dimensions.topic_name ?? "Unknown";
    const amt = parseFloat(b.total_amount);
    clusterTopic[c] ??= {};
    clusterTopic[c][t] = (clusterTopic[c][t] ?? 0) + amt;
    clusterTotals[c] = (clusterTotals[c] ?? 0) + amt;
  }
  return { clusterTopic, clusterTotals };
}

export interface ClusterRisk {
  clusterId: string;
  topTopic: string;
  topTopicCost: number;
  totalCost: number;
  ratio: number; // 0.0 – 1.0
}

export function buildConcentrationRiskData(
  buckets: TopicAttributionAggregationBucket[],
  topNClusters: number = 15,
): ClusterRisk[] {
  const { clusterTopic, clusterTotals } = aggregateByCluster(buckets);

  return Object.entries(clusterTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topNClusters)
    .map(([clusterId, totalCost]) => {
      const topics = clusterTopic[clusterId] ?? {};
      const [topTopic, topTopicCost] = Object.entries(topics).sort(
        (a, b) => b[1] - a[1],
      )[0] ?? ["Unknown", 0];
      const ratio = totalCost > 0 ? topTopicCost / totalCost : 0;
      return { clusterId, topTopic, topTopicCost, totalCost, ratio };
    })
    .sort((a, b) => b.ratio - a.ratio); // highest risk at top
}

export function riskColor(ratio: number): string {
  if (ratio >= 0.75) return "#f5222d"; // red
  if (ratio >= 0.5) return "#faad14"; // yellow
  return "#52c41a"; // green
}

export function formatRiskTooltip(
  risks: ClusterRisk[],
  dataIndex: number,
): string {
  const r = risks[dataIndex];
  return [
    `<b>${r.clusterId}</b>`,
    `Top topic: ${r.topTopic}`,
    `Top topic cost: ${formatCurrency(r.topTopicCost)}`,
    `Total cluster cost: ${formatCurrency(r.totalCost)}`,
    `Concentration: ${(r.ratio * 100).toFixed(1)}%`,
  ].join("<br/>");
}

export function buildTopClustersCostData(
  buckets: TopicAttributionAggregationBucket[],
  topNClusters: number = 10,
  topNTopics: number = 5,
): {
  clusters: string[];
  series: { name: string; type: "bar"; stack: string; data: number[] }[];
} {
  const { clusterTopic, clusterTotals } = aggregateByCluster(buckets);

  // globalTopicTotals needed only for topic ranking — compute here
  const globalTopicTotals: Record<string, number> = {};
  for (const topics of Object.values(clusterTopic)) {
    for (const [t, amt] of Object.entries(topics)) {
      globalTopicTotals[t] = (globalTopicTotals[t] ?? 0) + amt;
    }
  }

  // Top N clusters by total cost; ascending so highest renders at top of horizontal chart
  const clusters = Object.entries(clusterTotals)
    .sort((a, b) => a[1] - b[1])
    .slice(-topNClusters)
    .map(([c]) => c);

  const topTopics = Object.entries(globalTopicTotals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, topNTopics)
    .map(([t]) => t);

  return {
    clusters,
    series: [
      ...topTopics.map((topic) => ({
        name: topic,
        type: "bar" as const,
        stack: "total",
        data: clusters.map((c) => clusterTopic[c]?.[topic] ?? 0),
      })),
      {
        name: "Other",
        type: "bar" as const,
        stack: "total",
        data: clusters.map((c) => {
          const topSum = topTopics.reduce(
            (s, t) => s + (clusterTopic[c]?.[t] ?? 0),
            0,
          );
          return (clusterTotals[c] ?? 0) - topSum;
        }),
      },
    ],
  };
}
