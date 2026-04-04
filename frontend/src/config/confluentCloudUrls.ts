const CONFLUENT_CLOUD_BASE = "https://confluent.cloud";

export function environmentUrl(envId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}`;
}

export function clusterUrl(envId: string, clusterId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/clusters/${clusterId}`;
}

export function topicUrl(
  envId: string,
  clusterId: string,
  topicName: string,
): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/clusters/${clusterId}/topics/${topicName}`;
}

export function schemaRegistryUrl(envId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/stream-governance/schema-registry/overview`;
}

export function serviceAccountUrl(saId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/principals/${saId}?view=identity`;
}

export function userUrl(userId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/principals/${userId}?view=identity`;
}

export function identityProviderUrl(opId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/org/workload_identities/provider/oidc/view/${opId}`;
}

// both mTLS and OIDC pools share this URL for now; mTLS distinction deferred
export function identityPoolUrl(poolId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/principals/${poolId}?view=identity`;
}

export function apiKeyUrl(keyId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/api-keys/edit/${keyId}`;
}

export function flinkComputePoolUrl(envId: string, poolId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/flink/pools/${poolId}/overview`;
}

export function ksqldbClusterUrl(
  envId: string,
  kafkaClusterId: string,
  ksqlcId: string,
): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/clusters/${kafkaClusterId}/ksql/${ksqlcId}/editor`;
}

export function connectorUrl(
  envId: string,
  clusterId: string,
  connectorId: string,
): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/clusters/${clusterId}/connectors/${connectorId}`;
}
