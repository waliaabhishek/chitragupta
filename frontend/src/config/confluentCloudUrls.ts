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

export function schemaRegistryUrl(envId: string, srId: string): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/schema-registry/${srId}`;
}

export function serviceAccountUrl(): string {
  return `${CONFLUENT_CLOUD_BASE}/settings/org/service-accounts`;
}

export function connectorUrl(
  envId: string,
  clusterId: string,
  connectorId: string,
): string {
  return `${CONFLUENT_CLOUD_BASE}/environments/${envId}/clusters/${clusterId}/connectors/${connectorId}`;
}
