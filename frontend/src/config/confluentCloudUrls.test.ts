import { describe, expect, it } from "vitest";
import {
  environmentUrl,
  clusterUrl,
  topicUrl,
  serviceAccountUrl,
  schemaRegistryUrl,
  connectorUrl,
  userUrl,
  identityProviderUrl,
  identityPoolUrl,
  apiKeyUrl,
  flinkComputePoolUrl,
  ksqldbClusterUrl,
} from "./confluentCloudUrls";

describe("confluentCloudUrls", () => {
  describe("environmentUrl", () => {
    it("generates correct URL for environment ID", () => {
      expect(environmentUrl("env-abc123")).toBe(
        "https://confluent.cloud/environments/env-abc123",
      );
    });

    it("generates correct URL for another environment ID", () => {
      expect(environmentUrl("env-xyz999")).toBe(
        "https://confluent.cloud/environments/env-xyz999",
      );
    });
  });

  describe("clusterUrl", () => {
    it("generates correct URL for cluster with parent environment", () => {
      expect(clusterUrl("env-abc123", "lkc-def456")).toBe(
        "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456",
      );
    });

    it("generates correct URL for another cluster", () => {
      expect(clusterUrl("env-xyz999", "lkc-ghi789")).toBe(
        "https://confluent.cloud/environments/env-xyz999/clusters/lkc-ghi789",
      );
    });
  });

  describe("topicUrl", () => {
    it("generates correct URL for topic with cluster and environment", () => {
      expect(topicUrl("env-abc123", "lkc-def456", "my-topic")).toBe(
        "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/topics/my-topic",
      );
    });

    it("generates correct URL for topic with special characters in name", () => {
      expect(topicUrl("env-abc123", "lkc-def456", "orders.v2")).toBe(
        "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/topics/orders.v2",
      );
    });
  });

  describe("serviceAccountUrl", () => {
    it("returns per-principal URL for a service account ID", () => {
      expect(serviceAccountUrl("sa-abc123")).toBe(
        "https://confluent.cloud/settings/principals/sa-abc123?view=identity",
      );
    });

    it("returns per-principal URL for another service account ID", () => {
      expect(serviceAccountUrl("sa-xyz999")).toBe(
        "https://confluent.cloud/settings/principals/sa-xyz999?view=identity",
      );
    });
  });

  describe("schemaRegistryUrl", () => {
    it("generates correct URL for schema registry with parent environment", () => {
      expect(schemaRegistryUrl("env-abc123")).toBe(
        "https://confluent.cloud/environments/env-abc123/stream-governance/schema-registry/overview",
      );
    });

    it("generates correct URL for another schema registry environment", () => {
      expect(schemaRegistryUrl("env-xyz999")).toBe(
        "https://confluent.cloud/environments/env-xyz999/stream-governance/schema-registry/overview",
      );
    });
  });

  describe("connectorUrl", () => {
    it("generates correct URL for connector with cluster and environment", () => {
      expect(connectorUrl("env-abc123", "lkc-def456", "lcc-connector1")).toBe(
        "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/connectors/lcc-connector1",
      );
    });

    it("generates correct URL for another connector", () => {
      expect(connectorUrl("env-xyz999", "lkc-ghi789", "lcc-sink-01")).toBe(
        "https://confluent.cloud/environments/env-xyz999/clusters/lkc-ghi789/connectors/lcc-sink-01",
      );
    });
  });

  describe("userUrl", () => {
    it("returns per-principal URL for a user ID", () => {
      expect(userUrl("u-abc123")).toBe(
        "https://confluent.cloud/settings/principals/u-abc123?view=identity",
      );
    });

    it("returns per-principal URL for another user ID", () => {
      expect(userUrl("u-xyz999")).toBe(
        "https://confluent.cloud/settings/principals/u-xyz999?view=identity",
      );
    });
  });

  describe("identityProviderUrl", () => {
    it("returns workload identities OIDC view URL for a provider ID", () => {
      expect(identityProviderUrl("op-abc123")).toBe(
        "https://confluent.cloud/settings/org/workload_identities/provider/oidc/view/op-abc123",
      );
    });

    it("returns workload identities OIDC view URL for another provider ID", () => {
      expect(identityProviderUrl("op-xyz999")).toBe(
        "https://confluent.cloud/settings/org/workload_identities/provider/oidc/view/op-xyz999",
      );
    });
  });

  describe("identityPoolUrl", () => {
    it("returns per-principal URL for an identity pool ID", () => {
      expect(identityPoolUrl("pool-abc123")).toBe(
        "https://confluent.cloud/settings/principals/pool-abc123?view=identity",
      );
    });

    it("returns per-principal URL for another identity pool ID", () => {
      expect(identityPoolUrl("pool-xyz999")).toBe(
        "https://confluent.cloud/settings/principals/pool-xyz999?view=identity",
      );
    });
  });

  describe("apiKeyUrl", () => {
    it("returns api-keys edit URL for an API key ID", () => {
      expect(apiKeyUrl("TRFPF55LGU5RBQIT")).toBe(
        "https://confluent.cloud/settings/api-keys/edit/TRFPF55LGU5RBQIT",
      );
    });

    it("returns api-keys edit URL for another API key ID", () => {
      expect(apiKeyUrl("ABCD1234EFGH5678")).toBe(
        "https://confluent.cloud/settings/api-keys/edit/ABCD1234EFGH5678",
      );
    });
  });

  describe("flinkComputePoolUrl", () => {
    it("generates correct URL for a Flink compute pool with parent environment", () => {
      expect(flinkComputePoolUrl("env-abc123", "lfcp-def456")).toBe(
        "https://confluent.cloud/environments/env-abc123/flink/pools/lfcp-def456/overview",
      );
    });

    it("generates correct URL for another Flink compute pool", () => {
      expect(flinkComputePoolUrl("env-xyz999", "lfcp-ghi789")).toBe(
        "https://confluent.cloud/environments/env-xyz999/flink/pools/lfcp-ghi789/overview",
      );
    });
  });

  describe("ksqldbClusterUrl", () => {
    it("generates correct URL for a ksqlDB cluster", () => {
      expect(
        ksqldbClusterUrl("env-abc123", "lkc-def456", "lksqlc-ghi789"),
      ).toBe(
        "https://confluent.cloud/environments/env-abc123/clusters/lkc-def456/ksql/lksqlc-ghi789/editor",
      );
    });

    it("generates correct URL for another ksqlDB cluster", () => {
      expect(
        ksqldbClusterUrl("env-xyz999", "lkc-abc123", "lksqlc-def456"),
      ).toBe(
        "https://confluent.cloud/environments/env-xyz999/clusters/lkc-abc123/ksql/lksqlc-def456/editor",
      );
    });
  });
});
