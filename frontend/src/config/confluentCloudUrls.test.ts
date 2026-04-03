import { describe, expect, it } from "vitest";
import {
  environmentUrl,
  clusterUrl,
  topicUrl,
  serviceAccountUrl,
  schemaRegistryUrl,
  connectorUrl,
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
    it("returns org-level service accounts URL (no per-account path)", () => {
      expect(serviceAccountUrl()).toBe(
        "https://confluent.cloud/settings/org/service-accounts",
      );
    });

    it("returns same URL regardless of service account ID (org-level only)", () => {
      const url1 = serviceAccountUrl();
      const url2 = serviceAccountUrl();
      expect(url1).toBe(url2);
      expect(url1).not.toContain("sa-");
    });
  });

  describe("schemaRegistryUrl", () => {
    it("generates correct URL for schema registry with parent environment", () => {
      expect(schemaRegistryUrl("env-abc123", "lsrc-def456")).toBe(
        "https://confluent.cloud/environments/env-abc123/schema-registry/lsrc-def456",
      );
    });

    it("generates correct URL for another schema registry", () => {
      expect(schemaRegistryUrl("env-xyz999", "lsrc-ghi789")).toBe(
        "https://confluent.cloud/environments/env-xyz999/schema-registry/lsrc-ghi789",
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
});
