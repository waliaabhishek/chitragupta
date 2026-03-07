from __future__ import annotations

from datetime import UTC, datetime

import httpx
import respx
from pydantic import SecretStr

from core.models import CoreResource, ResourceStatus
from plugins.confluent_cloud.connections import CCloudConnection


class TestGatherEnvironments:
    """Tests for gather_environments()."""

    @respx.mock
    def test_gather_environments_standard(self):
        from plugins.confluent_cloud.gathering import gather_environments

        respx.get("https://api.confluent.cloud/org/v2/environments").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "env-abc",
                            "display_name": "production",
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        envs = list(gather_environments(conn, "confluent_cloud", "org-123"))

        assert len(envs) == 1
        assert envs[0].ecosystem == "confluent_cloud"
        assert envs[0].tenant_id == "org-123"
        assert envs[0].resource_id == "env-abc"
        assert envs[0].resource_type == "environment"
        assert envs[0].display_name == "production"
        assert envs[0].status == ResourceStatus.ACTIVE
        assert envs[0].created_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    @respx.mock
    def test_gather_environments_empty(self):
        from plugins.confluent_cloud.gathering import gather_environments

        respx.get("https://api.confluent.cloud/org/v2/environments").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        envs = list(gather_environments(conn, "confluent_cloud", "org-123"))

        assert envs == []

    @respx.mock
    def test_gather_environments_missing_created_at(self):
        """Environments without created_at should have created_at=None."""
        from plugins.confluent_cloud.gathering import gather_environments

        respx.get("https://api.confluent.cloud/org/v2/environments").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "env-no-created",
                            "display_name": "staging",
                            "metadata": {},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        envs = list(gather_environments(conn, "confluent_cloud", "org-123"))

        assert len(envs) == 1
        assert envs[0].resource_id == "env-no-created"
        assert envs[0].created_at is None


class TestGatherKafkaClusters:
    """Tests for gather_kafka_clusters()."""

    @respx.mock
    def test_gather_kafka_clusters_single_env(self):
        from plugins.confluent_cloud.gathering import gather_kafka_clusters

        respx.get("https://api.confluent.cloud/cmk/v2/clusters").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lkc-123",
                            "spec": {
                                "display_name": "prod-cluster",
                                "environment": {"id": "env-abc"},
                                "kafka_bootstrap_endpoint": "pkc-123.us-east-1.aws.confluent.cloud:9092",
                                "cloud": "AWS",
                                "region": "us-east-1",
                            },
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        clusters = list(gather_kafka_clusters(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert len(clusters) == 1
        assert clusters[0].ecosystem == "confluent_cloud"
        assert clusters[0].tenant_id == "org-123"
        assert clusters[0].resource_id == "lkc-123"
        assert clusters[0].resource_type == "kafka_cluster"
        assert clusters[0].display_name == "prod-cluster"
        assert clusters[0].parent_id == "env-abc"
        assert clusters[0].status == ResourceStatus.ACTIVE
        assert clusters[0].metadata["bootstrap_url"] == "pkc-123.us-east-1.aws.confluent.cloud:9092"
        assert clusters[0].metadata["cloud"] == "aws"  # Normalized to lowercase
        assert clusters[0].metadata["region"] == "us-east-1"

    @respx.mock
    def test_gather_kafka_clusters_multiple_envs(self):
        """Clusters are gathered per environment (fan-out)."""
        from plugins.confluent_cloud.gathering import gather_kafka_clusters

        route = respx.get("https://api.confluent.cloud/cmk/v2/clusters")
        route.side_effect = [
            httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lkc-1",
                            "spec": {
                                "display_name": "cluster-1",
                                "environment": {"id": "env-1"},
                                "cloud": "AWS",
                                "region": "us-east-1",
                            },
                            "metadata": {},
                        }
                    ],
                    "metadata": {},
                },
            ),
            httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lkc-2",
                            "spec": {
                                "display_name": "cluster-2",
                                "environment": {"id": "env-2"},
                                "cloud": "GCP",
                                "region": "us-central1",
                            },
                            "metadata": {},
                        }
                    ],
                    "metadata": {},
                },
            ),
        ]

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        clusters = list(gather_kafka_clusters(conn, "confluent_cloud", "org-123", ["env-1", "env-2"]))

        assert len(clusters) == 2
        assert {c.resource_id for c in clusters} == {"lkc-1", "lkc-2"}
        assert len(respx.calls) == 2

    @respx.mock
    def test_gather_kafka_clusters_empty_env(self):
        from plugins.confluent_cloud.gathering import gather_kafka_clusters

        respx.get("https://api.confluent.cloud/cmk/v2/clusters").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        clusters = list(gather_kafka_clusters(conn, "confluent_cloud", "org-123", ["env-empty"]))

        assert clusters == []

    @respx.mock
    def test_gather_kafka_clusters_no_envs(self):
        """No environments = no API calls, empty result."""
        from plugins.confluent_cloud.gathering import gather_kafka_clusters

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        clusters = list(gather_kafka_clusters(conn, "confluent_cloud", "org-123", []))

        assert clusters == []
        assert len(respx.calls) == 0


class TestGatherConnectors:
    """Tests for gather_connectors()."""

    @respx.mock
    def test_gather_connectors_service_account_mode(self):
        from plugins.confluent_cloud.gathering import gather_connectors

        # Connector API returns dict-of-dicts, not standard envelope
        respx.get("https://api.confluent.cloud/connect/v1/environments/env-abc/clusters/lkc-123/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "my-sink": {
                        "info": {
                            "config": {
                                "name": "my-sink",
                                "connector.class": "S3Sink",
                                "kafka.auth.mode": "SERVICE_ACCOUNT",
                                "kafka.service.account.id": "sa-owner1",
                            }
                        },
                        "id": {"id": "lcc-sink1"},
                    },
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-abc", "lkc-123")],
            )
        )

        assert len(connectors) == 1
        assert connectors[0].resource_id == "lcc-sink1"
        assert connectors[0].resource_type == "connector"
        assert connectors[0].display_name == "my-sink"
        assert connectors[0].parent_id == "lkc-123"
        assert connectors[0].metadata["kafka_auth_mode"] == "SERVICE_ACCOUNT"
        assert connectors[0].metadata["kafka_service_account_id"] == "sa-owner1"

    @respx.mock
    def test_gather_connectors_api_key_mode(self):
        from plugins.confluent_cloud.gathering import gather_connectors

        respx.get("https://api.confluent.cloud/connect/v1/environments/env-abc/clusters/lkc-123/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "api-key-connector": {
                        "info": {
                            "config": {
                                "name": "api-key-connector",
                                "connector.class": "S3Source",
                                "kafka.auth.mode": "KAFKA_API_KEY",
                                "kafka.api.key": "ABCD1234",
                            }
                        },
                        "id": {"id": "lcc-source1"},
                    },
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-abc", "lkc-123")],
            )
        )

        assert len(connectors) == 1
        assert connectors[0].metadata["kafka_auth_mode"] == "KAFKA_API_KEY"
        assert connectors[0].metadata["kafka_api_key"] == "ABCD1234"

    @respx.mock
    def test_gather_connectors_empty_cluster(self):
        from plugins.confluent_cloud.gathering import gather_connectors

        # Empty response (404 or no connectors) returns {}
        respx.get("https://api.confluent.cloud/connect/v1/environments/env-abc/clusters/lkc-123/connectors").mock(
            return_value=httpx.Response(200, json={})
        )  # get_raw returns {} on 404 or empty

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-abc", "lkc-123")],
            )
        )

        assert connectors == []

    @respx.mock
    def test_gather_connectors_multiple_clusters(self):
        """Fan-out across multiple clusters."""
        from plugins.confluent_cloud.gathering import gather_connectors

        # First cluster
        respx.get("https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "conn1": {
                        "info": {"config": {"name": "conn1"}},
                        "id": {"id": "lcc-1"},
                    },
                },
            )
        )
        # Second cluster
        respx.get("https://api.confluent.cloud/connect/v1/environments/env-2/clusters/lkc-2/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "conn2": {
                        "info": {"config": {"name": "conn2"}},
                        "id": {"id": "lcc-2"},
                    },
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-1", "lkc-1"), ("env-2", "lkc-2")],
            )
        )

        assert len(connectors) == 2
        assert {c.resource_id for c in connectors} == {"lcc-1", "lcc-2"}

    @respx.mock
    def test_gather_connectors_probes_api_key_when_no_auth_mode(self):
        """No kafka.auth.mode but kafka.api.key present → auth_mode resolved as KAFKA_API_KEY."""
        from plugins.confluent_cloud.gathering import gather_connectors

        respx.get("https://api.confluent.cloud/connect/v1/environments/env-abc/clusters/lkc-123/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "probe-connector": {
                        "info": {
                            "config": {
                                "name": "probe-connector",
                                "connector.class": "S3Sink",
                                # No kafka.auth.mode field
                                "kafka.api.key": "PROBE_KEY_123",
                            }
                        },
                        "id": {"id": "lcc-probe1"},
                    },
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-abc", "lkc-123")],
            )
        )

        assert len(connectors) == 1
        assert connectors[0].metadata["kafka_auth_mode"] == "KAFKA_API_KEY"
        assert connectors[0].metadata["kafka_api_key"] == "PROBE_KEY_123"

    @respx.mock
    def test_gather_connectors_probes_service_account_when_no_auth_mode(self):
        """No kafka.auth.mode but kafka.service.account.id present → auth_mode resolved as SERVICE_ACCOUNT."""
        from plugins.confluent_cloud.gathering import gather_connectors

        respx.get("https://api.confluent.cloud/connect/v1/environments/env-abc/clusters/lkc-123/connectors").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sa-probe-connector": {
                        "info": {
                            "config": {
                                "name": "sa-probe-connector",
                                "connector.class": "S3Source",
                                # No kafka.auth.mode field
                                "kafka.service.account.id": "sa-probed-456",
                            }
                        },
                        "id": {"id": "lcc-probe2"},
                    },
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        connectors = list(
            gather_connectors(
                conn,
                "confluent_cloud",
                "org-123",
                clusters=[("env-abc", "lkc-123")],
            )
        )

        assert len(connectors) == 1
        assert connectors[0].metadata["kafka_auth_mode"] == "SERVICE_ACCOUNT"
        assert connectors[0].metadata["kafka_service_account_id"] == "sa-probed-456"


class TestGatherSchemaRegistries:
    """Tests for gather_schema_registries()."""

    @respx.mock
    def test_gather_schema_registries_standard(self):
        from plugins.confluent_cloud.gathering import gather_schema_registries

        respx.get("https://api.confluent.cloud/srcm/v3/clusters").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lsrc-abc",
                            "spec": {
                                "display_name": "my-sr",
                                "environment": {"id": "env-abc"},
                                "cloud": "AWS",
                                "region": "us-east-1",
                                "http_endpoint": "https://psrc-123.us-east-1.aws.confluent.cloud",
                            },
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                                "resource_name": "crn://confluent.cloud/schema-registry=lsrc-abc",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        srs = list(gather_schema_registries(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert len(srs) == 1
        assert srs[0].resource_id == "lsrc-abc"
        assert srs[0].resource_type == "schema_registry"
        assert srs[0].display_name == "my-sr"
        assert srs[0].parent_id == "env-abc"
        assert srs[0].metadata["http_endpoint"] == "https://psrc-123.us-east-1.aws.confluent.cloud"
        assert srs[0].metadata["cloud"] == "aws"  # Normalized
        assert srs[0].metadata["region"] == "us-east-1"

    @respx.mock
    def test_gather_schema_registries_empty(self):
        from plugins.confluent_cloud.gathering import gather_schema_registries

        respx.get("https://api.confluent.cloud/srcm/v3/clusters").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        srs = list(gather_schema_registries(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert srs == []


class TestGatherKsqldbClusters:
    """Tests for gather_ksqldb_clusters()."""

    @respx.mock
    def test_gather_ksqldb_clusters_standard(self):
        from plugins.confluent_cloud.gathering import gather_ksqldb_clusters

        respx.get("https://api.confluent.cloud/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lksqlc-123",
                            "spec": {
                                "display_name": "my-ksql",
                                "environment": {"id": "env-abc"},
                                "kafka_cluster": {"id": "lkc-123"},
                                "credential_identity": {"id": "sa-owner"},
                                "csu": 4,
                            },
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        ksqls = list(gather_ksqldb_clusters(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert len(ksqls) == 1
        assert ksqls[0].resource_id == "lksqlc-123"
        assert ksqls[0].resource_type == "ksqldb_cluster"
        assert ksqls[0].display_name == "my-ksql"
        assert ksqls[0].parent_id == "env-abc"
        assert ksqls[0].owner_id == "sa-owner"
        assert ksqls[0].metadata["kafka_cluster_id"] == "lkc-123"
        assert ksqls[0].metadata["csu_count"] == 4

    @respx.mock
    def test_gather_ksqldb_missing_owner(self):
        """Missing credential_identity should use sentinel fallback."""
        from plugins.confluent_cloud.gathering import gather_ksqldb_clusters

        respx.get("https://api.confluent.cloud/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lksqlc-123",
                            "spec": {
                                "display_name": "my-ksql",
                                "environment": {"id": "env-abc"},
                            },
                            "metadata": {},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        ksqls = list(gather_ksqldb_clusters(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert ksqls[0].owner_id == "ksqldb_owner_unknown"  # Sentinel fallback


class TestGatherFlinkComputePools:
    """Tests for gather_flink_compute_pools()."""

    @respx.mock
    def test_gather_flink_compute_pools_allocatable(self):
        from plugins.confluent_cloud.gathering import gather_flink_compute_pools

        respx.get("https://api.confluent.cloud/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lfcp-abc",
                            "spec": {
                                "display_name": "my-pool",
                                "cloud": "aws",
                                "region": "us-east-1",
                            },
                            "metadata": {
                                "resource_name": "crn://confluent.cloud/organization=org-123/environment=env-abc/flink-compute-pool=lfcp-abc",
                                "created_at": "2024-01-01T00:00:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        # flink_regions: region_id -> (api_key, api_secret) - lowercase keys
        flink_regions = {"us-east-1": ("flink-key", "flink-secret")}
        pools = list(gather_flink_compute_pools(conn, "confluent_cloud", "org-123", ["env-abc"], flink_regions))

        assert len(pools) == 1
        assert pools[0].resource_id == "lfcp-abc"
        assert pools[0].resource_type == "flink_compute_pool"
        assert pools[0].metadata["is_allocatable"] is True
        assert pools[0].metadata["cloud"] == "aws"
        assert pools[0].metadata["region"] == "us-east-1"

    @respx.mock
    def test_gather_flink_compute_pools_normalizes_region_cloud(self):
        """Verify cloud and region are normalized (lowercase, stripped)."""
        from plugins.confluent_cloud.gathering import gather_flink_compute_pools

        respx.get("https://api.confluent.cloud/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lfcp-abc",
                            "spec": {
                                "display_name": "my-pool",
                                "cloud": "  AWS  ",  # Spaces + uppercase
                                "region": " US-EAST-1 ",  # Spaces + uppercase
                            },
                            "metadata": {},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        # Config uses lowercase keys
        flink_regions = {"us-east-1": ("flink-key", "flink-secret")}
        pools = list(gather_flink_compute_pools(conn, "confluent_cloud", "org-123", ["env-abc"], flink_regions))

        assert len(pools) == 1
        assert pools[0].metadata["cloud"] == "aws"
        assert pools[0].metadata["region"] == "us-east-1"
        # Allocatability check should match after normalization
        assert pools[0].metadata["is_allocatable"] is True

    @respx.mock
    def test_gather_flink_compute_pools_not_allocatable(self):
        from plugins.confluent_cloud.gathering import gather_flink_compute_pools

        respx.get("https://api.confluent.cloud/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "lfcp-abc",
                            "spec": {
                                "display_name": "my-pool",
                                "cloud": "aws",
                                "region": "us-west-2",
                            },
                            "metadata": {"resource_name": "crn://..."},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        # No matching region config
        flink_regions = {"us-east-1": ("key", "secret")}
        pools = list(gather_flink_compute_pools(conn, "confluent_cloud", "org-123", ["env-abc"], flink_regions))

        assert pools[0].metadata["is_allocatable"] is False


class TestGatherFlinkStatements:
    """Tests for gather_flink_statements()."""

    @respx.mock
    def test_gather_flink_statements_standard(self):
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        # Regional Flink API
        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-abc/statements"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "metadata": {"uid": "stmt-uid-123"},
                            "name": "my-statement",
                            "spec": {"principal": "sa-owner", "compute_pool": {"id": "lfcp-abc"}},
                            "status": {"phase": "RUNNING"},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        pool = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-abc",
            resource_type="flink_compute_pool",
            parent_id="env-abc",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )

        statements = list(
            gather_flink_statements(
                "confluent_cloud",
                "org-123",
                allocatable_pools=[(pool, "flink-key", "flink-secret")],
            )
        )

        assert len(statements) == 1
        assert statements[0].resource_id == "stmt-uid-123"
        assert statements[0].resource_type == "flink_statement"
        assert statements[0].owner_id == "sa-owner"
        assert statements[0].metadata["statement_name"] == "my-statement"
        assert statements[0].metadata["compute_pool_id"] == "lfcp-abc"
        assert statements[0].metadata["is_stopped"] is False

    @respx.mock
    def test_gather_flink_statements_stopped(self):
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-abc/statements"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "metadata": {"uid": "stmt-stopped"},
                            "name": "stopped-stmt",
                            "spec": {"principal": "sa-owner"},
                            "status": {"phase": "COMPLETED"},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        pool = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-abc",
            resource_type="flink_compute_pool",
            parent_id="env-abc",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )

        stmts = list(
            gather_flink_statements(
                "confluent_cloud",
                "org-123",
                allocatable_pools=[(pool, "k", "s")],
            )
        )

        assert stmts[0].metadata["is_stopped"] is True

    @respx.mock
    def test_gather_flink_statements_regional_url(self):
        """Verify correct regional base URL is constructed."""
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        # Expect request to eu-central-1.gcp regional URL
        respx.get(
            "https://flink.eu-central-1.gcp.confluent.cloud/sql/v1/organizations/org-123/environments/env-xyz/statements"
        ).mock(return_value=httpx.Response(200, json={"data": [], "metadata": {}}))

        pool = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-eu",
            resource_type="flink_compute_pool",
            parent_id="env-xyz",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "gcp", "region": "eu-central-1", "is_allocatable": True},
        )

        list(
            gather_flink_statements(
                "confluent_cloud",
                "org-123",
                allocatable_pools=[(pool, "eu-key", "eu-secret")],
            )
        )

        # Verify the call was made to the correct regional URL
        assert len(respx.calls) == 1
        assert "flink.eu-central-1.gcp.confluent.cloud" in str(respx.calls[0].request.url)

    @respx.mock
    def test_gather_flink_statements_missing_id_uses_sentinel(self):
        """Verify deterministic sentinel fallback when uid and name are missing."""
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-abc/statements"
        ).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "metadata": {},  # No uid
                            # No name
                            "spec": {"principal": "sa-owner"},
                            "status": {"phase": "RUNNING"},
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        pool = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-abc",
            resource_type="flink_compute_pool",
            parent_id="env-abc",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )

        stmts = list(
            gather_flink_statements(
                "confluent_cloud",
                "org-123",
                allocatable_pools=[(pool, "k", "s")],
            )
        )

        # Sentinel format: flink_stmt_unknown_{hash[:12]}
        assert len(stmts) == 1
        assert stmts[0].resource_id.startswith("flink_stmt_unknown_")
        assert len(stmts[0].resource_id) == len("flink_stmt_unknown_") + 12

    @respx.mock
    def test_gather_flink_statements_connection_reuse(self):
        """Verify connections are reused for pools in the same region."""
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        # Two pools in same region should reuse connection
        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-a/statements"
        ).mock(return_value=httpx.Response(200, json={"data": [], "metadata": {}}))
        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-b/statements"
        ).mock(return_value=httpx.Response(200, json={"data": [], "metadata": {}}))

        pool1 = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-1",
            resource_type="flink_compute_pool",
            parent_id="env-a",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )
        pool2 = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-2",
            resource_type="flink_compute_pool",
            parent_id="env-b",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )

        # Same credentials → same cache key → connection reused
        list(
            gather_flink_statements(
                "confluent_cloud",
                "org-123",
                allocatable_pools=[
                    (pool1, "shared-key", "shared-secret"),
                    (pool2, "shared-key", "shared-secret"),
                ],
            )
        )

        # Both requests made to same regional endpoint
        assert len(respx.calls) == 2


class TestGatherServiceAccounts:
    """Tests for gather_service_accounts()."""

    @respx.mock
    def test_gather_service_accounts_standard(self):
        from plugins.confluent_cloud.gathering import gather_service_accounts

        respx.get("https://api.confluent.cloud/iam/v2/service-accounts").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "sa-abc123",
                            "display_name": "my-service-account",
                            "description": "Production SA",
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        sas = list(gather_service_accounts(conn, "confluent_cloud", "org-123"))

        assert len(sas) == 1
        assert sas[0].identity_id == "sa-abc123"
        assert sas[0].identity_type == "service_account"
        assert sas[0].display_name == "my-service-account"
        assert sas[0].metadata["description"] == "Production SA"
        assert sas[0].created_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


class TestGatherUsers:
    """Tests for gather_users()."""

    @respx.mock
    def test_gather_users_standard(self):
        from plugins.confluent_cloud.gathering import gather_users

        respx.get("https://api.confluent.cloud/iam/v2/users").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "u-abc123",
                            "full_name": "John Doe",
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                                "resource_name": "crn://confluent.cloud/user=u-abc123",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        users = list(gather_users(conn, "confluent_cloud", "org-123"))

        assert len(users) == 1
        assert users[0].identity_id == "u-abc123"
        assert users[0].identity_type == "user"
        assert users[0].display_name == "John Doe"


class TestGatherApiKeys:
    """Tests for gather_api_keys()."""

    @respx.mock
    def test_gather_api_keys_standard(self):
        from plugins.confluent_cloud.gathering import gather_api_keys

        respx.get("https://api.confluent.cloud/iam/v2/api-keys").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "ABCD1234",
                            "spec": {
                                "description": "Production key",
                                "owner": {"id": "sa-owner1"},
                                "resource": {"id": "lkc-123"},
                            },
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        keys = list(gather_api_keys(conn, "confluent_cloud", "org-123"))

        assert len(keys) == 1
        assert keys[0].identity_id == "ABCD1234"
        assert keys[0].identity_type == "api_key"
        assert keys[0].metadata["owner_id"] == "sa-owner1"
        assert keys[0].metadata["resource_id"] == "lkc-123"


class TestGatherIdentityProviders:
    """Tests for gather_identity_providers()."""

    @respx.mock
    def test_gather_identity_providers_standard(self):
        from plugins.confluent_cloud.gathering import gather_identity_providers

        respx.get("https://api.confluent.cloud/iam/v2/identity-providers").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "op-abc",
                            "display_name": "Okta SSO",
                            "description": "Corporate SSO provider",
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                                "resource_name": "crn://confluent.cloud/identity-provider=op-abc",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        providers = list(gather_identity_providers(conn, "confluent_cloud", "org-123"))

        assert len(providers) == 1
        assert providers[0].identity_id == "op-abc"
        assert providers[0].identity_type == "identity_provider"
        assert providers[0].display_name == "Okta SSO"


class TestGatherIdentityPools:
    """Tests for gather_identity_pools()."""

    @respx.mock
    def test_gather_identity_pools_standard(self):
        from plugins.confluent_cloud.gathering import gather_identity_pools

        respx.get("https://api.confluent.cloud/iam/v2/identity-providers/op-abc/identity-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "pool-xyz",
                            "display_name": "Engineering",
                            "description": "Engineering identity pool",
                            "metadata": {
                                "created_at": "2024-01-15T10:30:00Z",
                            },
                        }
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        pools = list(gather_identity_pools(conn, "confluent_cloud", "org-123", ["op-abc"]))

        assert len(pools) == 1
        assert pools[0].identity_id == "pool-xyz"
        assert pools[0].identity_type == "identity_pool"
        assert pools[0].metadata["provider_id"] == "op-abc"

    @respx.mock
    def test_gather_identity_pools_multiple_providers(self):
        """Fan-out across multiple providers."""
        from plugins.confluent_cloud.gathering import gather_identity_pools

        respx.get("https://api.confluent.cloud/iam/v2/identity-providers/op-1/identity-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [{"id": "pool-1", "display_name": "Pool 1", "metadata": {}}],
                    "metadata": {},
                },
            )
        )
        respx.get("https://api.confluent.cloud/iam/v2/identity-providers/op-2/identity-pools").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [{"id": "pool-2", "display_name": "Pool 2", "metadata": {}}],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        pools = list(gather_identity_pools(conn, "confluent_cloud", "org-123", ["op-1", "op-2"]))

        assert len(pools) == 2
        assert {p.identity_id for p in pools} == {"pool-1", "pool-2"}


class TestParseIsoDatetime:
    """Tests for _parse_iso_datetime() helper."""

    def test_parse_iso_datetime_with_z_suffix(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        result = _parse_iso_datetime("2024-01-15T10:30:00Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_parse_iso_datetime_with_offset(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        result = _parse_iso_datetime("2024-01-15T10:30:00+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_parse_iso_datetime_with_microseconds(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        result = _parse_iso_datetime("2024-01-15T10:30:00.123456Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, 123456, tzinfo=UTC)

    def test_parse_iso_datetime_none(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        assert _parse_iso_datetime(None) is None

    def test_parse_iso_datetime_empty_string(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        assert _parse_iso_datetime("") is None

    def test_parse_iso_datetime_invalid(self):
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        # Should return None and log warning, not raise
        assert _parse_iso_datetime("not-a-datetime") is None

    def test_parse_iso_datetime_naive_gets_utc(self):
        """Naive datetime (no timezone) should be treated as UTC."""
        from plugins.confluent_cloud.gathering import _parse_iso_datetime

        result = _parse_iso_datetime("2024-01-15T10:30:00")
        assert result is not None
        assert result.tzinfo == UTC


class TestPageSizeOverrides:
    """Tests for GAP-05: per-endpoint page_size tuning."""

    @respx.mock
    def test_flink_compute_pools_uses_page_size_50(self) -> None:
        from plugins.confluent_cloud.gathering import gather_flink_compute_pools

        respx.get("https://api.confluent.cloud/fcpm/v2/compute-pools").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        list(gather_flink_compute_pools(conn, "confluent_cloud", "org-123", ["env-abc"], {}))

        assert len(respx.calls) == 1
        url_params = str(respx.calls[0].request.url)
        assert "page_size=50&" in url_params or url_params.endswith("page_size=50")

    @respx.mock
    def test_flink_statements_uses_page_size_50(self) -> None:
        from core.models import ResourceStatus
        from plugins.confluent_cloud.gathering import gather_flink_statements

        respx.get(
            "https://flink.us-east-1.aws.confluent.cloud/sql/v1/organizations/org-123/environments/env-abc/statements"
        ).mock(return_value=httpx.Response(200, json={"data": [], "metadata": {}}))

        pool = CoreResource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="lfcp-abc",
            resource_type="flink_compute_pool",
            parent_id="env-abc",
            status=ResourceStatus.ACTIVE,
            metadata={"cloud": "aws", "region": "us-east-1", "is_allocatable": True},
        )

        list(gather_flink_statements("confluent_cloud", "org-123", allocatable_pools=[(pool, "k", "s")]))

        assert len(respx.calls) == 1
        url_params = str(respx.calls[0].request.url)
        assert "page_size=50&" in url_params or url_params.endswith("page_size=50")

    @respx.mock
    def test_ksqldb_uses_page_size_100(self) -> None:
        from plugins.confluent_cloud.gathering import gather_ksqldb_clusters

        respx.get("https://api.confluent.cloud/ksqldbcm/v2/clusters").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        list(gather_ksqldb_clusters(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert len(respx.calls) == 1
        url_params = str(respx.calls[0].request.url)
        assert "page_size=100&" in url_params or url_params.endswith("page_size=100")

    @respx.mock
    def test_api_keys_uses_page_size_100(self) -> None:
        from plugins.confluent_cloud.gathering import gather_api_keys

        respx.get("https://api.confluent.cloud/iam/v2/api-keys").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        list(gather_api_keys(conn, "confluent_cloud", "org-123"))

        assert len(respx.calls) == 1
        url_params = str(respx.calls[0].request.url)
        assert "page_size=100&" in url_params or url_params.endswith("page_size=100")

    @respx.mock
    def test_schema_registry_uses_page_size_50(self) -> None:
        from plugins.confluent_cloud.gathering import gather_schema_registries

        respx.get("https://api.confluent.cloud/srcm/v3/clusters").mock(
            return_value=httpx.Response(200, json={"data": [], "metadata": {}})
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        list(gather_schema_registries(conn, "confluent_cloud", "org-123", ["env-abc"]))

        assert len(respx.calls) == 1
        url_params = str(respx.calls[0].request.url)
        assert "page_size=50&" in url_params or url_params.endswith("page_size=50")
