from __future__ import annotations

import pytest
from dataclasses import FrozenInstanceError


def test_flink_statement_from_resource():
    from core.models import Resource, ResourceStatus
    from plugins.confluent_cloud.models import CCloudFlinkStatement

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-abc",
        resource_type="flink_statement",
        display_name="my-statement",
        parent_id="env-xyz",
        owner_id="sa-owner",
        status=ResourceStatus.ACTIVE,
        metadata={
            "statement_name": "my-statement",
            "compute_pool_id": "pool-001",
            "is_stopped": False,
        },
    )

    stmt = CCloudFlinkStatement.from_resource(resource)

    assert stmt.resource_id == "stmt-abc"
    assert stmt.statement_name == "my-statement"
    assert stmt.compute_pool_id == "pool-001"
    assert stmt.environment_id == "env-xyz"
    assert stmt.owner_id == "sa-owner"
    assert stmt.is_stopped is False


def test_flink_statement_missing_metadata_raises():
    from core.models import Resource
    from plugins.confluent_cloud.models import CCloudFlinkStatement

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-abc",
        resource_type="flink_statement",
        metadata={},  # Missing required keys
    )

    with pytest.raises(KeyError):
        CCloudFlinkStatement.from_resource(resource)


def test_flink_statement_optional_fields():
    from core.models import Resource
    from plugins.confluent_cloud.models import CCloudFlinkStatement

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="stmt-abc",
        resource_type="flink_statement",
        # No parent_id, no owner_id
        metadata={
            "statement_name": "my-statement",
            "compute_pool_id": "pool-001",
            # No is_stopped - should default to False
        },
    )

    stmt = CCloudFlinkStatement.from_resource(resource)

    assert stmt.environment_id == ""
    assert stmt.owner_id == ""
    assert stmt.is_stopped is False


def test_flink_pool_from_resource():
    from core.models import Resource
    from plugins.confluent_cloud.models import CCloudFlinkPool

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="pool-001",
        resource_type="flink_compute_pool",
        display_name="my-pool",
        parent_id="env-xyz",
        metadata={
            "cloud": "aws",
            "region": "us-east-1",
            "max_cfu": 10,
        },
    )

    pool = CCloudFlinkPool.from_resource(resource)

    assert pool.resource_id == "pool-001"
    assert pool.pool_name == "my-pool"
    assert pool.cloud == "aws"
    assert pool.region == "us-east-1"
    assert pool.max_cfu == 10


def test_flink_pool_optional_fields():
    from core.models import Resource
    from plugins.confluent_cloud.models import CCloudFlinkPool

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="pool-001",
        resource_type="flink_compute_pool",
        # No display_name, no parent_id
        metadata={},  # No cloud, region, max_cfu
    )

    pool = CCloudFlinkPool.from_resource(resource)

    assert pool.pool_name == "pool-001"  # Falls back to resource_id
    assert pool.environment_id == ""
    assert pool.cloud == ""
    assert pool.region == ""
    assert pool.max_cfu == 0


def test_connector_from_resource():
    from core.models import Resource, ResourceStatus
    from plugins.confluent_cloud.models import CCloudConnector

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="conn-001",
        resource_type="connector",
        display_name="my-connector",
        parent_id="env-xyz",
        owner_id="sa-creator",
        status=ResourceStatus.DELETED,
        metadata={
            "connector_class": "io.confluent.kafka.connect.s3.S3SinkConnector",
            "cluster_id": "lkc-123",
        },
    )

    conn = CCloudConnector.from_resource(resource)

    assert conn.resource_id == "conn-001"
    assert conn.connector_name == "my-connector"
    assert conn.connector_class == "io.confluent.kafka.connect.s3.S3SinkConnector"
    assert conn.cluster_id == "lkc-123"
    assert conn.is_deleted is True


def test_connector_active_status():
    from core.models import Resource, ResourceStatus
    from plugins.confluent_cloud.models import CCloudConnector

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="conn-001",
        resource_type="connector",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )

    conn = CCloudConnector.from_resource(resource)

    assert conn.is_deleted is False


def test_views_are_frozen():
    from core.models import Resource
    from plugins.confluent_cloud.models import CCloudFlinkPool

    resource = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="pool-001",
        resource_type="flink_compute_pool",
        metadata={"cloud": "aws", "region": "us-east-1", "max_cfu": 10},
    )

    pool = CCloudFlinkPool.from_resource(resource)

    with pytest.raises(FrozenInstanceError):
        pool.max_cfu = 20  # type: ignore[misc]
