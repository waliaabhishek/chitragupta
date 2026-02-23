from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from plugins.confluent_cloud.models import CCloudConnector, CCloudFlinkPool, CCloudFlinkStatement


def test_flink_statement_from_resource(make_resource):
    resource = make_resource(
        resource_id="stmt-abc",
        resource_type="flink_statement",
        display_name="my-statement",
        parent_id="env-xyz",
        owner_id="sa-owner",
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


def test_flink_statement_missing_metadata_raises(make_resource):
    resource = make_resource(
        resource_id="stmt-abc",
        resource_type="flink_statement",
        metadata={},
    )

    with pytest.raises(KeyError):
        CCloudFlinkStatement.from_resource(resource)


def test_flink_statement_optional_fields(make_resource):
    resource = make_resource(
        resource_id="stmt-abc",
        resource_type="flink_statement",
        metadata={
            "statement_name": "my-statement",
            "compute_pool_id": "pool-001",
        },
    )

    stmt = CCloudFlinkStatement.from_resource(resource)

    assert stmt.environment_id == ""
    assert stmt.owner_id == ""
    assert stmt.is_stopped is False


def test_flink_pool_from_resource(make_resource):
    resource = make_resource(
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


def test_flink_pool_optional_fields(make_resource):
    resource = make_resource(
        resource_id="pool-001",
        resource_type="flink_compute_pool",
        metadata={},
    )

    pool = CCloudFlinkPool.from_resource(resource)

    assert pool.pool_name == "pool-001"  # Falls back to resource_id
    assert pool.environment_id == ""
    assert pool.cloud == ""
    assert pool.region == ""
    assert pool.max_cfu == 0


def test_connector_from_resource(make_resource):
    from core.models import ResourceStatus

    resource = make_resource(
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


def test_connector_active_status(make_resource):
    resource = make_resource(
        resource_id="conn-001",
        resource_type="connector",
        metadata={},
    )

    conn = CCloudConnector.from_resource(resource)

    assert conn.is_deleted is False


def test_views_are_frozen(make_resource):
    resource = make_resource(
        resource_id="pool-001",
        resource_type="flink_compute_pool",
        metadata={"cloud": "aws", "region": "us-east-1", "max_cfu": 10},
    )

    pool = CCloudFlinkPool.from_resource(resource)

    with pytest.raises(FrozenInstanceError):
        pool.max_cfu = 20  # type: ignore[misc]
