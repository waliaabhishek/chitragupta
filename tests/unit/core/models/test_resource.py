from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime

from core.models.resource import Resource, ResourceStatus

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


class TestResourceStatus:
    def test_values(self) -> None:
        assert ResourceStatus.ACTIVE == "active"
        assert ResourceStatus.DELETED == "deleted"

    def test_string_conversion(self) -> None:
        assert str(ResourceStatus.ACTIVE) == "active"
        assert ResourceStatus("deleted") is ResourceStatus.DELETED


class TestResource:
    def test_construction_all_fields(self) -> None:
        r = Resource(
            ecosystem="confluent",
            tenant_id="t-001",
            resource_id="lkc-abc",
            resource_type="kafka_cluster",
            display_name="my-cluster",
            parent_id="env-xyz",
            owner_id="u-owner",
            status=ResourceStatus.DELETED,
            created_at=_NOW,
            deleted_at=_NOW,
            last_seen_at=_NOW,
            metadata={"region": "us-east-1"},
        )
        assert r.ecosystem == "confluent"
        assert r.tenant_id == "t-001"
        assert r.resource_id == "lkc-abc"
        assert r.resource_type == "kafka_cluster"
        assert r.display_name == "my-cluster"
        assert r.parent_id == "env-xyz"
        assert r.owner_id == "u-owner"
        assert r.status is ResourceStatus.DELETED
        assert r.created_at == _NOW
        assert r.deleted_at == _NOW
        assert r.last_seen_at == _NOW
        assert r.metadata == {"region": "us-east-1"}

    def test_defaults_only(self) -> None:
        r = Resource(
            ecosystem="aws",
            tenant_id="t-002",
            resource_id="r-123",
            resource_type="ec2_instance",
        )
        assert r.display_name is None
        assert r.parent_id is None
        assert r.owner_id is None
        assert r.status is ResourceStatus.ACTIVE
        assert r.created_at is None
        assert r.deleted_at is None
        assert r.last_seen_at is None
        assert r.metadata == {}

    def test_metadata_independence(self) -> None:
        r1 = Resource(ecosystem="a", tenant_id="t", resource_id="r1", resource_type="x")
        r2 = Resource(ecosystem="a", tenant_id="t", resource_id="r2", resource_type="x")
        r1.metadata["key"] = "val"
        assert "key" not in r2.metadata

    def test_asdict_round_trip(self) -> None:
        r = Resource(
            ecosystem="confluent",
            tenant_id="t-001",
            resource_id="lkc-abc",
            resource_type="kafka_cluster",
            created_at=_NOW,
        )
        d = asdict(r)
        r2 = Resource(**d)
        assert r == r2
