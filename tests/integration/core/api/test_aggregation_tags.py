from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.models.identity import CoreIdentity
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

_BASE_URL = "/api/v1/tenants/test-tenant/chargebacks/aggregate"
_BASE_PARAMS: dict[str, str] = {
    "time_bucket": "day",
    "start_date": "2026-02-01",
    "end_date": "2026-02-28",
}


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_identity(backend: SQLModelBackend, identity_id: str) -> None:
    with backend.create_unit_of_work() as uow:
        uow.identities.upsert(
            CoreIdentity(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                identity_id=identity_id,
                identity_type="user",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_resource(backend: SQLModelBackend, resource_id: str) -> None:
    with backend.create_unit_of_work() as uow:
        uow.resources.upsert(
            CoreResource(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                resource_id=resource_id,
                resource_type="kafka_cluster",
                status=ResourceStatus.ACTIVE,
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                metadata={},
            )
        )
        uow.commit()


def _seed_chargeback(
    backend: SQLModelBackend,
    identity_id: str,
    resource_id: str | None,
    product_type: str,
    amount: Decimal,
    hour: int = 0,
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
            ChargebackRow(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                timestamp=datetime(2026, 2, 15, hour, tzinfo=UTC),
                resource_id=resource_id,
                product_category="compute",
                product_type=product_type,
                identity_id=identity_id,
                cost_type=CostType.USAGE,
                amount=amount,
                allocation_method="direct",
                allocation_detail=None,
                tags=[],
                metadata={},
            )
        )
        uow.commit()


def _add_identity_tag(backend: SQLModelBackend, identity_id: str, tag_key: str, tag_value: str) -> None:
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag("test-tenant", "identity", identity_id, tag_key, tag_value, "test")
        uow.commit()


def _add_resource_tag(backend: SQLModelBackend, resource_id: str, tag_key: str, tag_value: str) -> None:
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag("test-tenant", "resource", resource_id, tag_key, tag_value, "test")
        uow.commit()


def _seed_standard_fixtures(backend: SQLModelBackend) -> None:
    """
    Seed identities, resources, chargebacks, and tags used across most tag-aggregate tests.

    Layout:
        user-alice / res-a / kafka   / $10  identity: owner=alice, department=eng
        user-bob   / res-b / connect / $20  identity: owner=bob, department=ops
                                            resource:  team=platform
        user-bob   / res-c / kafka   / $15  resource:  team=commerce
        user-none  / res-d / connect / $30  (no tags)
    """
    for identity_id in ("user-alice", "user-bob", "user-none"):
        _seed_identity(backend, identity_id)
    for resource_id in ("res-a", "res-b", "res-c", "res-d"):
        _seed_resource(backend, resource_id)

    _seed_chargeback(backend, "user-alice", "res-a", "kafka", Decimal("10.00"), hour=0)
    _seed_chargeback(backend, "user-bob", "res-b", "connect", Decimal("20.00"), hour=1)
    _seed_chargeback(backend, "user-bob", "res-c", "kafka", Decimal("15.00"), hour=2)
    _seed_chargeback(backend, "user-none", "res-d", "connect", Decimal("30.00"), hour=3)

    _add_identity_tag(backend, "user-alice", "owner", "alice")
    _add_identity_tag(backend, "user-alice", "department", "eng")
    _add_identity_tag(backend, "user-bob", "owner", "bob")
    _add_identity_tag(backend, "user-bob", "department", "ops")
    _add_resource_tag(backend, "res-b", "team", "platform")
    _add_resource_tag(backend, "res-c", "team", "commerce")


# ---------------------------------------------------------------------------
# Tests 1-5: Tag GROUP BY
# ---------------------------------------------------------------------------


class TestAggregateTagGroupBy:
    """Tests 1-5: tag:{key} in group_by parameter."""

    def test_aggregate_group_by_tag_owner_returns_tag_dimension_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 1: group_by=tag:owner → buckets have 'tag:owner' dimension key; UNTAGGED bucket present."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) > 0
        owner_values = {b["dimensions"]["tag:owner"] for b in buckets}
        assert "alice" in owner_values
        assert "bob" in owner_values
        assert "UNTAGGED" in owner_values
        for b in buckets:
            assert "tag:owner" in b["dimensions"]

    def test_aggregate_group_by_two_tag_keys_produces_tuple_buckets(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 2: group_by=tag:owner&group_by=tag:department → each bucket has both tag keys."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params=[
                *_BASE_PARAMS.items(),
                ("group_by", "tag:owner"),
                ("group_by", "tag:department"),
            ],
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) > 0
        for b in buckets:
            assert "tag:owner" in b["dimensions"]
            assert "tag:department" in b["dimensions"]
        # alice/eng bucket must exist
        alice_eng = next(
            (
                b
                for b in buckets
                if b["dimensions"]["tag:owner"] == "alice" and b["dimensions"]["tag:department"] == "eng"
            ),
            None,
        )
        assert alice_eng is not None
        assert Decimal(alice_eng["total_amount"]) == Decimal("10.00")

    def test_aggregate_group_by_tag_and_dimension_composes(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 3: group_by=tag:owner&group_by=product_type → both tag and dimension keys present."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params=[
                *_BASE_PARAMS.items(),
                ("group_by", "tag:owner"),
                ("group_by", "product_type"),
            ],
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) > 0
        for b in buckets:
            assert "tag:owner" in b["dimensions"]
            assert "product_type" in b["dimensions"]
        # alice/kafka bucket
        alice_kafka = next(
            (
                b
                for b in buckets
                if b["dimensions"]["tag:owner"] == "alice" and b["dimensions"]["product_type"] == "kafka"
            ),
            None,
        )
        assert alice_kafka is not None

    def test_aggregate_group_by_nonexistent_tag_returns_single_untagged_bucket(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 4: group_by=tag:nonexistent → single UNTAGGED bucket with full cost."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:nonexistent"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 1
        assert buckets[0]["dimensions"]["tag:nonexistent"] == "UNTAGGED"
        assert Decimal(buckets[0]["total_amount"]) == Decimal("75.00")

    def test_aggregate_resource_tag_overrides_identity_tag_on_same_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 5: resource tag wins over identity tag when both have same key."""
        _seed_identity(in_memory_backend, "user-override")
        _seed_resource(in_memory_backend, "res-override")
        _seed_chargeback(in_memory_backend, "user-override", "res-override", "kafka", Decimal("50.00"), hour=0)
        _add_identity_tag(in_memory_backend, "user-override", "owner", "from-identity")
        _add_resource_tag(in_memory_backend, "res-override", "owner", "from-resource")

        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        owner_values = {b["dimensions"]["tag:owner"] for b in buckets}
        assert "from-resource" in owner_values
        assert "from-identity" not in owner_values


# ---------------------------------------------------------------------------
# Tests 6-9: Tag WHERE filtering
# ---------------------------------------------------------------------------


class TestAggregateTagFilter:
    """Tests 6-9: tag:{key}={value} filter params."""

    def test_aggregate_tag_filter_includes_only_matching_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 6: tag:department=eng → only alice's row; untagged rows absent."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "identity_id", "tag:department": "eng"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")
        identity_ids = {b["dimensions"]["identity_id"] for b in data["buckets"]}
        assert identity_ids == {"user-alice"}

    def test_aggregate_tag_filter_comma_values_are_intra_tag_or(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 7: tag:team=platform,commerce → rows where team IN (platform, commerce)."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "resource_id", "tag:team": "platform,commerce"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 2
        assert Decimal(data["total_amount"]) == Decimal("35.00")
        resource_ids = {b["dimensions"]["resource_id"] for b in data["buckets"]}
        assert "res-b" in resource_ids
        assert "res-c" in resource_ids

    def test_aggregate_multiple_tag_filters_are_anded(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 8: tag:owner=alice&tag:department=eng → both conditions must hold (AND)."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "identity_id", "tag:owner": "alice", "tag:department": "eng"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")
        identity_ids = {b["dimensions"]["identity_id"] for b in data["buckets"]}
        assert identity_ids == {"user-alice"}

    def test_aggregate_tag_filter_combined_with_tag_group_by(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 9: tag:department=eng&group_by=tag:owner → filtered to eng, then grouped by owner."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner", "tag:department": "eng"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 1
        assert buckets[0]["dimensions"]["tag:owner"] == "alice"
        assert Decimal(buckets[0]["total_amount"]) == Decimal("10.00")


# ---------------------------------------------------------------------------
# Tests 10-13: Validation
# ---------------------------------------------------------------------------


class TestAggregateTagValidation:
    """Tests 10-13: tag key validation and backward compatibility."""

    def test_aggregate_group_by_tag_empty_key_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 10: group_by=tag: (empty key) → 400 Invalid tag key format."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:"},
        )
        assert response.status_code == 400
        assert "Invalid tag key format" in response.json()["detail"]

    def test_aggregate_group_by_tag_and_invalid_dimension_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 11: group_by=tag:owner&group_by=bad_col → 400 for bad_col; tag:owner is not flagged."""
        response = app_with_backend.get(
            _BASE_URL,
            params=[
                *_BASE_PARAMS.items(),
                ("group_by", "tag:owner"),
                ("group_by", "bad_col"),
            ],
        )
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert "bad_col" in detail

    def test_aggregate_invalid_tag_filter_key_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 12: tag:!bad!=value → 400 Invalid tag key format."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "identity_id", "tag:!bad!": "value"},
        )
        assert response.status_code == 400
        assert "Invalid tag key format" in response.json()["detail"]

    def test_aggregate_no_tag_params_existing_behavior_unchanged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 13: no tag params → standard identity_id grouping works as before."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "identity_id"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 4
        identity_ids = {b["dimensions"]["identity_id"] for b in data["buckets"]}
        assert "user-alice" in identity_ids
        assert "user-bob" in identity_ids
        assert "user-none" in identity_ids

    def test_aggregate_group_by_empty_string_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 15: group_by= (empty string) → 400; empty group_by values are rejected."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": ""},
        )
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Test 14: Join deduplication
# ---------------------------------------------------------------------------


class TestAggregateTagJoinDeduplication:
    """Test 14: shared join when same tag key appears in both group_by and filter."""

    def test_aggregate_tag_in_group_by_and_filter_uses_single_join(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 14: group_by=tag:owner&tag:owner=alice → no SQL error; only alice bucket returned."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner", "tag:owner": "alice"},
        )
        # Must not 500 (no duplicate alias error from SQLAlchemy)
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 1
        assert buckets[0]["dimensions"]["tag:owner"] == "alice"
        assert Decimal(buckets[0]["total_amount"]) == Decimal("10.00")
