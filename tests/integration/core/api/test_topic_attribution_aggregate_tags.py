from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.topic_attribution import TopicAttributionRow
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

_BASE_URL = "/api/v1/tenants/test-tenant/topic-attributions/aggregate"
_BASE_PARAMS: dict[str, str] = {
    "time_bucket": "day",
    "start_date": "2026-02-01",
    "end_date": "2026-02-28",
}

_CLUSTER_ID = "lkc-test"
_ECOSYSTEM = "test-eco"
_TENANT_ID = "test-tenant"
_ENV_ID = "env-test"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _make_ta_row(
    topic_name: str,
    amount: Decimal,
    hour: int = 0,
    cluster_resource_id: str = _CLUSTER_ID,
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=_ECOSYSTEM,
        tenant_id=_TENANT_ID,
        timestamp=datetime(2026, 2, 15, hour, tzinfo=UTC),
        env_id=_ENV_ID,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        attribution_method="bytes_ratio",
        amount=amount,
    )


def _seed_ta_rows(backend: SQLModelBackend, rows: list[TopicAttributionRow]) -> None:
    with backend.create_unit_of_work() as uow:
        uow.topic_attributions.upsert_batch(rows)
        uow.commit()


def _add_resource_tag(
    backend: SQLModelBackend,
    cluster_resource_id: str,
    topic_name: str,
    tag_key: str,
    tag_value: str,
) -> None:
    entity_id = f"{cluster_resource_id}:topic:{topic_name}"
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag(_TENANT_ID, "resource", entity_id, tag_key, tag_value, "test")
        uow.commit()


def _seed_standard_fixtures(backend: SQLModelBackend) -> None:
    """
    Seed topic attribution rows and tags used across most tag-aggregate tests.

    Layout:
        payments-events  / $10  → owner=alice, department=eng
        orders-events    / $20  → owner=bob, department=ops
        metrics-events   / $30  → team=platform
        logs-events      / $15  → team=commerce
        untagged-events  / $25  → (no tags)

    Total: $100
    """
    rows = [
        _make_ta_row("payments-events", Decimal("10.00"), hour=0),
        _make_ta_row("orders-events", Decimal("20.00"), hour=1),
        _make_ta_row("metrics-events", Decimal("30.00"), hour=2),
        _make_ta_row("logs-events", Decimal("15.00"), hour=3),
        _make_ta_row("untagged-events", Decimal("25.00"), hour=4),
    ]
    _seed_ta_rows(backend, rows)

    _add_resource_tag(backend, _CLUSTER_ID, "payments-events", "owner", "alice")
    _add_resource_tag(backend, _CLUSTER_ID, "payments-events", "department", "eng")
    _add_resource_tag(backend, _CLUSTER_ID, "orders-events", "owner", "bob")
    _add_resource_tag(backend, _CLUSTER_ID, "orders-events", "department", "ops")
    _add_resource_tag(backend, _CLUSTER_ID, "metrics-events", "team", "platform")
    _add_resource_tag(backend, _CLUSTER_ID, "logs-events", "team", "commerce")


# ---------------------------------------------------------------------------
# Tests 1-5: Tag GROUP BY
# ---------------------------------------------------------------------------


class TestTopicAttributionAggregateTagGroupBy:
    """Tests 1-5: tag:{key} in group_by parameter."""

    def test_aggregate_group_by_tag_owner_returns_tag_dimension_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 1: group_by=tag:owner → buckets have 'tag:owner' dimension key; UNTAGGED present."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) > 0
        for b in buckets:
            assert "tag:owner" in b["dimensions"]
        owner_values = {b["dimensions"]["tag:owner"] for b in buckets}
        assert "alice" in owner_values
        assert "bob" in owner_values
        assert "UNTAGGED" in owner_values

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
        """Test 3: group_by=tag:owner&group_by=topic_name → both tag and dimension keys present."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params=[
                *_BASE_PARAMS.items(),
                ("group_by", "tag:owner"),
                ("group_by", "topic_name"),
            ],
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) > 0
        for b in buckets:
            assert "tag:owner" in b["dimensions"]
            assert "topic_name" in b["dimensions"]
        alice_payments = next(
            (
                b
                for b in buckets
                if b["dimensions"]["tag:owner"] == "alice" and b["dimensions"]["topic_name"] == "payments-events"
            ),
            None,
        )
        assert alice_payments is not None
        assert Decimal(alice_payments["total_amount"]) == Decimal("10.00")

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
        assert Decimal(buckets[0]["total_amount"]) == Decimal("100.00")

    def test_aggregate_group_by_tag_with_no_entity_tags_rows_returns_untagged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 5: no entity_tags → single UNTAGGED bucket."""
        rows = [
            _make_ta_row("topic-a", Decimal("20.00"), hour=0),
            _make_ta_row("topic-b", Decimal("30.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        # No tags seeded — entity_tags table is empty for these resources
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 1
        assert buckets[0]["dimensions"]["tag:owner"] == "UNTAGGED"
        assert Decimal(buckets[0]["total_amount"]) == Decimal("50.00")


# ---------------------------------------------------------------------------
# Tests 6-9: Tag WHERE filtering
# ---------------------------------------------------------------------------


class TestTopicAttributionAggregateTagFilter:
    """Tests 6-9: tag:{key}={value} filter params."""

    def test_aggregate_tag_filter_includes_only_matching_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 6: tag:department=eng → only payments-events; untagged rows absent."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "topic_name", "tag:department": "eng"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")
        topic_names = {b["dimensions"]["topic_name"] for b in data["buckets"]}
        assert topic_names == {"payments-events"}

    def test_aggregate_tag_filter_comma_values_are_intra_tag_or(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 7: tag:team=platform,commerce → rows where team IN (platform, commerce)."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "topic_name", "tag:team": "platform,commerce"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 2
        assert Decimal(data["total_amount"]) == Decimal("45.00")
        topic_names = {b["dimensions"]["topic_name"] for b in data["buckets"]}
        assert "metrics-events" in topic_names
        assert "logs-events" in topic_names

    def test_aggregate_multiple_tag_filters_are_anded(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 8: tag:owner=alice&tag:department=eng → AND: both conditions must hold per row."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "topic_name", "tag:owner": "alice", "tag:department": "eng"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")
        topic_names = {b["dimensions"]["topic_name"] for b in data["buckets"]}
        assert topic_names == {"payments-events"}

    def test_aggregate_tag_filter_combined_with_tag_group_by(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 9: tag:department=eng&group_by=tag:owner → filtered to dept=eng, grouped by owner within that set."""
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
# Tests 10-14: Validation
# ---------------------------------------------------------------------------


class TestTopicAttributionAggregateTagValidation:
    """Tests 10-14: tag key validation and backward compatibility."""

    def test_aggregate_group_by_tag_empty_key_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 10: group_by=tag: (empty key) → 400 Invalid tag key format."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:"},
        )
        assert response.status_code == 400
        assert "Invalid tag key format" in response.json()["detail"]

    def test_aggregate_group_by_tag_invalid_key_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 11: group_by=tag:bad!key → 400 Invalid tag key format in group_by."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:bad!key"},
        )
        assert response.status_code == 400
        assert "Invalid tag key format" in response.json()["detail"]

    def test_aggregate_invalid_tag_filter_key_returns_400(self, app_with_backend: TestClient) -> None:
        """Test 12: tag:!bad!=value → 400 Invalid tag key format."""
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "tag:!bad!": "value"},
        )
        assert response.status_code == 400
        assert "Invalid tag key format" in response.json()["detail"]

    def test_aggregate_group_by_nonexistent_dim_is_silently_dropped(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 13: nonexistent_dim silently dropped; 200 (existing behavior)."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "nonexistent_dim"},
        )
        assert response.status_code == 200
        data = response.json()
        for b in data["buckets"]:
            assert "nonexistent_dim" not in b["dimensions"]

    def test_aggregate_no_tag_params_existing_behavior_unchanged(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 14: no tag params → standard topic_name grouping works unchanged; no extra joins emitted."""
        _seed_standard_fixtures(in_memory_backend)
        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "topic_name"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 5
        topic_names = {b["dimensions"]["topic_name"] for b in data["buckets"]}
        assert "payments-events" in topic_names
        assert "orders-events" in topic_names
        assert "untagged-events" in topic_names


# ---------------------------------------------------------------------------
# Tests 15-16: Join construction
# ---------------------------------------------------------------------------


class TestTopicAttributionAggregateTagJoinConstruction:
    """Tests 15-16: join deduplication and resource-only join verification."""

    def test_aggregate_tag_in_group_by_and_filter_uses_single_join(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 15: tag:owner in both group_by and filter → dedup, no 500."""
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

    def test_aggregate_resource_tags_resolve_correctly_without_identity_join(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Test 16: only resource-type entity tags are joined (identity_id_col=None → no identity join in SQL)."""
        rows = [_make_ta_row("tagged-topic", Decimal("77.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "tagged-topic", "owner", "resource-owner")

        response = app_with_backend.get(
            _BASE_URL,
            params={**_BASE_PARAMS, "group_by": "tag:owner"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        # resource-owner tag value must appear from the resource entity join
        owner_values = {b["dimensions"]["tag:owner"] for b in buckets}
        assert "resource-owner" in owner_values
        # Only one tagged bucket (the tagged topic) + one UNTAGGED is unexpected here since
        # only one topic is seeded and it IS tagged → exactly one bucket with the tag value
        assert len(buckets) == 1
        assert Decimal(buckets[0]["total_amount"]) == Decimal("77.00")
