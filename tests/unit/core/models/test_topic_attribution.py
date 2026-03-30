from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.models.topic_attribution import (
    TopicAttributionAggregationBucket,
    TopicAttributionAggregationResult,
    TopicAttributionRow,
)


class TestTopicAttributionRow:
    def _make_row(self, **overrides) -> TopicAttributionRow:
        defaults = dict(
            ecosystem="eco",
            tenant_id="t1",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            env_id="env-1",
            cluster_resource_id="lkc-abc123",
            topic_name="orders-events",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            attribution_method="bytes_ratio",
            amount=Decimal("10.00"),
        )
        defaults.update(overrides)
        return TopicAttributionRow(**defaults)

    def test_topic_attribution_row_instantiation(self) -> None:
        row = self._make_row()
        assert row.ecosystem == "eco"
        assert row.tenant_id == "t1"
        assert row.cluster_resource_id == "lkc-abc123"
        assert row.topic_name == "orders-events"
        assert row.product_category == "KAFKA"
        assert row.product_type == "KAFKA_NETWORK_WRITE"
        assert row.attribution_method == "bytes_ratio"
        assert row.amount == Decimal("10.00")

    def test_topic_attribution_row_defaults(self) -> None:
        row = self._make_row()
        assert row.metadata == {}
        assert row.dimension_id is None

    def test_topic_attribution_row_with_metadata(self) -> None:
        row = self._make_row(metadata={"chain_tier": 0})
        assert row.metadata["chain_tier"] == 0

    def test_topic_attribution_row_with_dimension_id(self) -> None:
        row = self._make_row(dimension_id=42)
        assert row.dimension_id == 42


class TestTopicAttributionAggregationBucket:
    def test_bucket_instantiation(self) -> None:
        bucket = TopicAttributionAggregationBucket(
            dimensions={"topic_name": "orders-events"},
            time_bucket="2026-01-01",
            total_amount=Decimal("10.00"),
            row_count=2,
        )
        assert bucket.dimensions["topic_name"] == "orders-events"
        assert bucket.time_bucket == "2026-01-01"
        assert bucket.total_amount == Decimal("10.00")
        assert bucket.row_count == 2


class TestTopicAttributionAggregationResult:
    def test_result_instantiation(self) -> None:
        bucket = TopicAttributionAggregationBucket(
            dimensions={"topic_name": "a"},
            time_bucket="2026-01-01",
            total_amount=Decimal("5.00"),
            row_count=1,
        )
        result = TopicAttributionAggregationResult(
            buckets=[bucket],
            total_amount=Decimal("5.00"),
            total_rows=1,
        )
        assert len(result.buckets) == 1
        assert result.total_amount == Decimal("5.00")
        assert result.total_rows == 1
