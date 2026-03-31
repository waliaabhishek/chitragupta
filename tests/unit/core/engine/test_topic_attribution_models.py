from __future__ import annotations

import inspect
from datetime import UTC, datetime
from decimal import Decimal

import core.engine.helpers as helpers
import core.engine.topic_attribution_models as tam
from core.engine.topic_attribution_models import (
    TopicAttributionContext,
    TopicChainModel,
    TopicEvenSplitModel,
    TopicMissingMetricsFallbackModel,
    TopicUsageRatioModel,
)


def _make_config(missing_metrics_behavior: str = "even_split", exclude_topic_patterns: list[str] | None = None):
    from plugins.confluent_cloud.config import TopicAttributionConfig

    return TopicAttributionConfig(
        enabled=True,
        missing_metrics_behavior=missing_metrics_behavior,
        exclude_topic_patterns=exclude_topic_patterns or ["__consumer_offsets", "_schemas", "_confluent-*"],
    )


def _make_ctx(**overrides) -> TopicAttributionContext:
    defaults = dict(
        ecosystem="eco",
        tenant_id="t1",
        env_id="env-1",
        cluster_resource_id="lkc-abc",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        cluster_cost=Decimal("10.00"),
        topics=frozenset(["a", "b"]),
        topic_metrics={"topic_bytes_in": {"a": 80.0, "b": 20.0}},
        config=_make_config(),
    )
    defaults.update(overrides)
    return TopicAttributionContext(**defaults)


class TestTopicUsageRatioModel:
    def test_topic_usage_ratio_basic_split(self) -> None:
        """topic_metrics={"topic_bytes_in": {"a": 80.0, "b": 20.0}}, cluster_cost=10.00 → a=8.00, b=2.00, sum=10.00."""
        ctx = _make_ctx()
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        rows = model.attribute(ctx)

        assert rows is not None
        assert len(rows) == 2
        by_topic = {r.topic_name: r.amount for r in rows}
        assert by_topic["a"] == Decimal("8.00")
        assert by_topic["b"] == Decimal("2.00")
        assert sum(by_topic.values()) == Decimal("10.00")

    def test_topic_usage_ratio_returns_none_when_all_zero(self) -> None:
        """All metric values zero → returns None to signal chain fallback."""
        ctx = _make_ctx(topic_metrics={"topic_bytes_in": {"a": 0.0, "b": 0.0}})
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        result = model.attribute(ctx)
        assert result is None

    def test_topic_usage_ratio_returns_none_when_no_metric_data(self) -> None:
        ctx = _make_ctx(topic_metrics={})
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        result = model.attribute(ctx)
        assert result is None

    def test_topic_usage_ratio_amount_sum_equals_cluster_cost(self) -> None:
        ctx = _make_ctx(
            topics=frozenset(["x", "y", "z"]),
            topic_metrics={"topic_bytes_in": {"x": 10.0, "y": 30.0, "z": 60.0}},
            cluster_cost=Decimal("7.00"),
        )
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        rows = model.attribute(ctx)
        assert rows is not None
        total = sum(r.amount for r in rows)
        assert total == Decimal("7.00")


class TestTopicEvenSplitModel:
    def test_even_split_two_topics(self) -> None:
        ctx = _make_ctx(topics=frozenset(["a", "b"]), cluster_cost=Decimal("10.00"))
        model = TopicEvenSplitModel()
        rows = model.attribute(ctx)
        assert rows is not None
        assert len(rows) == 2
        by_topic = {r.topic_name: r.amount for r in rows}
        assert by_topic["a"] == Decimal("5.00")
        assert by_topic["b"] == Decimal("5.00")

    def test_even_split_returns_none_when_no_topics(self) -> None:
        ctx = _make_ctx(topics=frozenset())
        model = TopicEvenSplitModel()
        result = model.attribute(ctx)
        assert result is None

    def test_even_split_attribution_method(self) -> None:
        ctx = _make_ctx()
        model = TopicEvenSplitModel()
        rows = model.attribute(ctx)
        assert rows is not None
        assert all(r.attribution_method == "even_split" for r in rows)


class TestTopicMissingMetricsFallbackModel:
    def test_fallback_even_split(self) -> None:
        """Missing metrics + even_split behavior → 2 topics at $5.00 each."""
        ctx = _make_ctx(
            topic_metrics={"topic_bytes_in": {"a": 0.0, "b": 0.0}},
            config=_make_config(missing_metrics_behavior="even_split"),
        )
        model = TopicMissingMetricsFallbackModel()
        rows = model.attribute(ctx)
        assert len(rows) == 2
        by_topic = {r.topic_name: r.amount for r in rows}
        assert by_topic["a"] == Decimal("5.00")
        assert by_topic["b"] == Decimal("5.00")

    def test_fallback_skip(self) -> None:
        """Missing metrics + skip behavior → 0 rows."""
        ctx = _make_ctx(
            topic_metrics={"topic_bytes_in": {"a": 0.0, "b": 0.0}},
            config=_make_config(missing_metrics_behavior="skip"),
        )
        model = TopicMissingMetricsFallbackModel()
        rows = model.attribute(ctx)
        assert rows == []


class TestTopicChainModel:
    def test_chain_uses_first_successful_model(self) -> None:
        """Chain tries models in order, returns first non-None result."""
        ctx = _make_ctx()
        ratio_model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        fallback = TopicMissingMetricsFallbackModel()
        chain = TopicChainModel(models=[ratio_model, fallback])
        rows = chain.attribute(ctx)
        assert rows is not None
        assert len(rows) == 2
        # First model succeeds — rows should have bytes_ratio attribution
        assert all(r.attribution_method == "bytes_ratio" for r in rows)

    def test_chain_fallback_when_first_model_returns_none(self) -> None:
        """All metric values zero → ratio model returns None → fallback fires."""
        ctx = _make_ctx(
            topic_metrics={"topic_bytes_in": {"a": 0.0, "b": 0.0}},
            config=_make_config(missing_metrics_behavior="even_split"),
        )
        ratio_model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        fallback = TopicMissingMetricsFallbackModel()
        chain = TopicChainModel(models=[ratio_model, fallback])
        rows = chain.attribute(ctx)
        assert len(rows) == 2
        by_topic = {r.topic_name: r.amount for r in rows}
        assert by_topic["a"] == Decimal("5.00")
        assert by_topic["b"] == Decimal("5.00")

    def test_chain_fallback_skip(self) -> None:
        """All metrics zero + skip → 0 rows."""
        ctx = _make_ctx(
            topic_metrics={"topic_bytes_in": {"a": 0.0, "b": 0.0}},
            config=_make_config(missing_metrics_behavior="skip"),
        )
        ratio_model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        fallback = TopicMissingMetricsFallbackModel()
        chain = TopicChainModel(models=[ratio_model, fallback])
        rows = chain.attribute(ctx)
        assert rows == []

    def test_chain_sets_chain_tier_metadata(self) -> None:
        ctx = _make_ctx()
        ratio_model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        fallback = TopicMissingMetricsFallbackModel()
        chain = TopicChainModel(models=[ratio_model, fallback])
        rows = chain.attribute(ctx)
        assert rows is not None
        assert all(r.metadata.get("chain_tier") == 0 for r in rows)


class TestHelpersSymbolImport:
    def test_distribute_remainder_is_helpers_version(self) -> None:
        """After fix, tam._distribute_remainder must be the same object as helpers._distribute_remainder."""
        assert tam._distribute_remainder is helpers._distribute_remainder

    def test_cent_is_helpers_version(self) -> None:
        """After fix, tam._CENT must be the same object as helpers._CENT."""
        assert tam._CENT is helpers._CENT

    def test_distribute_remainder_source_file_is_helpers(self) -> None:
        """_distribute_remainder as seen from topic_attribution_models must originate in helpers.py."""
        source_file = inspect.getsourcefile(tam._distribute_remainder)
        assert source_file is not None
        assert source_file.endswith("helpers.py")


class TestDeadCodeRemoval:
    def test_no_metrics_available_field_on_context(self) -> None:
        """metrics_available must not exist as a field on TopicAttributionContext after removal."""
        assert "metrics_available" not in TopicAttributionContext.__dataclass_fields__

    def test_usage_ratio_no_metrics_available_guard(self) -> None:
        """Empty topic_metrics → cluster_total == 0.0 path returns None, not a removed guard."""
        ctx = _make_ctx(topic_metrics={})
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        result = model.attribute(ctx)
        assert result is None


class TestTopicFilter:
    def test_consumer_offsets_excluded_by_default(self) -> None:
        """__consumer_offsets excluded at _get_cluster_topics level; not in topics frozenset."""
        # ctx.topics reflects post-filter state — __consumer_offsets already removed
        ctx = _make_ctx(
            topics=frozenset(["orders", "payments"]),
            topic_metrics={"topic_bytes_in": {"orders": 60.0, "payments": 40.0}},
            cluster_cost=Decimal("10.00"),
            config=_make_config(exclude_topic_patterns=["__consumer_offsets"]),
        )
        model = TopicUsageRatioModel(metric_keys=("topic_bytes_in",))
        rows = model.attribute(ctx)
        assert rows is not None
        topic_names = {r.topic_name for r in rows}
        assert "__consumer_offsets" not in topic_names
        total = sum(r.amount for r in rows)
        assert total == Decimal("10.00")
