from __future__ import annotations

import pytest
from pydantic import ValidationError

_METRICS_STUB = {"type": "prometheus", "url": "http://prom:9090"}


def _make_base_config(**overrides) -> dict:
    base = {
        "ccloud_api": {"key": "k", "secret": "s"},
    }
    base.update(overrides)
    return base


class TestTopicAttributionConfigDefaults:
    def test_topic_attribution_disabled_by_default(self) -> None:
        """topic_attribution.enabled=False by default — no attribution loop."""
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(_make_base_config())
        assert config.topic_attribution.enabled is False

    def test_default_exclude_patterns_include_consumer_offsets(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(_make_base_config())
        patterns = config.topic_attribution.exclude_topic_patterns
        assert "__consumer_offsets" in patterns

    def test_default_missing_metrics_behavior_is_even_split(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(_make_base_config())
        assert config.topic_attribution.missing_metrics_behavior == "even_split"

    def test_default_retention_days_is_90(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(_make_base_config())
        assert config.topic_attribution.retention_days == 90


class TestTopicAttributionConfigEnabled:
    def test_can_enable_topic_attribution(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(topic_attribution={"enabled": True}, metrics=_METRICS_STUB)
        )
        assert config.topic_attribution.enabled is True

    def test_missing_metrics_behavior_skip(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(
                topic_attribution={
                    "enabled": True,
                    "missing_metrics_behavior": "skip",
                },
                metrics=_METRICS_STUB,
            )
        )
        assert config.topic_attribution.missing_metrics_behavior == "skip"

    def test_missing_metrics_behavior_invalid_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(
                    topic_attribution={
                        "enabled": True,
                        "missing_metrics_behavior": "invalid_mode",
                    },
                    metrics=_METRICS_STUB,
                )
            )


class TestTopicAttributionConfigCostMappingOverrides:
    def test_valid_cost_mapping_override(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(
                topic_attribution={
                    "enabled": True,
                    "cost_mapping_overrides": {
                        "KAFKA_PARTITION": "even_split",
                        "KAFKA_BASE": "bytes_ratio",
                    },
                },
                metrics=_METRICS_STUB,
            )
        )
        overrides = config.topic_attribution.cost_mapping_overrides
        assert overrides["KAFKA_PARTITION"] == "even_split"
        assert overrides["KAFKA_BASE"] == "bytes_ratio"

    def test_invalid_cost_mapping_method_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(
                    topic_attribution={
                        "enabled": True,
                        "cost_mapping_overrides": {"KAFKA_BASE": "bogus_method"},
                    },
                    metrics=_METRICS_STUB,
                )
            )

    def test_disabled_cost_mapping(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(
                topic_attribution={
                    "enabled": True,
                    "cost_mapping_overrides": {"KAFKA_BASE": "disabled"},
                },
                metrics=_METRICS_STUB,
            )
        )
        assert config.topic_attribution.cost_mapping_overrides["KAFKA_BASE"] == "disabled"


class TestTopicAttributionConfigMetricNameOverrides:
    def test_valid_metric_override(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(
                topic_attribution={
                    "enabled": True,
                    "metric_name_overrides": {
                        "topic_bytes_in": "custom_received_bytes",
                    },
                },
                metrics=_METRICS_STUB,
            )
        )
        assert config.topic_attribution.metric_name_overrides["topic_bytes_in"] == "custom_received_bytes"

    def test_invalid_metric_key_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(
                    topic_attribution={
                        "enabled": True,
                        "metric_name_overrides": {"invalid_key": "some_metric"},
                    },
                    metrics=_METRICS_STUB,
                )
            )

    def test_empty_metric_name_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(
                    topic_attribution={
                        "enabled": True,
                        "metric_name_overrides": {"topic_bytes_in": ""},
                    },
                    metrics=_METRICS_STUB,
                )
            )


class TestTopicAttributionConfigRetention:
    def test_custom_retention_days(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        config = CCloudPluginConfig.from_plugin_settings(
            _make_base_config(topic_attribution={"enabled": True, "retention_days": 30}, metrics=_METRICS_STUB)
        )
        assert config.topic_attribution.retention_days == 30

    def test_zero_retention_days_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(topic_attribution={"enabled": True, "retention_days": 0}, metrics=_METRICS_STUB)
            )

    def test_retention_days_over_365_raises(self) -> None:
        from plugins.confluent_cloud.config import CCloudPluginConfig

        with pytest.raises(ValidationError):
            CCloudPluginConfig.from_plugin_settings(
                _make_base_config(topic_attribution={"enabled": True, "retention_days": 400}, metrics=_METRICS_STUB)
            )


class TestBuildDiscoveryQueries:
    def test_defaults_return_three_queries(self) -> None:
        """build_discovery_queries({}) returns 3 queries with default Confluent metric names."""
        from plugins.confluent_cloud.overlays.topic_attribution import build_discovery_queries

        queries = build_discovery_queries({})
        assert len(queries) == 3
        expressions = [q.query_expression for q in queries]
        assert any("confluent_kafka_server_received_bytes" in e for e in expressions)
        assert any("confluent_kafka_server_sent_bytes" in e for e in expressions)
        assert any("confluent_kafka_server_retained_bytes" in e for e in expressions)

    def test_overrides_applied_to_discovery(self) -> None:
        """Overrides change the metric name in discovery query expressions."""
        from plugins.confluent_cloud.overlays.topic_attribution import build_discovery_queries

        queries = build_discovery_queries({"topic_bytes_in": "my_custom_metric"})
        by_key = {q.key: q for q in queries}
        assert "my_custom_metric" in by_key["disc_bytes_in"].query_expression

    def test_discovery_and_attribution_same_expressions(self) -> None:
        """Discovery and attribution resolve the same PromQL expressions for given overrides."""
        from core.engine.topic_attribution import build_metric_queries
        from plugins.confluent_cloud.overlays.topic_attribution import build_discovery_queries

        overrides = {"topic_bytes_in": "custom_received", "topic_retained_bytes": "custom_retained"}
        attr_queries = build_metric_queries(overrides)
        disc_queries = build_discovery_queries(overrides)
        attr_exprs = sorted(q.query_expression for q in attr_queries)
        disc_exprs = sorted(q.query_expression for q in disc_queries)
        assert attr_exprs == disc_exprs

    def test_all_keys_have_disc_prefix(self) -> None:
        """All discovery query keys start with disc_."""
        from plugins.confluent_cloud.overlays.topic_attribution import build_discovery_queries

        queries = build_discovery_queries({})
        keys = {q.key for q in queries}
        assert keys == {"disc_bytes_in", "disc_bytes_out", "disc_retained"}

    def test_all_queries_have_topic_label(self) -> None:
        """All discovery queries have 'topic' in label_keys and kafka_id as resource_label."""
        from plugins.confluent_cloud.overlays.topic_attribution import build_discovery_queries

        for query in build_discovery_queries({}):
            assert "topic" in query.label_keys
            assert query.resource_label == "kafka_id"

    def test_old_constants_removed(self) -> None:
        """Old hardcoded constants no longer importable."""
        with pytest.raises(ImportError):
            from plugins.confluent_cloud.overlays.topic_attribution import _DISCOVERY_QUERIES  # noqa: F401
        with pytest.raises(ImportError):
            from plugins.confluent_cloud.overlays.topic_attribution import _DISC_BYTES_IN  # noqa: F401

    def test_build_metric_queries_is_public(self) -> None:
        """build_metric_queries (no underscore) is importable from core.engine.topic_attribution."""
        from core.engine.topic_attribution import build_metric_queries  # noqa: F401

        assert callable(build_metric_queries)
