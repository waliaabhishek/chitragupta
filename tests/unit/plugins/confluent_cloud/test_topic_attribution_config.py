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


class TestTopicAttributionDiscoveryQueries:
    def test_discovery_queries_defined(self) -> None:
        """_DISCOVERY_QUERIES has 3 MetricQuery instances with correct keys."""
        from plugins.confluent_cloud.overlays.topic_attribution import (
            _DISC_BYTES_IN,
            _DISC_BYTES_OUT,
            _DISC_RETAINED,
            _DISCOVERY_QUERIES,
        )

        assert len(_DISCOVERY_QUERIES) == 3
        assert _DISCOVERY_QUERIES[0] is _DISC_BYTES_IN
        assert _DISCOVERY_QUERIES[1] is _DISC_BYTES_OUT
        assert _DISCOVERY_QUERIES[2] is _DISC_RETAINED

    def test_discovery_query_keys_are_unique(self) -> None:
        from plugins.confluent_cloud.overlays.topic_attribution import _DISCOVERY_QUERIES

        keys = [q.key for q in _DISCOVERY_QUERIES]
        assert len(keys) == len(set(keys))

    def test_discovery_queries_have_topic_label(self) -> None:
        from plugins.confluent_cloud.overlays.topic_attribution import _DISCOVERY_QUERIES

        for query in _DISCOVERY_QUERIES:
            assert "topic" in query.label_keys
            assert query.resource_label == "kafka_id"
