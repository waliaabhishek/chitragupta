"""Tests for task-049: discovery cache on SelfManagedKafkaPlugin.

Verifies that run_combined_discovery() is called exactly once across
initialize() + build_shared_context() by caching the result in
plugin._cached_discovery and clearing it after the first gather cycle.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from core.metrics.protocol import MetricsQueryError


def _make_row(key: str, labels: dict) -> object:
    from core.models import MetricRow

    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=1.0,
        labels=labels,
    )


def _base_settings(identity_source: str = "prometheus", resource_source: str = "prometheus") -> dict:
    settings: dict = {
        "cluster_id": "kafka-cache-test",
        "broker_count": 2,
        "cost_model": {
            "compute_hourly_rate": "0.10",
            "storage_per_gib_hourly": "0.0001",
            "network_ingress_per_gib": "0.01",
            "network_egress_per_gib": "0.02",
        },
        "identity_source": {"source": identity_source},
        "metrics": {"url": "http://prom:9090"},
    }
    if resource_source == "admin_api":
        settings["resource_source"] = {"source": "admin_api", "bootstrap_servers": "kafka:9092"}
    else:
        settings["resource_source"] = {"source": "prometheus"}
    return settings


_DISCOVERY_RESULT = (
    frozenset({"0", "1"}),
    frozenset({"orders", "payments"}),
    frozenset({"User:alice"}),
)


# ---------------------------------------------------------------------------
# Test 1: Cache hit eliminates duplicate call
# ---------------------------------------------------------------------------


class TestCacheHitEliminatesDuplicateCall:
    def test_run_combined_discovery_called_exactly_once_across_init_and_build(self) -> None:
        """initialize() caches discovery result; build_shared_context() reuses it — only one query total."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with patch(
            "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
            return_value=_DISCOVERY_RESULT,
        ) as mock_discovery:
            with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
                plugin.initialize(_base_settings(identity_source="prometheus"))

            # build_shared_context must NOT call run_combined_discovery again
            plugin.build_shared_context("tenant-1")

            mock_discovery.assert_called_once()


# ---------------------------------------------------------------------------
# Test 2: Cache cleared after first use
# ---------------------------------------------------------------------------


class TestCacheClearedAfterFirstUse:
    def test_cached_discovery_is_none_after_first_build_shared_context(self) -> None:
        """_cached_discovery is cleared to None after the first build_shared_context() call."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with patch(
            "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
            return_value=_DISCOVERY_RESULT,
        ):
            with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
                plugin.initialize(_base_settings(identity_source="prometheus"))

            plugin.build_shared_context("tenant-1")

        # Cache must be cleared after first gather cycle
        assert plugin._cached_discovery is None

    def test_second_build_shared_context_re_queries_after_cache_cleared(self) -> None:
        """After cache is cleared, second build_shared_context() triggers a new query (cache miss)."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with patch(
            "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
            return_value=_DISCOVERY_RESULT,
        ) as mock_discovery:
            with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
                plugin.initialize(_base_settings(identity_source="prometheus"))

            plugin.build_shared_context("tenant-1")
            # First build used cache → total calls = 1 (from init)

            plugin.build_shared_context("tenant-1")
            # Second build must re-query since cache is cleared → total calls = 2

        assert mock_discovery.call_count == 2


# ---------------------------------------------------------------------------
# Test 3: Cache not set on MetricsQueryError
# ---------------------------------------------------------------------------


class TestCacheNotSetOnMetricsQueryError:
    def test_cached_discovery_none_when_validation_raises_metrics_query_error(self) -> None:
        """_cached_discovery stays None when run_combined_discovery raises MetricsQueryError during init."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with (
            patch(
                "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
                side_effect=MetricsQueryError("Prometheus unreachable"),
            ),
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
        ):
            plugin.initialize(_base_settings(identity_source="prometheus"))

        assert plugin._cached_discovery is None

    def test_prometheus_principals_available_false_when_validation_raises(self) -> None:
        """_prometheus_principals_available is False when MetricsQueryError during validation."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with (
            patch(
                "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
                side_effect=MetricsQueryError("timeout"),
            ),
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
        ):
            plugin.initialize(_base_settings(identity_source="prometheus"))

        assert plugin._prometheus_principals_available is False
        assert plugin._cached_discovery is None


# ---------------------------------------------------------------------------
# Test 4: No cache when identity_source=static
# ---------------------------------------------------------------------------


class TestNoCacheForStaticIdentitySource:
    def test_cached_discovery_is_none_when_identity_source_static(self) -> None:
        """identity_source=static skips _validate_principal_label; _cached_discovery must be None."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with (
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
            patch(
                "plugins.self_managed_kafka.gathering.admin_api.create_admin_client",
                return_value=MagicMock(),
            ),
        ):
            plugin.initialize(_base_settings(identity_source="static", resource_source="admin_api"))

        assert plugin._cached_discovery is None

    def test_build_shared_context_does_not_error_with_static_identity(self) -> None:
        """build_shared_context() with static identity returns context without discovery sets."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with (
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
            patch(
                "plugins.self_managed_kafka.gathering.admin_api.create_admin_client",
                return_value=MagicMock(),
            ),
        ):
            plugin.initialize(_base_settings(identity_source="static", resource_source="admin_api"))

        result = plugin.build_shared_context("tenant-1")

        assert isinstance(result, SMKSharedContext)
        assert result.discovered_brokers is None
        assert result.discovered_topics is None
        assert result.discovered_principals is None


# ---------------------------------------------------------------------------
# Test 5: SMKSharedContext populated correctly from cache
# ---------------------------------------------------------------------------


class TestContextPopulatedCorrectlyFromCache:
    def test_context_uses_data_from_cached_discovery_not_re_query(self) -> None:
        """Context discovery sets come from the cached init result, not a fresh re-query.

        The mock returns different data on first vs second call.
        Without caching: context uses second-call data (wrong).
        With caching: context uses first-call (init) data (correct).
        """
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        init_result = (
            frozenset({"broker-A"}),
            frozenset({"topic-init"}),
            frozenset({"User:init-alice"}),
        )
        second_call_result = (
            frozenset({"broker-B"}),
            frozenset({"topic-second"}),
            frozenset({"User:second-bob"}),
        )

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with patch(
            "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
            side_effect=[init_result, second_call_result],
        ):
            with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
                plugin.initialize(_base_settings(identity_source="prometheus"))

            # build_shared_context must return data from the cached (init) result
            ctx = plugin.build_shared_context("tenant-1")

        # If cache is used: matches init_result
        # If no cache (re-queries): matches second_call_result → assertion fails
        assert ctx.discovered_brokers == frozenset({"broker-A"})
        assert ctx.discovered_topics == frozenset({"topic-init"})
        assert ctx.discovered_principals == frozenset({"User:init-alice"})

    def test_all_three_discovery_fields_populated_from_cache(self) -> None:
        """All three discovery frozensets in context match the cached discovery tuple."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        expected_brokers = frozenset({"0", "1", "2"})
        expected_topics = frozenset({"orders", "events", "payments"})
        expected_principals = frozenset({"User:alice", "User:bob", "User:carol"})

        plugin = SelfManagedKafkaPlugin()
        mock_ms = MagicMock()

        with patch(
            "plugins.self_managed_kafka.gathering.prometheus.run_combined_discovery",
            return_value=(expected_brokers, expected_topics, expected_principals),
        ):
            with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
                plugin.initialize(_base_settings(identity_source="prometheus"))

            ctx = plugin.build_shared_context("tenant-1")

        assert ctx.discovered_brokers == expected_brokers
        assert ctx.discovered_topics == expected_topics
        assert ctx.discovered_principals == expected_principals
