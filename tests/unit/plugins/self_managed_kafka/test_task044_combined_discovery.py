"""TASK-044: Consolidate Prometheus discovery queries tests.

Tests for run_combined_discovery(), brokers_to_resources(), topics_to_resources(),
principals_to_identities(), SMKSharedContext discovery fields, build_shared_context()
integration, and handler cached-context gather cycle.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from core.models import MetricRow


def make_row(key: str, labels: dict) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=1.0,
        labels=labels,
    )


@pytest.fixture
def mock_metrics_source() -> MagicMock:
    return MagicMock()


@pytest.fixture
def base_identity_config():
    from plugins.self_managed_kafka.config import IdentitySourceConfig

    return IdentitySourceConfig.model_validate(
        {
            "source": "prometheus",
            "principal_to_team": {"User:alice": "team-data"},
            "default_team": "UNASSIGNED",
        }
    )


def _base_plugin_settings(resource_source: str = "prometheus", identity_source: str = "prometheus") -> dict:
    settings: dict = {
        "cluster_id": "kafka-001",
        "broker_count": 3,
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


def _make_smk_ctx_with_discovery(
    cluster_id: str = "kafka-001",
    discovered_brokers: frozenset[str] | None = None,
    discovered_topics: frozenset[str] | None = None,
    discovered_principals: frozenset[str] | None = None,
):
    """Create SMKSharedContext with discovery sets pre-populated."""
    from core.models import CoreResource, ResourceStatus
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

    cluster = CoreResource(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        resource_id=cluster_id,
        resource_type="cluster",
        status=ResourceStatus.ACTIVE,
        metadata={},
    )
    return SMKSharedContext(
        cluster_resource=cluster,
        discovered_brokers=discovered_brokers,
        discovered_topics=discovered_topics,
        discovered_principals=discovered_principals,
    )


# ---------------------------------------------------------------------------
# Test 1: run_combined_discovery() with all three labels present
# ---------------------------------------------------------------------------


class TestRunCombinedDiscoveryAllLabels:
    def test_splits_into_three_frozensets(self, mock_metrics_source: MagicMock) -> None:
        """run_combined_discovery returns three frozensets split from combined metric rows."""
        from datetime import timedelta

        from plugins.self_managed_kafka.gathering.prometheus import run_combined_discovery

        # Combined query returns rows where each has broker, topic, and principal labels
        mock_metrics_source.query.return_value = {
            "combined_discovery": [
                make_row("combined_discovery", {"broker": "0", "topic": "orders", "principal": "User:alice"}),
                make_row("combined_discovery", {"broker": "1", "topic": "payments", "principal": "User:bob"}),
                make_row("combined_discovery", {"broker": "2", "topic": "orders", "principal": "User:alice"}),
            ]
        }

        brokers, topics, principals = run_combined_discovery(mock_metrics_source, timedelta(hours=1))

        assert isinstance(brokers, frozenset)
        assert isinstance(topics, frozenset)
        assert isinstance(principals, frozenset)
        assert brokers == frozenset({"0", "1", "2"})
        assert topics == frozenset({"orders", "payments"})
        assert principals == frozenset({"User:alice", "User:bob"})

    def test_query_called_exactly_once(self, mock_metrics_source: MagicMock) -> None:
        """run_combined_discovery issues exactly one query call."""
        from datetime import timedelta

        from plugins.self_managed_kafka.gathering.prometheus import run_combined_discovery

        mock_metrics_source.query.return_value = {"combined_discovery": []}

        run_combined_discovery(mock_metrics_source, timedelta(hours=1))

        mock_metrics_source.query.assert_called_once()


# ---------------------------------------------------------------------------
# Test 2: run_combined_discovery() with sparse rows
# ---------------------------------------------------------------------------


class TestRunCombinedDiscoverySparseRows:
    def test_sparse_rows_include_only_present_non_empty_values(self, mock_metrics_source: MagicMock) -> None:
        """Sparse rows: missing labels not included; no empty-string entries."""
        from datetime import timedelta

        from plugins.self_managed_kafka.gathering.prometheus import run_combined_discovery

        mock_metrics_source.query.return_value = {
            "combined_discovery": [
                # has broker and topic, no principal
                make_row("combined_discovery", {"broker": "0", "topic": "orders"}),
                # has topic and principal, no broker
                make_row("combined_discovery", {"topic": "payments", "principal": "User:carol"}),
                # has only broker
                make_row("combined_discovery", {"broker": "1"}),
                # has empty string for broker (should be excluded)
                make_row("combined_discovery", {"broker": "", "topic": "events"}),
            ]
        }

        brokers, topics, principals = run_combined_discovery(mock_metrics_source, timedelta(hours=1))

        assert "" not in brokers
        assert brokers == frozenset({"0", "1"})
        assert topics == frozenset({"orders", "payments", "events"})
        assert principals == frozenset({"User:carol"})


# ---------------------------------------------------------------------------
# Test 3: build_shared_context() with prometheus resource+identity sources
# ---------------------------------------------------------------------------


class TestBuildSharedContextPrometheusSource:
    def test_discovery_fields_non_none_when_prometheus(self) -> None:
        """build_shared_context with prometheus sources populates all three discovery fields."""

        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        settings = _base_plugin_settings(resource_source="prometheus", identity_source="prometheus")
        plugin = SelfManagedKafkaPlugin()

        mock_ms = MagicMock()
        mock_ms.query.return_value = {
            "combined_discovery": [
                make_row("combined_discovery", {"broker": "0", "topic": "orders", "principal": "User:alice"}),
            ]
        }

        with (
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
            patch.object(SelfManagedKafkaPlugin, "_validate_principal_label"),
        ):
            plugin.initialize(settings)

        mock_ms.query.reset_mock()
        mock_ms.query.return_value = {
            "combined_discovery": [
                make_row("combined_discovery", {"broker": "0", "topic": "orders", "principal": "User:alice"}),
            ]
        }

        result = plugin.build_shared_context("tenant-1")

        assert isinstance(result, SMKSharedContext)
        assert result.discovered_brokers == frozenset({"0"})
        assert result.discovered_topics == frozenset({"orders"})
        assert result.discovered_principals == frozenset({"User:alice"})

    def test_query_called_exactly_once_in_build_shared_context(self) -> None:
        """build_shared_context calls MetricsSource.query exactly once (combined query)."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        settings = _base_plugin_settings(resource_source="prometheus", identity_source="prometheus")
        plugin = SelfManagedKafkaPlugin()

        mock_ms = MagicMock()
        mock_ms.query.return_value = {"combined_discovery": []}

        with (
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
            patch.object(SelfManagedKafkaPlugin, "_validate_principal_label"),
        ):
            plugin.initialize(settings)

        mock_ms.query.reset_mock()
        mock_ms.query.return_value = {"combined_discovery": []}

        plugin.build_shared_context("tenant-1")

        mock_ms.query.assert_called_once()


# ---------------------------------------------------------------------------
# Test 4: build_shared_context() with admin_api resource + static identity
# ---------------------------------------------------------------------------


class TestBuildSharedContextNonPrometheusSource:
    def test_discovery_fields_none_when_admin_api_and_static(self) -> None:
        """build_shared_context with admin_api+static sources: discovery fields are None."""
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        settings = _base_plugin_settings(resource_source="admin_api", identity_source="static")
        plugin = SelfManagedKafkaPlugin()

        mock_ms = MagicMock()

        with (
            patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms),
            patch("plugins.self_managed_kafka.gathering.admin_api.create_admin_client", return_value=MagicMock()),
        ):
            plugin.initialize(settings)

        mock_ms.query.reset_mock()

        result = plugin.build_shared_context("tenant-1")

        assert isinstance(result, SMKSharedContext)
        assert result.discovered_brokers is None
        assert result.discovered_topics is None
        assert result.discovered_principals is None
        mock_ms.query.assert_not_called()


# ---------------------------------------------------------------------------
# Test 5: handler.gather_resources() uses cached SMKSharedContext discovery sets
# ---------------------------------------------------------------------------


class TestHandlerGatherResourcesUsesCachedSets:
    def test_yields_broker_resources_from_cached_sets(self, mock_metrics_source: MagicMock) -> None:
        """gather_resources() yields broker Resources from pre-populated context; no Prometheus call."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        ctx = _make_smk_ctx_with_discovery(
            discovered_brokers=frozenset({"0", "1"}),
            discovered_topics=frozenset({"orders"}),
            discovered_principals=frozenset({"User:alice"}),
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))

        resource_types = [r.resource_type for r in resources]
        assert "cluster" in resource_types
        assert "broker" in resource_types
        assert "topic" in resource_types

        broker_ids = {r.resource_id for r in resources if r.resource_type == "broker"}
        assert "kafka-001:broker:0" in broker_ids
        assert "kafka-001:broker:1" in broker_ids

        topic_ids = {r.resource_id for r in resources if r.resource_type == "topic"}
        assert "kafka-001:topic:orders" in topic_ids

        # No additional MetricsSource call — sets came from cache
        mock_metrics_source.query.assert_not_called()


# ---------------------------------------------------------------------------
# Test 6: handler.gather_identities() reads _current_gather_ctx
# ---------------------------------------------------------------------------


class TestHandlerGatherIdentitiesReadsCachedContext:
    def test_yields_identities_from_cached_principals(self, mock_metrics_source: MagicMock) -> None:
        """gather_identities() after gather_resources() uses _current_gather_ctx; no Prometheus call."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {
                    "source": "prometheus",
                    "principal_to_team": {"User:alice": "team-data", "User:bob": "team-analytics"},
                    "default_team": "UNASSIGNED",
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        ctx = _make_smk_ctx_with_discovery(
            discovered_brokers=frozenset({"0"}),
            discovered_topics=frozenset({"orders"}),
            discovered_principals=frozenset({"User:alice", "User:bob"}),
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        uow = MagicMock()

        # Simulate gather_resources() having stored _current_gather_ctx
        list(handler.gather_resources("tenant-1", uow, ctx))
        mock_metrics_source.query.reset_mock()

        identities = list(handler.gather_identities("tenant-1", uow))

        identity_ids = {i.identity_id for i in identities}
        assert "User:alice" in identity_ids
        assert "User:bob" in identity_ids

        # No additional Prometheus call — principals came from cached context
        mock_metrics_source.query.assert_not_called()


# ---------------------------------------------------------------------------
# Test 7: handler.gather_identities() without prior gather_resources()
# ---------------------------------------------------------------------------


class TestHandlerGatherIdentitiesWithoutPriorGatherResources:
    def test_returns_empty_without_error_when_no_ctx(self, mock_metrics_source: MagicMock) -> None:
        """gather_identities() with _current_gather_ctx=None returns empty without error."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {"source": "prometheus"},
                "metrics": {"url": "http://prom:9090"},
            }
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        uow = MagicMock()

        # No gather_resources() called first — _current_gather_ctx is None
        identities = list(handler.gather_identities("tenant-1", uow))

        assert identities == []


# ---------------------------------------------------------------------------
# Test 8: End-to-end gather cycle produces same Resources and Identities
# ---------------------------------------------------------------------------


class TestEndToEndGatherCycleConsolidated:
    def test_full_gather_cycle_produces_expected_resources_and_identities(self, mock_metrics_source: MagicMock) -> None:
        """Full gather cycle: same Resource and Identity objects with consolidated query."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = SelfManagedKafkaConfig.from_plugin_settings(
            {
                "cluster_id": "kafka-001",
                "broker_count": 3,
                "cost_model": {
                    "compute_hourly_rate": "0.10",
                    "storage_per_gib_hourly": "0.0001",
                    "network_ingress_per_gib": "0.01",
                    "network_egress_per_gib": "0.02",
                },
                "identity_source": {
                    "source": "prometheus",
                    "principal_to_team": {"User:alice": "team-data"},
                    "default_team": "UNASSIGNED",
                },
                "metrics": {"url": "http://prom:9090"},
            }
        )

        # Context with pre-populated discovery sets (from combined query)
        ctx = _make_smk_ctx_with_discovery(
            discovered_brokers=frozenset({"0", "1", "2"}),
            discovered_topics=frozenset({"orders", "payments"}),
            discovered_principals=frozenset({"User:alice"}),
        )

        handler = SelfManagedKafkaHandler(config, mock_metrics_source)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))
        identities = list(handler.gather_identities("tenant-1", uow))

        # Resources: 1 cluster + 3 brokers + 2 topics = 6
        resource_types = [r.resource_type for r in resources]
        assert resource_types.count("cluster") == 1
        assert resource_types.count("broker") == 3
        assert resource_types.count("topic") == 2

        # Identities: 1 for alice
        assert len(identities) == 1
        assert identities[0].identity_id == "User:alice"

        # Zero Prometheus calls during both gather phases
        mock_metrics_source.query.assert_not_called()


# ---------------------------------------------------------------------------
# Test 9: Integration — plugin.build_shared_context() → handler gather cycle
# ---------------------------------------------------------------------------


class TestPluginHandlerIntegration:
    def test_plugin_context_flows_through_handler_gather_cycle(self) -> None:
        """Integration: plugin.build_shared_context() output drives handler gather_resources/gather_identities."""
        from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        settings = _base_plugin_settings(resource_source="prometheus", identity_source="prometheus")
        settings["identity_source"] = {
            "source": "prometheus",
            "principal_to_team": {"User:alice": "team-data"},
            "default_team": "UNASSIGNED",
        }

        mock_ms = MagicMock()
        combined_rows = [
            make_row("combined_discovery", {"broker": "0", "topic": "orders", "principal": "User:alice"}),
            make_row("combined_discovery", {"broker": "1", "topic": "payments", "principal": "User:alice"}),
        ]
        mock_ms.query.return_value = {"combined_discovery": combined_rows}

        plugin = SelfManagedKafkaPlugin()
        with patch("plugins.self_managed_kafka.plugin.create_metrics_source", return_value=mock_ms):
            plugin.initialize(settings)

        # Reset call count — combined_discovery was called once during _validate_principal_label
        mock_ms.query.reset_mock()
        mock_ms.query.return_value = {"combined_discovery": combined_rows}

        # Phase 1: build shared context (consumes cached result from _validate_principal_label, no new query)
        ctx = plugin.build_shared_context("tenant-1")

        # Phase 2: handler gather cycle using real plugin output
        config = SelfManagedKafkaConfig.from_plugin_settings(settings)
        handler = SelfManagedKafkaHandler(config, mock_ms)
        uow = MagicMock()

        resources = list(handler.gather_resources("tenant-1", uow, ctx))
        identities = list(handler.gather_identities("tenant-1", uow))

        resource_types = [r.resource_type for r in resources]
        assert "cluster" in resource_types
        assert "broker" in resource_types
        assert "topic" in resource_types

        identity_ids = {i.identity_id for i in identities}
        assert "User:alice" in identity_ids

        # build_shared_context consumed cached result from _validate_principal_label — no new query
        # handler gather_resources/gather_identities also make no combined_discovery calls
        mock_ms.query.assert_not_called()


# ---------------------------------------------------------------------------
# Test 10: run_combined_discovery() uses custom discovery_window_hours
# ---------------------------------------------------------------------------


class TestRunCombinedDiscoveryWindow:
    def test_uses_custom_discovery_window_hours(self, mock_metrics_source: MagicMock) -> None:
        """run_combined_discovery with discovery_window_hours=6 passes start=now-6h to query."""
        from datetime import timedelta
        from unittest.mock import patch

        from plugins.self_managed_kafka.gathering.prometheus import run_combined_discovery

        mock_metrics_source.query.return_value = {"combined_discovery": []}

        # Freeze time for predictable assertions
        with patch("plugins.self_managed_kafka.gathering.prometheus.datetime") as mock_dt:
            mock_now = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
            mock_dt.now.return_value = mock_now

            run_combined_discovery(mock_metrics_source, timedelta(hours=1), discovery_window_hours=6)

        call_kwargs = mock_metrics_source.query.call_args[1]
        expected_start = datetime(2026, 3, 1, 6, 0, 0, tzinfo=UTC)  # 12:00 - 6 hours
        assert call_kwargs["start"] == expected_start
        assert call_kwargs["end"] == mock_now
