"""Tests for Flink identity resolution helper."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import Identity, MetricRow, Resource


class TestResolveFlinkIdentityNoMetrics:
    """Tests for resolve_flink_identity with no metrics data."""

    def test_no_metrics_data_returns_empty(self, mock_uow: MagicMock) -> None:
        """No metrics_data returns empty IdentityResolution."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert len(result.metrics_derived) == 0
        assert result.context == {}

    def test_empty_metrics_data_returns_empty(self, mock_uow: MagicMock) -> None:
        """Empty metrics_data dict returns empty IdentityResolution."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data={},
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert result.context == {}


class TestResolveFlinkIdentityMetricsNoMatch:
    """Tests for metrics that don't match the resource_id."""

    def test_metrics_for_different_pool_returns_empty(self, mock_uow: MagicMock) -> None:
        """Metrics for a different pool ID returns empty result."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=5.0,
                    labels={"compute_pool_id": "lfcp-OTHER", "flink_statement_name": "stmt-1"},
                )
            ]
        }

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert result.context == {}

    def test_zero_value_metrics_filtered_out(self, mock_uow: MagicMock) -> None:
        """Zero-value metrics rows are filtered out."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=0.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-1"},
                )
            ]
        }

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert result.context == {}


class TestResolveFlinkIdentityWithStatements:
    """Tests for metrics-driven identity resolution with statement owners."""

    def test_single_statement_owner_found(self, mock_uow: MagicMock) -> None:
        """Single statement with known owner resolves correctly."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=10.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "my-statement"},
                )
            ]
        }

        stmt_resource = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-uid-1",
            resource_type="flink_statement",
            display_name="my-statement",
            owner_id="sa-owner-1",
            metadata={"statement_name": "my-statement", "compute_pool_id": "lfcp-pool-1"},
        )
        owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-1",
            identity_type="service_account",
            display_name="Statement Owner",
        )

        mock_uow.resources.find_by_period.return_value = ([stmt_resource], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-owner-1" in result.resource_active.ids()
        assert result.context["stmt_owner_cfu"] == {"sa-owner-1": 10.0}

    def test_two_statements_different_owners(self, mock_uow: MagicMock) -> None:
        """Two statements with different owners resolve correctly."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=30.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-a"},
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=70.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-b"},
                ),
            ]
        }

        stmt_a = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-a",
            resource_type="flink_statement",
            display_name="stmt-a",
            owner_id="sa-1",
            metadata={"statement_name": "stmt-a", "compute_pool_id": "lfcp-pool-1"},
        )
        stmt_b = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-b",
            resource_type="flink_statement",
            display_name="stmt-b",
            owner_id="sa-2",
            metadata={"statement_name": "stmt-b", "compute_pool_id": "lfcp-pool-1"},
        )
        sa_1 = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )
        sa_2 = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-2",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = ([stmt_a, stmt_b], 2)
        mock_uow.identities.find_by_period.return_value = ([sa_1, sa_2], 2)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 2
        assert "sa-1" in result.resource_active.ids()
        assert "sa-2" in result.resource_active.ids()
        assert result.context["stmt_owner_cfu"] == {"sa-1": 30.0, "sa-2": 70.0}

    def test_same_owner_multiple_statements_aggregates_cfu(self, mock_uow: MagicMock) -> None:
        """Same owner for multiple statements has CFU aggregated."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=20.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-x"},
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=30.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-y"},
                ),
            ]
        }

        stmt_x = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-x",
            resource_type="flink_statement",
            display_name="stmt-x",
            owner_id="sa-same",
            metadata={"statement_name": "stmt-x", "compute_pool_id": "lfcp-pool-1"},
        )
        stmt_y = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-y",
            resource_type="flink_statement",
            display_name="stmt-y",
            owner_id="sa-same",
            metadata={"statement_name": "stmt-y", "compute_pool_id": "lfcp-pool-1"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-same",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = ([stmt_x, stmt_y], 2)
        mock_uow.identities.find_by_period.return_value = ([sa], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert result.context["stmt_owner_cfu"] == {"sa-same": 50.0}

    def test_statement_not_in_resources_uses_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """Statement from metrics not found in resources uses unknown sentinel."""
        from plugins.confluent_cloud.handlers.flink_identity import (
            FLINK_STMT_OWNER_UNKNOWN,
            resolve_flink_identity,
        )

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=15.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "missing-stmt"},
                )
            ]
        }

        mock_uow.resources.find_by_period.return_value = ([], 0)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert FLINK_STMT_OWNER_UNKNOWN in result.resource_active.ids()
        assert result.context["stmt_owner_cfu"] == {FLINK_STMT_OWNER_UNKNOWN: 15.0}

    def test_statement_with_no_owner_id_uses_unknown_sentinel(self, mock_uow: MagicMock) -> None:
        """Statement resource with no owner_id uses unknown sentinel."""
        from plugins.confluent_cloud.handlers.flink_identity import (
            FLINK_STMT_OWNER_UNKNOWN,
            resolve_flink_identity,
        )

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=5.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "orphan-stmt"},
                )
            ]
        }

        stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-orphan",
            resource_type="flink_statement",
            display_name="orphan-stmt",
            owner_id=None,
            metadata={"statement_name": "orphan-stmt", "compute_pool_id": "lfcp-pool-1"},
        )

        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert FLINK_STMT_OWNER_UNKNOWN in result.resource_active.ids()
        assert result.context["stmt_owner_cfu"] == {FLINK_STMT_OWNER_UNKNOWN: 5.0}

    def test_owner_not_in_identities_creates_sentinel_from_id(self, mock_uow: MagicMock) -> None:
        """Owner not in identities DB creates sentinel from owner_id prefix."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=8.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-z"},
                )
            ]
        }

        stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-z",
            resource_type="flink_statement",
            display_name="stmt-z",
            owner_id="sa-unknown-999",
            metadata={"statement_name": "stmt-z", "compute_pool_id": "lfcp-pool-1"},
        )

        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert "sa-unknown-999" in result.resource_active.ids()
        sentinel = result.resource_active.get("sa-unknown-999")
        assert sentinel is not None
        assert sentinel.identity_type == "service_account"
        assert result.context["stmt_owner_cfu"] == {"sa-unknown-999": 8.0}

    def test_metrics_derived_and_tenant_period_are_empty(self, mock_uow: MagicMock) -> None:
        """metrics_derived and tenant_period remain empty when resolution works."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=10.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-1"},
                )
            ]
        }

        stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-1",
            resource_type="flink_statement",
            display_name="stmt-1",
            owner_id="sa-1",
            metadata={"statement_name": "stmt-1", "compute_pool_id": "lfcp-pool-1"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([sa], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.tenant_period) == 0
        assert len(result.metrics_derived) == 0
        assert len(result.resource_active) == 1

    def test_multiple_metric_rows_for_same_statement_aggregate(self, mock_uow: MagicMock) -> None:
        """Multiple metric rows for the same statement aggregate CFU values."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        metrics = {
            "flink_cfu_primary": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, 0, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=5.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-1"},
                ),
                MetricRow(
                    timestamp=datetime(2026, 2, 1, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=3.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-1"},
                ),
            ]
        }

        stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-1",
            resource_type="flink_statement",
            display_name="stmt-1",
            owner_id="sa-1",
            metadata={"statement_name": "stmt-1", "compute_pool_id": "lfcp-pool-1"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([sa], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert result.context["stmt_owner_cfu"] == {"sa-1": 8.0}


class TestFlinkFallbackFromRunningStatements:
    """Tests for no-metrics fallback: query running statements from resource DB."""

    def test_no_metrics_running_statements_attributed_to_owners(self, mock_uow: MagicMock) -> None:
        """No metrics, running statements in DB → owners appear in resource_active with weight 1.0."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        running_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-uid-run1",
            resource_type="flink_statement",
            display_name="running-stmt",
            owner_id="sa-owner-1",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": False},
        )
        owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-1",
            identity_type="service_account",
            display_name="Owner 1",
        )

        mock_uow.resources.find_by_period.return_value = ([running_stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-owner-1" in result.resource_active.ids()
        assert "stmt_owner_cfu" in result.context
        assert result.context["stmt_owner_cfu"] == {"sa-owner-1": 1.0}

    def test_stopped_statements_excluded_only_running_owner_returned(self, mock_uow: MagicMock) -> None:
        """COMPLETED/FAILED/STOPPED statements excluded; only RUNNING statement owner appears."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        running_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-run",
            resource_type="flink_statement",
            display_name="running-stmt",
            owner_id="sa-running",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": False},
        )
        stopped_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-stop",
            resource_type="flink_statement",
            display_name="stopped-stmt",
            owner_id="sa-stopped",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": True},
        )
        completed_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-done",
            resource_type="flink_statement",
            display_name="completed-stmt",
            owner_id="sa-completed",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": True},
        )
        failed_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-fail",
            resource_type="flink_statement",
            display_name="failed-stmt",
            owner_id="sa-failed",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": True},
        )
        running_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-running",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = (
            [running_stmt, stopped_stmt, completed_stmt, failed_stmt],
            4,
        )
        mock_uow.identities.find_by_period.return_value = ([running_owner], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-running" in result.resource_active.ids()
        assert result.context["stmt_owner_cfu"] == {"sa-running": 1.0}

    def test_no_metrics_no_running_statements_returns_empty_and_queries_db(self, mock_uow: MagicMock) -> None:
        """No metrics, no running statements → empty result; secondary DB lookup still attempted."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        mock_uow.resources.find_by_period.return_value = ([], 0)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert "stmt_owner_cfu" not in result.context
        # Secondary path must query the resource DB (not short-circuit before looking)
        mock_uow.resources.find_by_period.assert_called_once()

    def test_no_metrics_statements_for_different_pool_returns_empty(self, mock_uow: MagicMock) -> None:
        """No metrics; DB has running statements but for a different compute pool → empty result."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        other_pool_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-other",
            resource_type="flink_statement",
            display_name="other-stmt",
            owner_id="sa-other",
            metadata={"compute_pool_id": "lfcp-OTHER", "is_stopped": False},
        )

        mock_uow.resources.find_by_period.return_value = ([other_pool_stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert "stmt_owner_cfu" not in result.context
        mock_uow.resources.find_by_period.assert_called_once()

    def test_fallback_unknown_owner_creates_sentinel(self, mock_uow: MagicMock) -> None:
        """Running statement with owner_id not in identities table creates sentinel."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-uid",
            resource_type="flink_statement",
            display_name="my-stmt",
            owner_id="sa-unknown-xyz",
            metadata={"compute_pool_id": "lfcp-pool-1", "is_stopped": False},
        )
        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)  # No matching identity

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert "sa-unknown-xyz" in result.resource_active.ids()
        sentinel = result.resource_active.get("sa-unknown-xyz")
        assert sentinel.identity_type == "service_account"  # inferred from "sa-" prefix
        assert result.context["stmt_owner_cfu"] == {"sa-unknown-xyz": 1.0}


class TestFallbackFromRunningStatementsDirect:
    """Direct unit tests for _fallback_from_running_statements using is_stopped key."""

    def test_fallback_stopped_statement_excluded(self, mock_uow: MagicMock) -> None:
        """Stopped statement (is_stopped=True) must be excluded; result is empty."""
        from plugins.confluent_cloud.handlers.flink_identity import _fallback_from_running_statements

        stopped_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-stopped",
            resource_type="flink_statement",
            display_name="stopped-stmt",
            owner_id="sa-stopped",
            metadata={"compute_pool_id": "pool-1", "is_stopped": True},
        )
        mock_uow.resources.find_by_period.return_value = ([stopped_stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        owner_weight, identity_set = _fallback_from_running_statements(
            compute_pool_id="pool-1",
            tenant_id="org-123",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert identity_set.ids() == set()
        assert owner_weight == {}

    def test_fallback_running_statement_included(self, mock_uow: MagicMock) -> None:
        """Running statement (is_stopped=False) must appear in the returned IdentitySet."""
        from plugins.confluent_cloud.handlers.flink_identity import _fallback_from_running_statements

        running_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-running",
            resource_type="flink_statement",
            display_name="running-stmt",
            owner_id="sa-active",
            metadata={"compute_pool_id": "pool-1", "is_stopped": False},
        )
        owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-active",
            identity_type="service_account",
            display_name="Active Owner",
        )
        mock_uow.resources.find_by_period.return_value = ([running_stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        owner_weight, identity_set = _fallback_from_running_statements(
            compute_pool_id="pool-1",
            tenant_id="org-123",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert "sa-active" in identity_set.ids()
        assert owner_weight == {"sa-active": 1.0}

    def test_fallback_mixed_statements_only_running_included(self, mock_uow: MagicMock) -> None:
        """2 running + 3 stopped statements: only 2 running owners appear in result."""
        from plugins.confluent_cloud.handlers.flink_identity import _fallback_from_running_statements

        stmts = [
            Resource(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                resource_id=f"stmt-run-{i}",
                resource_type="flink_statement",
                display_name=f"running-{i}",
                owner_id=f"sa-run-{i}",
                metadata={"compute_pool_id": "pool-1", "is_stopped": False},
            )
            for i in range(2)
        ] + [
            Resource(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                resource_id=f"stmt-stop-{i}",
                resource_type="flink_statement",
                display_name=f"stopped-{i}",
                owner_id=f"sa-stop-{i}",
                metadata={"compute_pool_id": "pool-1", "is_stopped": True},
            )
            for i in range(3)
        ]
        running_owners = [
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=f"sa-run-{i}",
                identity_type="service_account",
            )
            for i in range(2)
        ]
        mock_uow.resources.find_by_period.return_value = (stmts, 5)
        mock_uow.identities.find_by_period.return_value = (running_owners, 2)

        owner_weight, identity_set = _fallback_from_running_statements(
            compute_pool_id="pool-1",
            tenant_id="org-123",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(owner_weight) == 2
        assert "sa-run-0" in owner_weight
        assert "sa-run-1" in owner_weight
        assert not any(f"sa-stop-{i}" in owner_weight for i in range(3))
        assert identity_set.ids() == {"sa-run-0", "sa-run-1"}

    def test_fallback_regression_via_resolve_flink_identity_stopped_excluded(self, mock_uow: MagicMock) -> None:
        """Regression: resolve_flink_identity with metrics_data=None must not count stopped statements."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        running_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-run",
            resource_type="flink_statement",
            display_name="running",
            owner_id="sa-active",
            metadata={"compute_pool_id": "pool-reg", "is_stopped": False},
        )
        stopped_stmt = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="stmt-stop",
            resource_type="flink_statement",
            display_name="stopped",
            owner_id="sa-ghost",
            metadata={"compute_pool_id": "pool-reg", "is_stopped": True},
        )
        active_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-active",
            identity_type="service_account",
        )
        mock_uow.resources.find_by_period.return_value = ([running_stmt, stopped_stmt], 2)
        mock_uow.identities.find_by_period.return_value = ([active_owner], 1)

        result = resolve_flink_identity(
            tenant_id="org-123",
            resource_id="pool-reg",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        stmt_owner_cfu = result.context.get("stmt_owner_cfu", {})
        assert "sa-ghost" not in stmt_owner_cfu
        assert "sa-active" in stmt_owner_cfu
