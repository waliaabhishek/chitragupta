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
            "confluent_flink_num_cfu": [
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
            "confluent_flink_num_cfu": [
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
            "confluent_flink_num_cfu": [
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

        mock_uow.resources.find_by_period.return_value = [stmt_resource]
        mock_uow.identities.find_by_period.return_value = [owner]

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "stmt-a"},
        )
        stmt_b = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-b",
            resource_type="flink_statement",
            display_name="stmt-b",
            owner_id="sa-2",
            metadata={"statement_name": "stmt-b"},
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

        mock_uow.resources.find_by_period.return_value = [stmt_a, stmt_b]
        mock_uow.identities.find_by_period.return_value = [sa_1, sa_2]

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "stmt-x"},
        )
        stmt_y = Resource(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            resource_id="uid-y",
            resource_type="flink_statement",
            display_name="stmt-y",
            owner_id="sa-same",
            metadata={"statement_name": "stmt-y"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-same",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = [stmt_x, stmt_y]
        mock_uow.identities.find_by_period.return_value = [sa]

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
            "confluent_flink_num_cfu": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="confluent_flink_num_cfu",
                    value=15.0,
                    labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "missing-stmt"},
                )
            ]
        }

        mock_uow.resources.find_by_period.return_value = []
        mock_uow.identities.find_by_period.return_value = []

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "orphan-stmt"},
        )

        mock_uow.resources.find_by_period.return_value = [stmt]
        mock_uow.identities.find_by_period.return_value = []

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "stmt-z"},
        )

        mock_uow.resources.find_by_period.return_value = [stmt]
        mock_uow.identities.find_by_period.return_value = []

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "stmt-1"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = [stmt]
        mock_uow.identities.find_by_period.return_value = [sa]

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
            "confluent_flink_num_cfu": [
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
            metadata={"statement_name": "stmt-1"},
        )
        sa = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )

        mock_uow.resources.find_by_period.return_value = [stmt]
        mock_uow.identities.find_by_period.return_value = [sa]

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
