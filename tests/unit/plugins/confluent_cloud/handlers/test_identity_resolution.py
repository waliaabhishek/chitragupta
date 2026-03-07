"""Tests for Kafka/SR identity resolution helper."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import CoreIdentity, MetricRow


class TestResolveKafkaSrIdentities:
    """Tests for resolve_kafka_sr_identities function."""

    def test_with_api_keys_resolves_owners(self, mock_uow: MagicMock) -> None:
        """API key scoped to cluster with sa- owner resolves to owner in resource_active."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        api_key = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner",
            identity_type="service_account",
            display_name="My SA",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.identities.find_by_period.return_value = ([api_key, sa_owner], 2)

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-owner" in result.resource_active.ids()

    def test_extracts_metrics_principals(self, mock_uow: MagicMock) -> None:
        """Principal IDs from metrics are added to metrics_derived."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        mock_uow.identities.find_by_period.return_value = ([], 0)
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="bytes_in",
                    value=1000.0,
                    labels={"kafka_id": "lkc-abc", "principal_id": "sa-metrics-user"},
                ),
            ],
        }

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=metrics_data,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.metrics_derived) == 1
        # Sentinel identity created for unknown principal
        assert "sa-metrics-user" in result.metrics_derived.ids()

    def test_api_key_for_different_cluster_not_included(self, mock_uow: MagicMock) -> None:
        """API key scoped to different cluster is not included."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        api_key = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lkc-OTHER", "owner_id": "sa-owner"},
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.identities.find_by_period.return_value = ([api_key], 1)

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0

    def test_api_key_without_owner_id_skipped(self, mock_uow: MagicMock) -> None:
        """API key without owner_id is skipped."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        api_key = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc"},  # No owner_id
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_uow.identities.find_by_period.return_value = ([api_key], 1)

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0

    def test_tenant_period_is_empty(self, mock_uow: MagicMock) -> None:
        """tenant_period is returned empty (orchestrator fills it)."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        mock_uow.identities.find_by_period.return_value = ([], 0)

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.tenant_period) == 0

    def test_multiple_api_keys_same_owner_deduped(self, mock_uow: MagicMock) -> None:
        """Multiple API keys with same owner result in single entry."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            resolve_kafka_sr_identities,
        )

        api_key_1 = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-1",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner"},
        )
        api_key_2 = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-2",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner"},
        )
        sa_owner = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner",
            identity_type="service_account",
        )
        mock_uow.identities.find_by_period.return_value = (
            [api_key_1, api_key_2, sa_owner],
            3,
        )

        result = resolve_kafka_sr_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_start=datetime(2026, 2, 1, tzinfo=UTC),
            billing_end=datetime(2026, 2, 2, tzinfo=UTC),
            metrics_data=None,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-owner" in result.resource_active.ids()


class TestCreateSentinel:
    """Tests for create_sentinel_from_id function."""

    def test_sa_prefix_service_account(self) -> None:
        """sa- prefix creates service_account type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("sa-123", "org", "confluent_cloud")
        assert result.identity_type == "service_account"
        assert result.identity_id == "sa-123"
        assert result.ecosystem == "confluent_cloud"
        assert result.tenant_id == "org"

    def test_u_prefix_user(self) -> None:
        """u- prefix creates user type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("u-456", "org", "confluent_cloud")
        assert result.identity_type == "user"

    def test_pool_prefix_identity_pool(self) -> None:
        """pool- prefix creates identity_pool type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("pool-789", "org", "confluent_cloud")
        assert result.identity_type == "identity_pool"

    def test_unknown_prefix_unknown_type(self) -> None:
        """Unknown prefix creates unknown type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("xyz-123", "org", "confluent_cloud")
        assert result.identity_type == "unknown"

    def test_no_dash_unknown_type(self) -> None:
        """ID without dash creates unknown type."""
        from plugins.confluent_cloud.handlers._identity_helpers import (
            create_sentinel_from_id,
        )

        result = create_sentinel_from_id("nohyphen", "org", "confluent_cloud")
        assert result.identity_type == "unknown"


class TestExtractPrincipalsFromMetrics:
    """Tests for _extract_principals_from_metrics function."""

    def test_extracts_unique_principals(self) -> None:
        """Extracts unique principal_id values from all metrics."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            _extract_principals_from_metrics,
        )

        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    100.0,
                    {"principal_id": "sa-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    200.0,
                    {"principal_id": "sa-2"},
                ),
            ],
            "bytes_out": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_out",
                    150.0,
                    {"principal_id": "sa-1"},
                ),  # Duplicate
            ],
        }

        result = _extract_principals_from_metrics(metrics_data)

        assert result == {"sa-1", "sa-2"}

    def test_empty_metrics_returns_empty_set(self) -> None:
        """Empty metrics data returns empty set."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            _extract_principals_from_metrics,
        )

        result = _extract_principals_from_metrics({})
        assert result == set()

    def test_skips_rows_without_principal_id(self) -> None:
        """Rows without principal_id label are skipped."""
        from plugins.confluent_cloud.handlers.identity_resolution import (
            _extract_principals_from_metrics,
        )

        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    100.0,
                    {"kafka_id": "lkc-abc"},  # No principal_id
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    200.0,
                    {"principal_id": "sa-1"},
                ),
            ],
        }

        result = _extract_principals_from_metrics(metrics_data)

        assert result == {"sa-1"}
