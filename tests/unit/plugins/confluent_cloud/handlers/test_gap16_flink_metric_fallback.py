"""GAP-16: Tests for Flink CFU metric name divergence fix.

Verifies that resolve_flink_identity selects flink_cfu_primary data first
and falls back to flink_cfu_fallback when primary is absent, and that
FlinkHandler.get_metrics_for_product_type returns both metric queries.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from core.models import CoreIdentity, CoreResource, Identity, MetricRow, Resource

_BILLING_START = datetime(2026, 2, 1, tzinfo=UTC)
_BILLING_END = datetime(2026, 2, 2, tzinfo=UTC)
_TENANT = "org-123"
_POOL = "lfcp-pool-1"


def _make_metric_row(key: str, stmt_name: str, value: float) -> MetricRow:
    return MetricRow(
        timestamp=_BILLING_START,
        metric_key=key,
        value=value,
        labels={"compute_pool_id": _POOL, "flink_statement_name": stmt_name},
    )


def _make_stmt_resource(stmt_name: str, owner_id: str) -> Resource:
    return CoreResource(
        ecosystem="confluent_cloud",
        tenant_id=_TENANT,
        resource_id=f"uid-{stmt_name}",
        resource_type="flink_statement",
        display_name=stmt_name,
        owner_id=owner_id,
        metadata={"statement_name": stmt_name, "compute_pool_id": _POOL},
    )


def _make_owner(owner_id: str) -> Identity:
    return CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id=_TENANT,
        identity_id=owner_id,
        identity_type="service_account",
    )


class TestFlinkMetricFallbackResolution:
    """Tests for primary/fallback metric key selection in resolve_flink_identity."""

    def test_only_legacy_metric_present_uses_fallback(self, mock_uow: MagicMock) -> None:
        """metrics_data has only flink_cfu_fallback key → fallback data drives allocation."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        stmt = _make_stmt_resource("stmt-legacy", "sa-legacy-owner")
        owner = _make_owner("sa-legacy-owner")
        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        metrics = {
            "flink_cfu_fallback": [
                _make_metric_row(
                    "confluent_flink_statement_utilization_cfu_minutes_consumed",
                    "stmt-legacy",
                    42.0,
                )
            ]
        }

        result = resolve_flink_identity(
            tenant_id=_TENANT,
            resource_id=_POOL,
            billing_start=_BILLING_START,
            billing_end=_BILLING_END,
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-legacy-owner" in result.resource_active.ids()
        assert result.context.get("stmt_owner_cfu") == {"sa-legacy-owner": 42.0}

    def test_only_primary_metric_present_uses_primary(self, mock_uow: MagicMock) -> None:
        """metrics_data has only flink_cfu_primary key → primary data drives allocation."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        stmt = _make_stmt_resource("stmt-primary", "sa-primary-owner")
        owner = _make_owner("sa-primary-owner")
        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        metrics = {"flink_cfu_primary": [_make_metric_row("confluent_flink_num_cfu", "stmt-primary", 99.0)]}

        result = resolve_flink_identity(
            tenant_id=_TENANT,
            resource_id=_POOL,
            billing_start=_BILLING_START,
            billing_end=_BILLING_END,
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 1
        assert "sa-primary-owner" in result.resource_active.ids()
        assert result.context.get("stmt_owner_cfu") == {"sa-primary-owner": 99.0}

    def test_both_metrics_present_primary_takes_precedence(self, mock_uow: MagicMock) -> None:
        """Both flink_cfu_primary and flink_cfu_fallback present → primary used, no double-count."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        stmt = _make_stmt_resource("stmt-shared", "sa-shared-owner")
        owner = _make_owner("sa-shared-owner")
        mock_uow.resources.find_by_period.return_value = ([stmt], 1)
        mock_uow.identities.find_by_period.return_value = ([owner], 1)

        metrics = {
            "flink_cfu_primary": [_make_metric_row("confluent_flink_num_cfu", "stmt-shared", 10.0)],
            "flink_cfu_fallback": [
                _make_metric_row(
                    "confluent_flink_statement_utilization_cfu_minutes_consumed",
                    "stmt-shared",
                    20.0,
                )
            ],
        }

        result = resolve_flink_identity(
            tenant_id=_TENANT,
            resource_id=_POOL,
            billing_start=_BILLING_START,
            billing_end=_BILLING_END,
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        # Primary value (10.0) used — fallback (20.0) ignored — no double-count (30.0)
        assert result.context.get("stmt_owner_cfu") == {"sa-shared-owner": 10.0}

    def test_neither_metric_present_falls_through_to_unallocated(self, mock_uow: MagicMock) -> None:
        """metrics_data has neither flink_cfu_primary nor flink_cfu_fallback → empty resolution."""
        from plugins.confluent_cloud.handlers.flink_identity import resolve_flink_identity

        # No running statements in DB either
        mock_uow.resources.find_by_period.return_value = ([], 0)
        mock_uow.identities.find_by_period.return_value = ([], 0)

        # metrics_data with unrelated keys (no flink_cfu_primary or flink_cfu_fallback)
        metrics: dict = {}

        result = resolve_flink_identity(
            tenant_id=_TENANT,
            resource_id=_POOL,
            billing_start=_BILLING_START,
            billing_end=_BILLING_END,
            metrics_data=metrics,
            uow=mock_uow,
            ecosystem="confluent_cloud",
        )

        assert len(result.resource_active) == 0
        assert result.context == {}


class TestFlinkHandlerGetMetricsBothKeys:
    """Tests that FlinkHandler returns both primary and fallback MetricQuery objects."""

    def test_get_metrics_returns_both_primary_and_fallback(self) -> None:
        """get_metrics_for_product_type('FLINK_NUM_CFU') returns 2 MetricQuery with correct keys."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        metrics = handler.get_metrics_for_product_type("FLINK_NUM_CFU")

        assert len(metrics) == 2

        keys = {mq.key for mq in metrics}
        assert "flink_cfu_primary" in keys
        assert "flink_cfu_fallback" in keys

        primary = next(mq for mq in metrics if mq.key == "flink_cfu_primary")
        fallback = next(mq for mq in metrics if mq.key == "flink_cfu_fallback")

        assert "confluent_flink_num_cfu" in primary.query_expression
        assert "confluent_flink_statement_utilization_cfu_minutes_consumed" in fallback.query_expression
