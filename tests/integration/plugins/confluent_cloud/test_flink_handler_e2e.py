"""End-to-end integration tests for Flink handler.

These tests verify the full flow:
billing_line -> handler selection -> metrics -> identity resolution -> allocator -> ChargebackRows
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    CostType,
    Identity,
    MetricRow,
    Resource,
)


@pytest.fixture
def mock_uow_for_flink_with_owners() -> MagicMock:
    """Mock UnitOfWork with Flink statement resources and owner identities."""
    uow = MagicMock()

    stmt_a = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="uid-a",
        resource_type="flink_statement",
        display_name="stmt-alpha",
        owner_id="sa-alice",
        metadata={"statement_name": "stmt-alpha", "compute_pool_id": "lfcp-pool-1"},
    )
    stmt_b = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="uid-b",
        resource_type="flink_statement",
        display_name="stmt-beta",
        owner_id="sa-bob",
        metadata={"statement_name": "stmt-beta", "compute_pool_id": "lfcp-pool-1"},
    )

    sa_alice = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-alice",
        identity_type="service_account",
        display_name="Alice SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    sa_bob = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-bob",
        identity_type="service_account",
        display_name="Bob SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([stmt_a, stmt_b], 2)

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([sa_alice, sa_bob], 2)
    identity_map = {i.identity_id: i for i in [sa_alice, sa_bob]}
    uow.identities.get.side_effect = lambda ecosystem, tenant_id, identity_id: identity_map.get(identity_id)

    return uow


@pytest.fixture
def mock_uow_for_flink_no_statements() -> MagicMock:
    """Mock UnitOfWork with no statement resources."""
    uow = MagicMock()
    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([], 0)
    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([], 0)
    return uow


@pytest.fixture
def flink_billing_line() -> BillingLineItem:
    """Billing line for FLINK_NUM_CFU."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lfcp-pool-1",
        product_category="FLINK",
        product_type="FLINK_NUM_CFU",
        quantity=Decimal("10"),
        unit_price=Decimal("10"),
        total_cost=Decimal("100"),
    )


@pytest.fixture
def flink_metrics_two_owners() -> dict[str, list[MetricRow]]:
    """Metrics data with two statement owners."""
    return {
        "flink_cfu_primary": [
            MetricRow(
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                metric_key="confluent_flink_num_cfu",
                value=60.0,
                labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-alpha"},
            ),
            MetricRow(
                timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                metric_key="confluent_flink_num_cfu",
                value=40.0,
                labels={"compute_pool_id": "lfcp-pool-1", "flink_statement_name": "stmt-beta"},
            ),
        ]
    }


class TestFlinkBillingToChargebackEndToEnd:
    """End-to-end tests for Flink allocation."""

    def test_flink_billing_to_chargeback_usage_ratio_e2e(
        self,
        mock_uow_for_flink_with_owners: MagicMock,
        flink_billing_line: BillingLineItem,
        flink_metrics_two_owners: dict[str, list[MetricRow]],
    ) -> None:
        """Full flow: billing + metrics -> identity resolution -> ratio allocation.

        Two statements owned by different SAs split cost by CFU ratio (60/40).
        """
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # Step 1: Verify metrics are needed
        metrics_queries = handler.get_metrics_for_product_type("FLINK_NUM_CFU")
        assert len(metrics_queries) == 2
        keys = {mq.key for mq in metrics_queries}
        assert "flink_cfu_primary" in keys
        assert "flink_cfu_fallback" in keys

        # Step 2: Resolve identities with metrics
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_timestamp=flink_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=flink_metrics_two_owners,
            uow=mock_uow_for_flink_with_owners,
        )
        assert len(identity_res.resource_active) == 2
        assert "sa-alice" in identity_res.resource_active.ids()
        assert "sa-bob" in identity_res.resource_active.ids()
        assert "stmt_owner_cfu" in identity_res.context

        # Step 3: Get allocator and execute (context carries stmt_owner_cfu)
        allocator = handler.get_allocator("FLINK_NUM_CFU")
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=identity_res,
            split_amount=flink_billing_line.total_cost,
            metrics_data=flink_metrics_two_owners,
            params={},
        )
        result = allocator(ctx)

        # Step 4: Verify chargeback rows
        assert len(result.rows) == 2
        total_allocated = sum(row.amount for row in result.rows)
        assert total_allocated == Decimal("100")

        row_by_id = {r.identity_id: r for r in result.rows}
        assert row_by_id["sa-alice"].amount == Decimal("60.0000")
        assert row_by_id["sa-bob"].amount == Decimal("40.0000")
        assert all(r.cost_type == CostType.USAGE for r in result.rows)

    def test_flink_no_metrics_fallback_e2e(
        self,
        mock_uow_for_flink_no_statements: MagicMock,
        flink_billing_line: BillingLineItem,
    ) -> None:
        """Flink without metrics falls back to UNALLOCATED."""
        from plugins.confluent_cloud.handlers.flink import FlinkHandler

        handler = FlinkHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # Resolve identities without metrics
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lfcp-pool-1",
            billing_timestamp=flink_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_flink_no_statements,
        )
        assert len(identity_res.resource_active) == 0
        assert identity_res.context == {}

        # Allocate (no stmt_owner_cfu, no identities -> UNALLOCATED)
        allocator = handler.get_allocator("FLINK_NUM_CFU")
        ctx = AllocationContext(
            timeslice=flink_billing_line.timestamp,
            billing_line=flink_billing_line,
            identities=identity_res,
            split_amount=flink_billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].cost_type == CostType.USAGE


class TestPluginHandlerIntegrationFlink:
    """Integration tests for plugin -> Flink handler flow."""

    def test_plugin_provides_flink_handler(self) -> None:
        """Plugin provides flink handler that can resolve allocators."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()

        assert "flink" in handlers
        allocator = handlers["flink"].get_allocator("FLINK_NUM_CFU")
        assert callable(allocator)

    def test_flink_handler_product_coverage(self) -> None:
        """Flink handler covers expected product types."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()
        flink_handler = handlers["flink"]

        expected = {"FLINK_NUM_CFU", "FLINK_NUM_CFUS"}
        assert expected <= set(flink_handler.handles_product_types)

    def test_flink_handler_returns_metrics_queries(self) -> None:
        """Flink handler returns non-empty metrics queries (unlike connector/ksqldb)."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()
        flink_handler = handlers["flink"]

        queries = flink_handler.get_metrics_for_product_type("FLINK_NUM_CFU")
        assert len(queries) == 2
        keys = {mq.key for mq in queries}
        assert "flink_cfu_primary" in keys
        assert "flink_cfu_fallback" in keys
