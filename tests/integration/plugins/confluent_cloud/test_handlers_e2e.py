"""End-to-end integration tests for Kafka and Schema Registry handlers.

These tests verify the full flow:
billing_line → handler selection → identity resolution → allocator → ChargebackRows
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    Identity,
    MetricRow,
)


@pytest.fixture
def mock_uow_with_identities() -> MagicMock:
    """Mock UnitOfWork with identity data for Kafka cluster."""
    uow = MagicMock()

    # Setup identities
    api_key = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="api-key-1",
        identity_type="api_key",
        metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner-1"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    sa_owner = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-owner-1",
        identity_type="service_account",
        display_name="Test SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([api_key, sa_owner], 2)
    return uow


@pytest.fixture
def kafka_billing_line() -> BillingLineItem:
    """Billing line for KAFKA_NUM_CKU."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NUM_CKU",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )


class TestKafkaCkuEndToEnd:
    """End-to-end tests for Kafka CKU allocation."""

    def test_full_flow_with_metrics(
        self, mock_uow_with_identities: MagicMock, kafka_billing_line: BillingLineItem
    ) -> None:
        """Full flow: billing line → metrics queries → identity resolution → allocation."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # Step 1: Get metrics queries
        metrics_queries = handler.get_metrics_for_product_type("KAFKA_NUM_CKU")
        assert len(metrics_queries) == 2
        assert {m.key for m in metrics_queries} == {"bytes_in", "bytes_out"}

        # Step 2: Simulate metrics data (as if returned by Prometheus)
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="bytes_in",
                    value=7000.0,
                    labels={"kafka_id": "lkc-abc", "principal_id": "sa-owner-1"},
                ),
            ],
            "bytes_out": [
                MetricRow(
                    timestamp=datetime(2026, 2, 1, tzinfo=UTC),
                    metric_key="bytes_out",
                    value=3000.0,
                    labels={"kafka_id": "lkc-abc", "principal_id": "sa-owner-1"},
                ),
            ],
        }

        # Step 3: Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=kafka_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=metrics_data,
            uow=mock_uow_with_identities,
        )
        assert len(identity_res.resource_active) >= 1
        assert "sa-owner-1" in identity_res.merged_active.ids()

        # Step 4: Get allocator and execute
        allocator = handler.get_allocator("KAFKA_NUM_CKU")
        ctx = AllocationContext(
            timeslice=kafka_billing_line.timestamp,
            billing_line=kafka_billing_line,
            identities=identity_res,
            split_amount=kafka_billing_line.total_cost,
            metrics_data=metrics_data,
            params={},  # Use default 70/30 ratios
        )
        result = allocator(ctx)

        # Step 5: Verify chargeback rows
        assert len(result.rows) > 0
        total_allocated = sum(row.amount for row in result.rows)
        assert total_allocated == Decimal("100")

        # All rows should go to sa-owner-1 (only identity)
        for row in result.rows:
            assert row.identity_id == "sa-owner-1"

    def test_no_metrics_fallback(
        self, mock_uow_with_identities: MagicMock, kafka_billing_line: BillingLineItem
    ) -> None:
        """Without metrics, falls back to even split."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # No metrics available
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=kafka_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_with_identities,
        )

        allocator = handler.get_allocator("KAFKA_NUM_CKU")
        ctx = AllocationContext(
            timeslice=kafka_billing_line.timestamp,
            billing_line=kafka_billing_line,
            identities=identity_res,
            split_amount=kafka_billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Should still allocate full amount
        total_allocated = sum(row.amount for row in result.rows)
        assert total_allocated == Decimal("100")


class TestKafkaNetworkEndToEnd:
    """End-to-end tests for Kafka network allocation."""

    def test_network_usage_based_split(self, mock_uow_with_identities: MagicMock) -> None:
        """Network costs split by bytes in/out ratio."""
        from plugins.confluent_cloud.handlers.kafka import KafkaHandler

        handler = KafkaHandler(connection=None, config=None, ecosystem="confluent_cloud")

        # Add second identity for testing split
        api_key_2 = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-2",
            identity_type="api_key",
            metadata={"resource_id": "lkc-abc", "owner_id": "sa-owner-2"},
        )
        sa_owner_2 = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-2",
            identity_type="service_account",
        )
        existing_items, _ = mock_uow_with_identities.identities.find_by_period.return_value
        new_items = existing_items + [api_key_2, sa_owner_2]
        mock_uow_with_identities.identities.find_by_period.return_value = (new_items, len(new_items))

        billing_line = BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="lkc-abc",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_READ",
            quantity=Decimal("1"),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )

        # sa-owner-1: 80% usage, sa-owner-2: 20% usage
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    800.0,
                    {"principal_id": "sa-owner-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    200.0,
                    {"principal_id": "sa-owner-2"},
                ),
            ],
            "bytes_out": [],
        }

        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lkc-abc",
            billing_timestamp=billing_line.timestamp,
            billing_duration=timedelta(hours=24),
            metrics_data=metrics_data,
            uow=mock_uow_with_identities,
        )

        allocator = handler.get_allocator("KAFKA_NETWORK_READ")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identity_res,
            split_amount=billing_line.total_cost,
            metrics_data=metrics_data,
            params={},
        )
        result = allocator(ctx)

        # Verify proportional split
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")

        sa1_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-owner-1")
        sa2_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-owner-2")

        # sa-owner-1 should get more (80%)
        assert sa1_amount > sa2_amount
        assert sa1_amount == Decimal("80")
        assert sa2_amount == Decimal("20")


class TestSchemaRegistryEndToEnd:
    """End-to-end tests for Schema Registry allocation."""

    def test_sr_even_split(self, mock_uow_with_identities: MagicMock) -> None:
        """Schema Registry costs split evenly."""
        from plugins.confluent_cloud.handlers.schema_registry import (
            SchemaRegistryHandler,
        )

        handler = SchemaRegistryHandler(connection=None, config=None, ecosystem="confluent_cloud")

        # Update mock for SR resource
        api_key = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="api-key-sr",
            identity_type="api_key",
            metadata={"resource_id": "lsrc-xyz", "owner_id": "sa-owner-1"},
        )
        sa_owner = Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-owner-1",
            identity_type="service_account",
        )
        mock_uow_with_identities.identities.find_by_period.return_value = (
            [api_key, sa_owner],
            2,
        )

        sr_billing = BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="lsrc-xyz",
            product_category="SCHEMA_REGISTRY",
            product_type="SCHEMA_REGISTRY",
            quantity=Decimal("1"),
            unit_price=Decimal("50"),
            total_cost=Decimal("50"),
        )

        # SR doesn't need metrics
        assert handler.get_metrics_for_product_type("SCHEMA_REGISTRY") == []

        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lsrc-xyz",
            billing_timestamp=sr_billing.timestamp,
            billing_duration=timedelta(hours=24),
            metrics_data=None,
            uow=mock_uow_with_identities,
        )

        allocator = handler.get_allocator("SCHEMA_REGISTRY")
        ctx = AllocationContext(
            timeslice=sr_billing.timestamp,
            billing_line=sr_billing,
            identities=identity_res,
            split_amount=sr_billing.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("50")
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-owner-1"


class TestPluginHandlerIntegration:
    """Integration tests for plugin → handler flow."""

    def test_plugin_provides_working_handlers(self) -> None:
        """Plugin provides handlers that can resolve allocators."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()

        # Verify all expected handlers exist
        assert "kafka" in handlers
        assert "schema_registry" in handlers

        # Verify handlers can get allocators
        kafka_allocator = handlers["kafka"].get_allocator("KAFKA_NUM_CKU")
        assert callable(kafka_allocator)

        sr_allocator = handlers["schema_registry"].get_allocator("SCHEMA_REGISTRY")
        assert callable(sr_allocator)

    def test_plugin_handler_product_coverage(self) -> None:
        """Handlers cover expected product types."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()

        # Collect all handled product types
        all_product_types: set[str] = set()
        for handler in handlers.values():
            all_product_types.update(handler.handles_product_types)

        # Verify expected product types are covered
        expected = {
            "KAFKA_NUM_CKU",
            "KAFKA_NUM_CKUS",
            "KAFKA_BASE",
            "KAFKA_PARTITION",
            "KAFKA_STORAGE",
            "KAFKA_NETWORK_READ",
            "KAFKA_NETWORK_WRITE",
            "SCHEMA_REGISTRY",
            "GOVERNANCE_BASE",
            "NUM_RULES",
        }
        assert expected <= all_product_types
