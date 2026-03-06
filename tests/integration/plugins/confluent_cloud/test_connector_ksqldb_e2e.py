"""End-to-end integration tests for Connector and ksqlDB handlers.

These tests verify the full flow:
billing_line -> handler selection -> identity resolution -> allocator -> ChargebackRows
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
    Resource,
)


@pytest.fixture
def mock_uow_for_connector() -> MagicMock:
    """Mock UnitOfWork with connector resource and service account identity."""
    uow = MagicMock()

    # Connector resource with SERVICE_ACCOUNT auth mode
    connector = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="connector-abc",
        resource_type="connector",
        display_name="Test Connector",
        metadata={
            "kafka_auth_mode": "SERVICE_ACCOUNT",
            "kafka_service_account_id": "sa-xxx",
        },
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    # Service account identity
    sa_xxx = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-xxx",
        identity_type="service_account",
        display_name="Connector SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([connector], 1)
    uow.resources.get.return_value = connector

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([sa_xxx], 1)
    uow.identities.get.return_value = sa_xxx

    return uow


@pytest.fixture
def mock_uow_for_connector_api_key() -> MagicMock:
    """Mock UnitOfWork with connector using API key auth mode."""
    uow = MagicMock()

    # Connector resource with KAFKA_API_KEY auth mode
    connector = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="connector-def",
        resource_type="connector",
        display_name="API Key Connector",
        metadata={
            "kafka_auth_mode": "KAFKA_API_KEY",
            "kafka_api_key": "api-key-1",
        },
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    # API key identity with owner reference
    api_key = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="api-key-1",
        identity_type="api_key",
        metadata={"owner_id": "sa-owner-1"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    # Owner service account
    sa_owner = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-owner-1",
        identity_type="service_account",
        display_name="API Key Owner SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([connector], 1)
    uow.resources.get.return_value = connector

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([api_key, sa_owner], 2)
    identity_map = {i.identity_id: i for i in [api_key, sa_owner]}
    uow.identities.get.side_effect = lambda ecosystem, tenant_id, identity_id: identity_map.get(identity_id)

    return uow


@pytest.fixture
def mock_uow_for_connector_unknown_mode() -> MagicMock:
    """Mock UnitOfWork with connector using UNKNOWN auth mode."""
    uow = MagicMock()

    # Connector resource with UNKNOWN or missing auth mode
    connector = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="connector-ghi",
        resource_type="connector",
        display_name="Unknown Auth Connector",
        metadata={
            "kafka_auth_mode": "UNKNOWN",  # Or could be missing entirely
        },
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([connector], 1)
    uow.resources.get.return_value = connector

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([], 0)
    uow.identities.get.return_value = None

    return uow


@pytest.fixture
def mock_uow_for_ksqldb() -> MagicMock:
    """Mock UnitOfWork with ksqlDB resource and owner identity."""
    uow = MagicMock()

    # ksqlDB resource with owner_id
    ksqldb_app = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="lksqlc-ksql1",
        resource_type="ksqldb_cluster",
        display_name="Test ksqlDB",
        metadata={
            "owner_id": "sa-ksql-owner",
        },
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    # Owner service account
    sa_owner = Identity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id="sa-ksql-owner",
        identity_type="service_account",
        display_name="ksqlDB Owner SA",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
    uow.resources.get.return_value = ksqldb_app

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([sa_owner], 1)
    uow.identities.get.return_value = sa_owner

    return uow


@pytest.fixture
def mock_uow_for_ksqldb_missing_owner() -> MagicMock:
    """Mock UnitOfWork with ksqlDB resource without owner_id."""
    uow = MagicMock()

    # ksqlDB resource without owner_id
    ksqldb_app = Resource(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        resource_id="lksqlc-ksql2",
        resource_type="ksqldb_cluster",
        display_name="Orphan ksqlDB",
        metadata={},  # No owner_id
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    uow.resources = MagicMock()
    uow.resources.find_by_period.return_value = ([ksqldb_app], 1)
    uow.resources.get.return_value = ksqldb_app

    uow.identities = MagicMock()
    uow.identities.find_by_period.return_value = ([], 0)
    uow.identities.get.return_value = None

    return uow


@pytest.fixture
def connector_billing_line() -> BillingLineItem:
    """Billing line for CONNECT_CAPACITY."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="connector-abc",
        product_category="CONNECT",
        product_type="CONNECT_CAPACITY",
        quantity=Decimal("1"),
        unit_price=Decimal("50"),
        total_cost=Decimal("50"),
    )


@pytest.fixture
def ksqldb_billing_line() -> BillingLineItem:
    """Billing line for KSQL_NUM_CSU."""
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="lksqlc-ksql1",
        product_category="KSQL",
        product_type="KSQL_NUM_CSU",
        quantity=Decimal("4"),
        unit_price=Decimal("25"),
        total_cost=Decimal("100"),
    )


class TestConnectorBillingToChargebackEndToEnd:
    """End-to-end tests for Connector allocation."""

    def test_connector_billing_to_chargeback_e2e(
        self, mock_uow_for_connector: MagicMock, connector_billing_line: BillingLineItem
    ) -> None:
        """Full flow: billing line -> identity resolution -> allocation.

        Connector with SERVICE_ACCOUNT auth mode allocates to service account.
        """
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # Step 1: Verify no metrics needed
        metrics_queries = handler.get_metrics_for_product_type("CONNECT_CAPACITY")
        assert metrics_queries == []

        # Step 2: Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="connector-abc",
            billing_timestamp=connector_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_connector,
        )
        assert len(identity_res.resource_active) == 1
        assert "sa-xxx" in identity_res.merged_active.ids()

        # Step 3: Get allocator and execute
        allocator = handler.get_allocator("CONNECT_CAPACITY")
        ctx = AllocationContext(
            timeslice=connector_billing_line.timestamp,
            billing_line=connector_billing_line,
            identities=identity_res,
            split_amount=connector_billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Step 4: Verify chargeback rows
        assert len(result.rows) == 1
        total_allocated = sum(row.amount for row in result.rows)
        assert total_allocated == Decimal("50")

        # All allocation should go to sa-xxx
        assert result.rows[0].identity_id == "sa-xxx"

    def test_connector_api_key_mode_e2e(self, mock_uow_for_connector_api_key: MagicMock) -> None:
        """Connector with KAFKA_API_KEY auth mode resolves to API key owner."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        billing_line = BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="connector-def",
            product_category="CONNECT",
            product_type="CONNECT_CAPACITY",
            quantity=Decimal("1"),
            unit_price=Decimal("50"),
            total_cost=Decimal("50"),
        )

        # Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="connector-def",
            billing_timestamp=billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_connector_api_key,
        )

        # Should resolve to API key owner (sa-owner-1)
        assert len(identity_res.resource_active) == 1
        assert "sa-owner-1" in identity_res.merged_active.ids()

        # Allocate
        allocator = handler.get_allocator("CONNECT_CAPACITY")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identity_res,
            split_amount=billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Verify allocation to sa-owner-1
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-owner-1"
        assert result.rows[0].amount == Decimal("50")

    def test_connector_unknown_mode_fallback_e2e(self, mock_uow_for_connector_unknown_mode: MagicMock) -> None:
        """Connector with UNKNOWN auth mode uses connector_id for per-connector attribution."""
        from plugins.confluent_cloud.handlers.connectors import ConnectorHandler

        handler = ConnectorHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        billing_line = BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="connector-ghi",
            product_category="CONNECT",
            product_type="CONNECT_CAPACITY",
            quantity=Decimal("1"),
            unit_price=Decimal("75"),
            total_cost=Decimal("75"),
        )

        # Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="connector-ghi",
            billing_timestamp=billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_connector_unknown_mode,
        )

        # UNKNOWN mode uses connector_id as per-connector sentinel for individual attribution
        assert len(identity_res.resource_active) == 1
        assert "connector-ghi" in identity_res.merged_active.ids()
        assert "connector_credentials_unknown" not in identity_res.merged_active.ids()

        # Allocate
        allocator = handler.get_allocator("CONNECT_CAPACITY")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identity_res,
            split_amount=billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Verify allocation to per-connector sentinel
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "connector-ghi"
        assert result.rows[0].amount == Decimal("75")


class TestKsqldbBillingToChargebackEndToEnd:
    """End-to-end tests for ksqlDB allocation."""

    def test_ksqldb_billing_to_chargeback_e2e(
        self, mock_uow_for_ksqldb: MagicMock, ksqldb_billing_line: BillingLineItem
    ) -> None:
        """Full flow: billing line -> identity resolution -> allocation.

        ksqlDB with owner_id allocates to owner service account.
        """
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        # Step 1: Verify no metrics needed
        metrics_queries = handler.get_metrics_for_product_type("KSQL_NUM_CSU")
        assert metrics_queries == []

        # Step 2: Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lksqlc-ksql1",
            billing_timestamp=ksqldb_billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_ksqldb,
        )
        assert len(identity_res.resource_active) == 1
        assert "sa-ksql-owner" in identity_res.merged_active.ids()

        # Step 3: Get allocator and execute
        allocator = handler.get_allocator("KSQL_NUM_CSU")
        ctx = AllocationContext(
            timeslice=ksqldb_billing_line.timestamp,
            billing_line=ksqldb_billing_line,
            identities=identity_res,
            split_amount=ksqldb_billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Step 4: Verify chargeback rows
        assert len(result.rows) == 1
        total_allocated = sum(row.amount for row in result.rows)
        assert total_allocated == Decimal("100")

        # All allocation should go to sa-ksql-owner
        assert result.rows[0].identity_id == "sa-ksql-owner"

    def test_ksqldb_missing_owner_fallback_e2e(self, mock_uow_for_ksqldb_missing_owner: MagicMock) -> None:
        """ksqlDB without owner_id falls back to sentinel identity."""
        from plugins.confluent_cloud.handlers.ksqldb import KsqldbHandler
        from plugins.confluent_cloud.handlers.ksqldb_identity import KSQLDB_OWNER_UNKNOWN

        handler = KsqldbHandler(connection=None, config=None, ecosystem="confluent_cloud")
        billing_duration = timedelta(hours=24)

        billing_line = BillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="lksqlc-ksql2",
            product_category="KSQL",
            product_type="KSQL_NUM_CSU",
            quantity=Decimal("2"),
            unit_price=Decimal("25"),
            total_cost=Decimal("50"),
        )

        # Resolve identities
        identity_res = handler.resolve_identities(
            tenant_id="org-123",
            resource_id="lksqlc-ksql2",
            billing_timestamp=billing_line.timestamp,
            billing_duration=billing_duration,
            metrics_data=None,
            uow=mock_uow_for_ksqldb_missing_owner,
        )

        # Should fall back to unknown sentinel
        assert len(identity_res.resource_active) == 1
        assert KSQLDB_OWNER_UNKNOWN in identity_res.merged_active.ids()

        # Allocate
        allocator = handler.get_allocator("KSQL_NUM_CSU")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identity_res,
            split_amount=billing_line.total_cost,
            metrics_data=None,
            params={},
        )
        result = allocator(ctx)

        # Verify allocation to sentinel
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == KSQLDB_OWNER_UNKNOWN
        assert result.rows[0].amount == Decimal("50")


class TestPluginHandlerIntegrationConnectorKsqldb:
    """Integration tests for plugin -> handler flow for connectors and ksqlDB."""

    def test_plugin_provides_connector_and_ksqldb_handlers(self) -> None:
        """Plugin provides connector and ksqlDB handlers that can resolve allocators."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()

        # Verify handlers exist
        assert "connector" in handlers
        assert "ksqldb" in handlers

        # Verify handlers can get allocators
        connector_allocator = handlers["connector"].get_allocator("CONNECT_CAPACITY")
        assert callable(connector_allocator)

        ksqldb_allocator = handlers["ksqldb"].get_allocator("KSQL_NUM_CSU")
        assert callable(ksqldb_allocator)

    def test_connector_handler_product_coverage(self) -> None:
        """Connector handler covers expected product types."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()
        connector_handler = handlers["connector"]

        expected = {
            "CONNECT_CAPACITY",
            "CONNECT_NUM_TASKS",
            "CONNECT_THROUGHPUT",
            "CUSTOM_CONNECT_PLUGIN",
        }
        assert expected <= set(connector_handler.handles_product_types)

    def test_ksqldb_handler_product_coverage(self) -> None:
        """ksqlDB handler covers expected product types."""
        from plugins.confluent_cloud import ConfluentCloudPlugin

        plugin = ConfluentCloudPlugin()
        plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})

        handlers = plugin.get_service_handlers()
        ksqldb_handler = handlers["ksqldb"]

        expected = {
            "KSQL_NUM_CSU",
            "KSQL_NUM_CSUS",
        }
        assert expected <= set(ksqldb_handler.handles_product_types)
