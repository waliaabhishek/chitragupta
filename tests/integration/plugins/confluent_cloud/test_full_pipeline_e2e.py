"""Full pipeline end-to-end integration tests for CCloud plugin.

Tests the complete flow: plugin initialization -> handler dispatch ->
identity resolution -> allocator execution -> ChargebackRows.
Covers all 7 handlers and all product type categories.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    CostType,
    Identity,
    IdentityResolution,
    IdentitySet,
)


def _make_billing_line(
    product_type: str,
    product_category: str = "PLATFORM",
    resource_id: str = "org-123",
    amount: Decimal = Decimal("100"),
) -> BillingLineItem:
    return BillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id=resource_id,
        product_category=product_category,
        product_type=product_type,
        quantity=Decimal("1"),
        unit_price=amount,
        total_cost=amount,
    )


def _make_identity_set(*ids: str) -> IdentitySet:
    s = IdentitySet()
    for iid in ids:
        s.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id=iid,
                identity_type="service_account",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
            )
        )
    return s


def _initialized_plugin():
    """Return an initialized ConfluentCloudPlugin."""
    from plugins.confluent_cloud import ConfluentCloudPlugin

    plugin = ConfluentCloudPlugin()
    plugin.initialize({"ccloud_api": {"key": "k", "secret": "s"}})
    return plugin


class TestFullPipelineWithAllProductTypes:
    """Test handler dispatch for every product category."""

    def test_kafka_handler_dispatched(self) -> None:
        """Kafka product types route to kafka handler."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        kafka = handlers["kafka"]

        for pt in ("KAFKA_NUM_CKU", "KAFKA_BASE", "KAFKA_NETWORK_READ"):
            assert pt in kafka.handles_product_types
            allocator = kafka.get_allocator(pt)
            assert callable(allocator)

    def test_schema_registry_handler_dispatched(self) -> None:
        """Schema registry product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        sr = handlers["schema_registry"]

        for pt in sr.handles_product_types:
            allocator = sr.get_allocator(pt)
            assert callable(allocator)

    def test_connector_handler_dispatched(self) -> None:
        """Connector product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        conn = handlers["connector"]

        for pt in ("CONNECT_CAPACITY", "CONNECT_NUM_TASKS", "CONNECT_THROUGHPUT"):
            assert pt in conn.handles_product_types
            allocator = conn.get_allocator(pt)
            assert callable(allocator)

    def test_ksqldb_handler_dispatched(self) -> None:
        """ksqlDB product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        ksqldb = handlers["ksqldb"]

        for pt in ksqldb.handles_product_types:
            allocator = ksqldb.get_allocator(pt)
            assert callable(allocator)

    def test_flink_handler_dispatched(self) -> None:
        """Flink product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        flink = handlers["flink"]

        for pt in ("FLINK_NUM_CFU", "FLINK_NUM_CFUS"):
            assert pt in flink.handles_product_types
            allocator = flink.get_allocator(pt)
            assert callable(allocator)

    def test_org_wide_handler_dispatched(self) -> None:
        """Org-wide product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        org_wide = handlers["org_wide"]

        for pt in ("AUDIT_LOG_READ", "SUPPORT"):
            assert pt in org_wide.handles_product_types
            allocator = org_wide.get_allocator(pt)
            assert callable(allocator)

    def test_default_handler_dispatched(self) -> None:
        """Default handler product types route correctly."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        default = handlers["default"]

        for pt in (
            "TABLEFLOW_DATA_PROCESSED",
            "TABLEFLOW_NUM_TOPICS",
            "TABLEFLOW_STORAGE",
            "CLUSTER_LINKING_PER_LINK",
            "CLUSTER_LINKING_READ",
            "CLUSTER_LINKING_WRITE",
        ):
            assert pt in default.handles_product_types
            allocator = default.get_allocator(pt)
            assert callable(allocator)


class TestHandlerOrderingKafkaFirst:
    """Verify handler iteration order."""

    def test_kafka_is_first_handler(self) -> None:
        """Kafka must be first for environment gathering."""
        plugin = _initialized_plugin()
        keys = list(plugin.get_service_handlers().keys())
        assert keys[0] == "kafka"

    def test_default_is_last_handler(self) -> None:
        """Default (catch-all) must be last."""
        plugin = _initialized_plugin()
        keys = list(plugin.get_service_handlers().keys())
        assert keys[-1] == "default"

    def test_full_handler_order(self) -> None:
        """All 7 handlers in expected order."""
        plugin = _initialized_plugin()
        keys = list(plugin.get_service_handlers().keys())
        assert keys == [
            "kafka",
            "schema_registry",
            "connector",
            "ksqldb",
            "flink",
            "org_wide",
            "default",
        ]


class TestUnknownProductTypeToUnallocated:
    """Test that unknown product types are not claimed by any handler."""

    def test_unknown_type_not_in_any_handler(self) -> None:
        """A truly unknown product type is not handled by any handler.

        The orchestrator would fall back to _allocate_to_unallocated().
        """
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()

        unknown_type = "BRAND_NEW_UNKNOWN_SERVICE"
        for name, handler in handlers.items():
            assert unknown_type not in handler.handles_product_types, (
                f"Handler {name} unexpectedly claims {unknown_type}"
            )

    def test_no_handler_accepts_unknown_allocator(self) -> None:
        """All handlers raise ValueError for unknown product types."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()

        for _name, handler in handlers.items():
            with pytest.raises(ValueError):
                handler.get_allocator("TOTALLY_UNKNOWN_XYZ")


class TestOrgWideSplitAcrossAllIdentities:
    """End-to-end test: AUDIT_LOG_READ split across tenant-period identities."""

    def test_audit_log_read_even_split(self) -> None:
        """AUDIT_LOG_READ: $100 split across 4 tenant-period identities."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()
        org_wide = handlers["org_wide"]

        billing_line = _make_billing_line("AUDIT_LOG_READ", amount=Decimal("100"))

        # Orchestrator would inject tenant_period identities
        identities = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=_make_identity_set("sa-1", "sa-2", "sa-3", "sa-4"),
        )

        allocator = org_wide.get_allocator("AUDIT_LOG_READ")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identities,
            split_amount=billing_line.total_cost,
        )
        result = allocator(ctx)

        assert len(result.rows) == 4
        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")
        assert all(r.cost_type == CostType.SHARED for r in result.rows)
        assert {r.identity_id for r in result.rows} == {"sa-1", "sa-2", "sa-3", "sa-4"}
        assert all(r.amount == Decimal("25.0000") for r in result.rows)

    def test_support_even_split_two(self) -> None:
        """SUPPORT: $60 split across 2 identities -> $30 each."""
        plugin = _initialized_plugin()
        org_wide = plugin.get_service_handlers()["org_wide"]

        billing_line = _make_billing_line("SUPPORT", amount=Decimal("60"))
        identities = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=_make_identity_set("u-alice", "u-bob"),
        )

        allocator = org_wide.get_allocator("SUPPORT")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identities,
            split_amount=billing_line.total_cost,
        )
        result = allocator(ctx)

        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("60")

    def test_org_wide_no_identities_unallocated(self) -> None:
        """Org-wide with no identities -> UNALLOCATED."""
        plugin = _initialized_plugin()
        org_wide = plugin.get_service_handlers()["org_wide"]

        billing_line = _make_billing_line("AUDIT_LOG_READ", amount=Decimal("50"))
        identities = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        allocator = org_wide.get_allocator("AUDIT_LOG_READ")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identities,
            split_amount=billing_line.total_cost,
        )
        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("50")


class TestDefaultHandlerEndToEnd:
    """End-to-end tests for default handler allocations."""

    def test_tableflow_to_unallocated(self) -> None:
        """TABLEFLOW_STORAGE -> resource_id, SHARED."""
        plugin = _initialized_plugin()
        default = plugin.get_service_handlers()["default"]

        billing_line = _make_billing_line(
            "TABLEFLOW_STORAGE",
            product_category="TABLEFLOW",
            resource_id="tableflow-1",
            amount=Decimal("25"),
        )
        identities = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        allocator = default.get_allocator("TABLEFLOW_STORAGE")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identities,
            split_amount=billing_line.total_cost,
        )
        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == billing_line.resource_id
        assert result.rows[0].amount == Decimal("25")
        assert result.rows[0].cost_type == CostType.SHARED

    def test_cluster_linking_to_unallocated(self) -> None:
        """CLUSTER_LINKING_READ -> resource_id, USAGE."""
        plugin = _initialized_plugin()
        default = plugin.get_service_handlers()["default"]

        billing_line = _make_billing_line(
            "CLUSTER_LINKING_READ",
            product_category="CLUSTER_LINKING",
            resource_id="lkc-link-1",
            amount=Decimal("40"),
        )
        identities = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        allocator = default.get_allocator("CLUSTER_LINKING_READ")
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=identities,
            split_amount=billing_line.total_cost,
        )
        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == billing_line.resource_id
        assert result.rows[0].amount == Decimal("40")


class TestProductTypeCoverage:
    """Verify all expected product types are covered across handlers."""

    def test_all_reference_product_types_covered(self) -> None:
        """All product types from the reference CHARGEBACK_EXECUTORS are covered."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()

        all_covered: set[str] = set()
        for handler in handlers.values():
            all_covered.update(handler.handles_product_types)

        # Product types from the reference protocol.py CHARGEBACK_EXECUTORS
        expected = {
            # Kafka
            "KAFKA_NUM_CKU",
            "KAFKA_NUM_CKUS",
            "KAFKA_BASE",
            "KAFKA_PARTITION",
            "KAFKA_STORAGE",
            "KAFKA_NETWORK_READ",
            "KAFKA_NETWORK_WRITE",
            # Schema Registry
            "GOVERNANCE_BASE",
            "SCHEMA_REGISTRY",
            "NUM_RULES",
            # Connectors
            "CONNECT_CAPACITY",
            "CONNECT_NUM_TASKS",
            "CONNECT_THROUGHPUT",
            "CUSTOM_CONNECT_PLUGIN",
            # ksqlDB
            "KSQL_NUM_CSU",
            "KSQL_NUM_CSUS",
            # Flink
            "FLINK_NUM_CFU",
            "FLINK_NUM_CFUS",
            # Org-wide
            "AUDIT_LOG_READ",
            "SUPPORT",
            # Default
            "TABLEFLOW_DATA_PROCESSED",
            "TABLEFLOW_NUM_TOPICS",
            "TABLEFLOW_STORAGE",
            "CLUSTER_LINKING_PER_LINK",
            "CLUSTER_LINKING_READ",
            "CLUSTER_LINKING_WRITE",
        }
        missing = expected - all_covered
        assert not missing, f"Product types not covered by any handler: {missing}"

    def test_no_product_type_overlap(self) -> None:
        """No product type is claimed by multiple handlers."""
        plugin = _initialized_plugin()
        handlers = plugin.get_service_handlers()

        seen: dict[str, str] = {}
        for name, handler in handlers.items():
            for pt in handler.handles_product_types:
                assert pt not in seen, f"Product type {pt} claimed by both {seen[pt]} and {name}"
                seen[pt] = name
