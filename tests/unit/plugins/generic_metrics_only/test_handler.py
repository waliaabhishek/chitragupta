"""Tests for GenericMetricsOnlyHandler."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from core.models import CoreIdentity, MetricRow


def make_metric_row(key: str, value: float, labels: dict | None = None) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels=labels or {},
    )


@pytest.fixture
def pg_config():
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

    return GenericMetricsOnlyConfig.model_validate(
        {
            "ecosystem_name": "self_managed_postgres",
            "cluster_id": "pg-prod-1",
            "metrics": {"url": "http://prom:9090"},
            "identity_source": {
                "source": "prometheus",
                "label": "datname",
                "discovery_query": "group by (datname) (pg_stat_database_blks_hit)",
                "default_team": "UNASSIGNED",
            },
            "cost_types": [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                },
                {
                    "name": "PG_NETWORK",
                    "product_category": "postgres",
                    "rate": "0.05",
                    "cost_quantity": {
                        "type": "network_gib",
                        "query": "sum(pg_stat_database_blks_read)",
                    },
                    "allocation_strategy": "usage_ratio",
                    "allocation_query": "sum by (datname) (pg_stat_database_blks_read)",
                    "allocation_label": "datname",
                },
            ],
        }
    )


@pytest.fixture
def static_config():
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

    return GenericMetricsOnlyConfig.model_validate(
        {
            "ecosystem_name": "self_managed_postgres",
            "cluster_id": "pg-prod-1",
            "metrics": {"url": "http://prom:9090"},
            "identity_source": {
                "source": "static",
                "static_identities": [
                    {
                        "identity_id": "team-data",
                        "identity_type": "team",
                        "display_name": "Data Team",
                        "team": "data",
                    }
                ],
            },
            "cost_types": [
                {
                    "name": "PG_COMPUTE",
                    "product_category": "postgres",
                    "rate": "2.50",
                    "cost_quantity": {"type": "fixed", "count": 2},
                    "allocation_strategy": "even_split",
                }
            ],
        }
    )


@pytest.fixture
def mock_metrics():
    return MagicMock()


class TestHandlerHandlesProductTypes:
    def test_handles_product_types_returns_names_in_config_order(self, pg_config, mock_metrics) -> None:
        """Test case 7: handles_product_types returns names in config order."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        assert list(handler.handles_product_types) == ["PG_COMPUTE", "PG_NETWORK"]


class TestHandlerGetAllocator:
    def test_usage_ratio_allocator_falls_back_when_metrics_data_none(self, pg_config, mock_metrics) -> None:
        """GIT-001: handler.py:23 — usage_ratio allocator falls back to even split when metrics_data is None."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.models import (
            CoreBillingLineItem,
            IdentityResolution,
            IdentitySet,
        )
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")

        iset = IdentitySet()
        iset.add(
            CoreIdentity(
                ecosystem="self_managed_postgres", tenant_id="tenant-1", identity_id="alice", identity_type="principal"
            )
        )
        iset.add(
            CoreIdentity(
                ecosystem="self_managed_postgres", tenant_id="tenant-1", identity_id="bob", identity_type="principal"
            )
        )
        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="tenant-1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=iset, metrics_derived=IdentitySet(), tenant_period=IdentitySet()
            ),
            split_amount=Decimal("10.00"),
            metrics_data=None,
            params={},
        )

        result = allocator(ctx)

        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"alice", "bob"}
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_usage_ratio_allocator_falls_back_when_no_valid_metric_values(self, pg_config, mock_metrics) -> None:
        """GIT-002: handler.py:30 — usage_ratio allocator falls back when metrics have zero values or missing labels."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.models import (
            CoreBillingLineItem,
            IdentityResolution,
            IdentitySet,
            MetricRow,
        )
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")

        iset = IdentitySet()
        iset.add(
            CoreIdentity(
                ecosystem="self_managed_postgres", tenant_id="tenant-1", identity_id="alice", identity_type="principal"
            )
        )
        iset.add(
            CoreIdentity(
                ecosystem="self_managed_postgres", tenant_id="tenant-1", identity_id="bob", identity_type="principal"
            )
        )
        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="tenant-1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=iset, metrics_derived=IdentitySet(), tenant_period=IdentitySet()
            ),
            split_amount=Decimal("10.00"),
            metrics_data={
                "alloc_PG_NETWORK": [
                    MetricRow(
                        timestamp=datetime(2026, 2, 1, tzinfo=UTC), metric_key="alloc_PG_NETWORK", value=0.0, labels={}
                    ),
                ]
            },
            params={},
        )

        result = allocator(ctx)

        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"alice", "bob"}
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_get_allocator_unknown_type_raises(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("NONEXISTENT")


class TestHandlerGetMetricsForProductType:
    def test_even_split_with_prometheus_source_returns_discovery_query(self, pg_config, mock_metrics) -> None:
        """Test case 10: even_split + prometheus returns MetricQuery(key="discovery", ...)."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_COMPUTE")

        assert len(queries) == 1
        assert queries[0].key == "discovery"
        assert queries[0].query_expression == "group by (datname) (pg_stat_database_blks_hit)"

    def test_usage_ratio_returns_alloc_metric_query(self, pg_config, mock_metrics) -> None:
        """Test case 11: usage_ratio returns MetricQuery(key="alloc_PG_NETWORK", ...)."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_NETWORK")

        assert len(queries) == 1
        assert queries[0].key == "alloc_PG_NETWORK"
        assert queries[0].query_expression == "sum by (datname) (pg_stat_database_blks_read)"

    def test_even_split_with_static_source_returns_empty_list(self, static_config, mock_metrics) -> None:
        """Test case 12: static-only source returns []."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=static_config, metrics_source=mock_metrics)
        queries = handler.get_metrics_for_product_type("PG_COMPUTE")

        assert queries == []

    def test_unknown_product_type_returns_empty_list(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        assert handler.get_metrics_for_product_type("NONEXISTENT") == []


class TestHandlerResolveIdentities:
    def test_resolve_identities_prometheus_source_extracts_from_metrics_data(self, pg_config, mock_metrics) -> None:
        """Test case 13: discovery metrics produce metrics_derived containing extracted identity."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        alice_row = make_metric_row("discovery", 1.0, {"datname": "alice"})
        metrics_data = {"discovery": [alice_row]}

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=metrics_data,
            uow=mock_uow,
        )

        identity_ids = list(result.metrics_derived.ids())
        assert "alice" in identity_ids

    def test_resolve_identities_static_source_returns_in_resource_active(self, static_config, mock_metrics) -> None:
        """Test case 14: static identity config returns identity in resource_active."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=static_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=None,
            uow=mock_uow,
        )

        identity_ids = list(result.resource_active.ids())
        assert "team-data" in identity_ids

    def test_resolve_identities_no_metrics_data_produces_empty_metrics_derived(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=None,
            uow=mock_uow,
        )

        assert list(result.metrics_derived.ids()) == []

    def test_resolve_identities_deduplicates_repeated_identity(self, pg_config, mock_metrics) -> None:
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        mock_uow = MagicMock()

        # Same identity appears multiple times in metrics
        alice_row_1 = make_metric_row("discovery", 10.0, {"datname": "alice"})
        alice_row_2 = make_metric_row("discovery", 20.0, {"datname": "alice"})
        metrics_data = {"discovery": [alice_row_1, alice_row_2]}

        result = handler.resolve_identities(
            tenant_id="tenant-1",
            resource_id="pg-prod-1",
            billing_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            billing_duration=timedelta(days=1),
            metrics_data=metrics_data,
            uow=mock_uow,
        )

        identity_ids = list(result.metrics_derived.ids())
        assert identity_ids.count("alice") == 1


class TestTask024EvenSplitAllocatorAssignment:
    """TASK-024: _make_even_split_allocator removed; even-split cost types use a ChainModel directly."""

    def test_make_even_split_allocator_not_in_handler_module(self) -> None:
        from plugins.generic_metrics_only import handler as handler_module

        assert not hasattr(handler_module, "_make_even_split_allocator")

    def test_even_split_cost_type_gets_chain_model_directly(self, pg_config, mock_metrics) -> None:
        from core.engine.allocation_models import ChainModel
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        assert isinstance(handler.get_allocator("PG_COMPUTE"), ChainModel)


class TestTask080ChainModelMigration:
    """TASK-080: GenericMetricsOnlyHandler migrated to ChainModel composable allocation."""

    # --- even_split tests ---

    def test_even_split_tier0_merged_active_splits_evenly(self, pg_config, mock_metrics) -> None:
        """Tier 0: merged_active has two principals → even split, chain_tier=0, EVEN_SPLIT_ALLOCATION."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_COMPUTE")
        assert isinstance(allocator, ChainModel)

        iset = IdentitySet()
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="alice", identity_type="principal"))
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="bob", identity_type="principal"))

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_COMPUTE",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=iset,
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data=None,
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 2
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"alice", "bob"}
        assert all(r.metadata.get("chain_tier") == 0 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.EVEN_SPLIT_ALLOCATION for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_even_split_tier1_terminal_no_identities(self, pg_config, mock_metrics) -> None:
        """Tier 1: empty merged_active → UNALLOCATED, chain_tier=1, NO_IDENTITIES_LOCATED."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_COMPUTE")
        assert isinstance(allocator, ChainModel)

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_COMPUTE",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=IdentitySet(),
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data=None,
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].metadata.get("chain_tier") == 1
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED

    # --- usage_ratio tests ---

    def test_usage_ratio_tier0_metrics_present_proportional_split(self, pg_config, mock_metrics) -> None:
        """Tier 0: metrics_data present with label values → proportional split, chain_tier=0, USAGE_RATIO_ALLOCATION."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")
        assert isinstance(allocator, ChainModel)

        iset = IdentitySet()
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="alice", identity_type="principal"))
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="bob", identity_type="principal"))

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        alice_row = make_metric_row("alloc_PG_NETWORK", 75.0, {"datname": "alice"})
        bob_row = make_metric_row("alloc_PG_NETWORK", 25.0, {"datname": "bob"})
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=iset,
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data={"alloc_PG_NETWORK": [alice_row, bob_row]},
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 2
        assert all(r.metadata.get("chain_tier") == 0 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result.rows)
        amounts = {r.identity_id: r.amount for r in result.rows}
        assert amounts["alice"] > amounts["bob"]
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_usage_ratio_tier1_metrics_data_none_falls_back_to_even_split(self, pg_config, mock_metrics) -> None:
        """Tier 1: metrics_data=None, non-empty merged_active → even split, chain_tier=1, NO_METRICS_LOCATED."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")
        assert isinstance(allocator, ChainModel)

        iset = IdentitySet()
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="alice", identity_type="principal"))
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="bob", identity_type="principal"))

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=iset,
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data=None,
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 2
        assert all(r.metadata.get("chain_tier") == 1 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert {r.identity_id for r in result.rows} == {"alice", "bob"}
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_usage_ratio_tier1_metrics_key_absent_falls_back_to_even_split(self, pg_config, mock_metrics) -> None:
        """Tier 1: metrics_data={} (key absent), non-empty merged_active → even split, chain_tier=1, NO_METRICS_LOCATED."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, CoreIdentity, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")
        assert isinstance(allocator, ChainModel)

        iset = IdentitySet()
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="alice", identity_type="principal"))
        iset.add(CoreIdentity(ecosystem="self_managed_postgres", tenant_id="t1", identity_id="bob", identity_type="principal"))

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=iset,
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data={},  # key absent
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 2
        assert all(r.metadata.get("chain_tier") == 1 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert {r.identity_id for r in result.rows} == {"alice", "bob"}
        assert sum(r.amount for r in result.rows) == Decimal("10.00")

    def test_usage_ratio_tier2_terminal_no_metrics_no_identities(self, pg_config, mock_metrics) -> None:
        """Tier 2: metrics_data=None, empty merged_active → UNALLOCATED, chain_tier=2, NO_IDENTITIES_LOCATED."""
        from decimal import Decimal

        from core.engine.allocation import AllocationContext
        from core.engine.allocation_models import ChainModel
        from core.models import CoreBillingLineItem, IdentityResolution, IdentitySet
        from core.models.chargeback import AllocationDetail
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        allocator = handler.get_allocator("PG_NETWORK")
        assert isinstance(allocator, ChainModel)

        billing_line = CoreBillingLineItem(
            ecosystem="self_managed_postgres",
            tenant_id="t1",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="pg-prod-1",
            product_category="postgres",
            product_type="PG_NETWORK",
            quantity=Decimal("1"),
            unit_price=Decimal("10.00"),
            total_cost=Decimal("10.00"),
        )
        ctx = AllocationContext(
            timeslice=billing_line.timestamp,
            billing_line=billing_line,
            identities=IdentityResolution(
                resource_active=IdentitySet(),
                metrics_derived=IdentitySet(),
                tenant_period=IdentitySet(),
            ),
            split_amount=Decimal("10.00"),
            metrics_data=None,
            params={},
        )

        result = allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].metadata.get("chain_tier") == 2
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED

    def test_import_hygiene_no_imperative_allocators_in_handler(self) -> None:
        """allocate_evenly_with_fallback and _make_usage_ratio_allocator must not exist in handler module."""
        import ast
        import importlib.util
        import pathlib

        handler_path = pathlib.Path("src/plugins/generic_metrics_only/handler.py")
        source = handler_path.read_text()

        assert "allocate_evenly_with_fallback" not in source, (
            "allocate_evenly_with_fallback still present in handler.py"
        )
        assert "_make_usage_ratio_allocator" not in source, (
            "_make_usage_ratio_allocator still present in handler.py"
        )

    def test_get_allocator_unknown_product_type_raises_value_error(self, pg_config, mock_metrics) -> None:
        """get_allocator with unknown product type raises ValueError with descriptive message."""
        from plugins.generic_metrics_only.handler import GenericMetricsOnlyHandler

        handler = GenericMetricsOnlyHandler(config=pg_config, metrics_source=mock_metrics)
        with pytest.raises(ValueError, match="Unknown product type: NONEXISTENT"):
            handler.get_allocator("NONEXISTENT")

    def test_integration_plugin_initialize_creates_chain_models(self, pg_config) -> None:
        """Integration: plugin.initialize() wires ChainModels through handler for all product types."""
        from unittest.mock import patch

        from core.engine.allocation_models import ChainModel
        from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin

        plugin = GenericMetricsOnlyPlugin()
        # Patch create_metrics_source to avoid real HTTP connection
        with patch("plugins.generic_metrics_only.plugin.create_metrics_source"):
            plugin.initialize(pg_config.model_dump())

        handler = plugin._handler
        assert handler is not None
        for pt in handler.handles_product_types:
            allocator = handler.get_allocator(pt)
            assert isinstance(allocator, ChainModel), f"{pt} allocator is not a ChainModel"
