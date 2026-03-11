"""Tests for SMK allocation models (task-075).

Written in TDD red phase — all tests FAIL until
src/plugins/self_managed_kafka/allocation_models.py is implemented.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.engine.allocation_models import ChainModel
from core.models import (
    BillingLineItem,
    CoreIdentity,
    CostType,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail

if TYPE_CHECKING:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def smk_billing_line() -> BillingLineItem:
    """Standard SMK network billing line."""
    return CoreBillingLineItem(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="kafka-cluster-001",
        product_category="kafka",
        product_type="SELF_KAFKA_NETWORK_INGRESS",
        quantity=Decimal("100"),
        unit_price=Decimal("0.01"),
        total_cost=Decimal("100"),
    )


def make_identity(identity_id: str) -> CoreIdentity:
    return CoreIdentity(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        identity_id=identity_id,
        identity_type="principal",
    )


def make_identity_set(*ids: str) -> IdentitySet:
    iset = IdentitySet()
    for i in ids:
        iset.add(make_identity(i))
    return iset


def make_resolution(
    resource_active: IdentitySet | None = None,
    metrics_derived: IdentitySet | None = None,
    tenant_period: IdentitySet | None = None,
) -> IdentityResolution:
    return IdentityResolution(
        resource_active=resource_active or IdentitySet(),
        metrics_derived=metrics_derived or IdentitySet(),
        tenant_period=tenant_period or IdentitySet(),
    )


def make_metric_row(value: float, principal: str, metric_key: str = "bytes_in_per_principal") -> MetricRow:
    return MetricRow(datetime(2026, 2, 1, tzinfo=UTC), metric_key, value, {"principal": principal})


def make_ctx(
    billing_line: BillingLineItem,
    resolution: IdentityResolution,
    split_amount: Decimal,
    metrics_data: dict | None = None,
) -> AllocationContext:
    return AllocationContext(
        timeslice=billing_line.timestamp,
        billing_line=billing_line,
        identities=resolution,
        split_amount=split_amount,
        metrics_data=metrics_data,
        params={},
    )


def make_smk_config() -> SelfManagedKafkaConfig:
    """Create a minimal SelfManagedKafkaConfig for handler tests."""
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.from_plugin_settings(
        {
            "cluster_id": "kafka-cluster-001",
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


# ---------------------------------------------------------------------------
# Test 1: Tier 0 — ingress usage ratio (70/30 split)
# ---------------------------------------------------------------------------


class TestSmkIngressTier0UsageRatio:
    """SMK_INGRESS_MODEL Tier 0 fires on bytes_in_per_principal with usage-ratio split."""

    def test_70_30_split_by_bytes_in(self, smk_billing_line: BillingLineItem) -> None:
        """alice=700, bob=300 bytes → 70%/30% split, chain_tier=0, USAGE_RATIO_ALLOCATION."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        metrics_data = {
            "bytes_in_per_principal": [
                make_metric_row(700, "User:alice"),
                make_metric_row(300, "User:bob"),
            ]
        }
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        row_by_id = {r.identity_id: r for r in result.rows}
        assert set(row_by_id.keys()) == {"User:alice", "User:bob"}
        assert row_by_id["User:alice"].amount == pytest.approx(Decimal("70.0000"), abs=Decimal("0.01"))
        assert row_by_id["User:bob"].amount == pytest.approx(Decimal("30.0000"), abs=Decimal("0.01"))
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result.rows)
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)


# ---------------------------------------------------------------------------
# Test 2: Tier 0 — direction isolation (wrong metric key falls to Tier 1)
# ---------------------------------------------------------------------------


class TestSmkIngressDirectionIsolation:
    """SMK_INGRESS_MODEL ignores bytes_out_per_principal — Tier 0 returns None, Tier 1 fires."""

    def test_ingress_model_with_egress_key_falls_to_tier1(self, smk_billing_line: BillingLineItem) -> None:
        """SMK_INGRESS_MODEL with only bytes_out_per_principal → Tier 1 even split."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        metrics_data = {
            "bytes_out_per_principal": [
                make_metric_row(700, "User:alice", "bytes_out_per_principal"),
                make_metric_row(300, "User:bob", "bytes_out_per_principal"),
            ]
        }
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        # Must fall to Tier 1 (even split) not Tier 0
        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert len(result.rows) == 2
        assert sum(r.amount for r in result.rows) == Decimal("100")


# ---------------------------------------------------------------------------
# Test 3: Tier 0 — egress usage ratio (EGRESS fires Tier 0, INGRESS fires Tier 1)
# ---------------------------------------------------------------------------


class TestSmkEgressTier0UsageRatio:
    """SMK_EGRESS_MODEL Tier 0 fires on bytes_out_per_principal; SMK_INGRESS_MODEL falls to Tier 1."""

    def test_egress_model_fires_tier0_ingress_fires_tier1(self, smk_billing_line: BillingLineItem) -> None:
        """bytes_out_per_principal data → EGRESS fires Tier 0, INGRESS fires Tier 1."""
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL, SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        metrics_data = {
            "bytes_out_per_principal": [
                make_metric_row(600, "User:alice", "bytes_out_per_principal"),
                make_metric_row(400, "User:bob", "bytes_out_per_principal"),
            ]
        }
        ctx_egress = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)
        ctx_ingress = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result_egress = SMK_EGRESS_MODEL(ctx_egress)
        result_ingress = SMK_INGRESS_MODEL(ctx_ingress)

        # EGRESS fires Tier 0
        assert all(r.metadata["chain_tier"] == 0 for r in result_egress.rows)
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result_egress.rows)

        # INGRESS falls to Tier 1 (wrong key)
        assert all(r.metadata["chain_tier"] == 1 for r in result_ingress.rows)
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result_ingress.rows)


# ---------------------------------------------------------------------------
# Test 4: Tier 1 — no metrics, resource_active present → even split
# ---------------------------------------------------------------------------


class TestSmkTier1NoMetrics:
    """Tier 1: empty metrics_data, resource_active → even split."""

    def test_no_metrics_even_split_resource_active(self, smk_billing_line: BillingLineItem) -> None:
        """metrics_data={}, resource_active=[alice, bob] → even split, chain_tier=1, NO_METRICS_LOCATED."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data={})

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        identity_ids = {r.identity_id for r in result.rows}
        assert identity_ids == {"User:alice", "User:bob"}
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert all(r.metadata["chain_tier"] == 1 for r in result.rows)
        assert all(r.cost_type == CostType.SHARED for r in result.rows)


# ---------------------------------------------------------------------------
# Test 5: Tier 1 — zero usage, resource_active present → Tier 1 fires
# ---------------------------------------------------------------------------


class TestSmkTier1ZeroUsage:
    """Tier 1: metrics present but value=0 → Tier 0 skips, Tier 1 fires."""

    def test_zero_usage_falls_to_tier1(self, smk_billing_line: BillingLineItem) -> None:
        """bytes_in=0 for alice, resource_active=[bob] → Tier 1 even split with bob, NO_METRICS_LOCATED."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:bob"))
        metrics_data = {
            "bytes_in_per_principal": [
                make_metric_row(0, "User:alice"),
            ]
        }
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "User:bob"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_METRICS_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 1
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 6: Tier 2 — terminal to UNALLOCATED when all identity sets empty
# ---------------------------------------------------------------------------


class TestSmkTier2Terminal:
    """Tier 2: all identity sets empty → terminal to UNALLOCATED."""

    def test_empty_identities_terminal_to_unallocated(self, smk_billing_line: BillingLineItem) -> None:
        """metrics_data={}, all identity sets empty → identity_id=UNALLOCATED, NO_IDENTITIES_LOCATED, chain_tier=2."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution()
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data={})

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].cost_type == CostType.SHARED


# ---------------------------------------------------------------------------
# Test 7: Zero-value principal excluded from Tier 0
# ---------------------------------------------------------------------------


class TestSmkZeroValuePrincipalExclusion:
    """Zero-value principal rows are excluded from Tier 0 usage dict."""

    def test_zero_value_principal_excluded_tier0_fires_for_nonzero(self, smk_billing_line: BillingLineItem) -> None:
        """alice=0, bob=500 bytes. resource_active=[alice]. Tier 0 fires for bob only (100%)."""
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice"))
        metrics_data = {
            "bytes_in_per_principal": [
                make_metric_row(0, "User:alice"),
                make_metric_row(500, "User:bob"),
            ]
        }
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        identity_ids = {r.identity_id for r in result.rows}
        # bob gets 100% at Tier 0; alice's zero-value row is excluded
        assert "User:bob" in identity_ids
        assert "User:alice" not in identity_ids
        assert len(result.rows) == 1
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION
        assert result.rows[0].metadata["chain_tier"] == 0


# ---------------------------------------------------------------------------
# Test 8: Model independence
# ---------------------------------------------------------------------------


class TestSmkModelIndependence:
    """SMK_INGRESS_MODEL and SMK_EGRESS_MODEL are distinct instances."""

    def test_ingress_model_is_not_egress_model(self) -> None:
        """SMK_INGRESS_MODEL is not SMK_EGRESS_MODEL — separate ChainModel instances."""
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL, SMK_INGRESS_MODEL

        assert SMK_INGRESS_MODEL is not SMK_EGRESS_MODEL
        assert isinstance(SMK_INGRESS_MODEL, ChainModel)
        assert isinstance(SMK_EGRESS_MODEL, ChainModel)

    def test_models_fire_tier0_independently(self, smk_billing_line: BillingLineItem) -> None:
        """Each model uses its own metric key for Tier 0."""
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL, SMK_INGRESS_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice"))
        metrics_in = {"bytes_in_per_principal": [make_metric_row(500, "User:alice")]}
        metrics_out = {"bytes_out_per_principal": [make_metric_row(500, "User:alice", "bytes_out_per_principal")]}

        ctx_in = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_in)
        ctx_out = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_out)

        result_in = SMK_INGRESS_MODEL(ctx_in)
        result_out = SMK_EGRESS_MODEL(ctx_out)

        assert all(r.metadata["chain_tier"] == 0 for r in result_in.rows)
        assert all(r.metadata["chain_tier"] == 0 for r in result_out.rows)


# ---------------------------------------------------------------------------
# Test 9: COMPUTE/STORAGE use SMK_INFRA_MODEL (ChainModel)
# ---------------------------------------------------------------------------


class TestSmkComputeStorageUnaffected:
    """COMPUTE and STORAGE allocators are SMK_INFRA_MODEL (a ChainModel)."""

    def test_compute_allocator_is_infra_model(self) -> None:
        """get_allocator('SELF_KAFKA_COMPUTE') returns SMK_INFRA_MODEL ChainModel."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        allocator = handler.get_allocator("SELF_KAFKA_COMPUTE")

        assert allocator is SMK_INFRA_MODEL
        assert isinstance(allocator, ChainModel)

    def test_storage_allocator_is_infra_model(self) -> None:
        """get_allocator('SELF_KAFKA_STORAGE') returns SMK_INFRA_MODEL ChainModel."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        allocator = handler.get_allocator("SELF_KAFKA_STORAGE")

        assert allocator is SMK_INFRA_MODEL
        assert isinstance(allocator, ChainModel)


# ---------------------------------------------------------------------------
# Test 10: kafka_allocators not importable
# ---------------------------------------------------------------------------


class TestSmkKafkaAllocatorsRemoved:
    """kafka_allocators.py is deleted — importing from it raises ImportError."""

    def test_kafka_allocators_ingress_raises_import_error(self) -> None:
        """self_kafka_network_ingress_allocator no longer importable."""
        with pytest.raises(ImportError):
            from plugins.self_managed_kafka.allocators.kafka_allocators import (  # noqa: F401
                self_kafka_network_ingress_allocator,
            )

    def test_kafka_allocators_egress_raises_import_error(self) -> None:
        """self_kafka_network_egress_allocator no longer importable."""
        with pytest.raises(ImportError):
            from plugins.self_managed_kafka.allocators.kafka_allocators import (  # noqa: F401
                self_kafka_network_egress_allocator,
            )


# ---------------------------------------------------------------------------
# Test 11: Wiring integration — handler.get_allocator delegates through ChainModel
# ---------------------------------------------------------------------------


class TestSmkHandlerWiring:
    """Verify _ALLOCATOR_MAP wiring: handler.get_allocator()(ctx) executes correctly."""

    def test_handler_ingress_allocator_executes_via_chain_model(self, smk_billing_line: BillingLineItem) -> None:
        """handler.get_allocator('SELF_KAFKA_NETWORK_INGRESS')(ctx) returns AllocationResult."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        allocator = handler.get_allocator("SELF_KAFKA_NETWORK_INGRESS")

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        metrics_data = {
            "bytes_in_per_principal": [
                make_metric_row(600, "User:alice"),
                make_metric_row(400, "User:bob"),
            ]
        }
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"), metrics_data)

        result = allocator(ctx)

        assert isinstance(result, AllocationResult)
        assert sum(r.amount for r in result.rows) == Decimal("100")
        row_by_id = {r.identity_id: r for r in result.rows}
        assert set(row_by_id.keys()) == {"User:alice", "User:bob"}
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)


# ---------------------------------------------------------------------------
# Test 12: SMK_INFRA_MODEL — 3-tier ChainModel for COMPUTE and STORAGE
# ---------------------------------------------------------------------------


class TestSmkInfraModel:
    """SMK_INFRA_MODEL: 3-tier ChainModel for COMPUTE and STORAGE product types."""

    def test_tier0_metrics_derived_present(self, smk_billing_line: BillingLineItem) -> None:
        """metrics_derived=[alice, bob] → even split, chain_tier=0, CostType.USAGE."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL

        resolution = make_resolution(
            metrics_derived=make_identity_set("User:alice", "User:bob"),
        )
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"))
        result = SMK_INFRA_MODEL(ctx)
        assert len(result.rows) == 2
        assert all(r.metadata["chain_tier"] == 0 for r in result.rows)
        assert all(r.cost_type == CostType.USAGE for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_tier1_resource_active_fallback(self, smk_billing_line: BillingLineItem) -> None:
        """metrics_derived=[], resource_active=[alice] → chain_tier=1, NO_ACTIVE_IDENTITIES_LOCATED."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL

        resolution = make_resolution(resource_active=make_identity_set("User:alice"))
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"))
        result = SMK_INFRA_MODEL(ctx)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "User:alice"
        assert result.rows[0].metadata["chain_tier"] == 1
        assert result.rows[0].allocation_detail == AllocationDetail.NO_ACTIVE_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED

    def test_tier2_terminal_unallocated(self, smk_billing_line: BillingLineItem) -> None:
        """All sets empty → UNALLOCATED, chain_tier=2, NO_IDENTITIES_LOCATED."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL

        resolution = make_resolution()
        ctx = make_ctx(smk_billing_line, resolution, Decimal("100"))
        result = SMK_INFRA_MODEL(ctx)
        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].metadata["chain_tier"] == 2
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].cost_type == CostType.SHARED

    def test_compute_and_storage_share_infra_model(self) -> None:
        """get_allocator returns SMK_INFRA_MODEL for both COMPUTE and STORAGE."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        assert handler.get_allocator("SELF_KAFKA_COMPUTE") is SMK_INFRA_MODEL
        assert handler.get_allocator("SELF_KAFKA_STORAGE") is SMK_INFRA_MODEL
        assert isinstance(handler.get_allocator("SELF_KAFKA_COMPUTE"), ChainModel)

    def test_network_ingress_model_unaffected(self) -> None:
        """get_allocator('SELF_KAFKA_NETWORK_INGRESS') returns SMK_INGRESS_MODEL, not SMK_INFRA_MODEL."""
        from plugins.self_managed_kafka.allocation_models import SMK_INFRA_MODEL, SMK_INGRESS_MODEL
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        assert handler.get_allocator("SELF_KAFKA_NETWORK_INGRESS") is SMK_INGRESS_MODEL
        assert handler.get_allocator("SELF_KAFKA_NETWORK_INGRESS") is not SMK_INFRA_MODEL

    def test_get_allocator_raises_for_unknown_product_type(self) -> None:
        """get_allocator raises ValueError for unknown product type."""
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        config = make_smk_config()
        handler = SelfManagedKafkaHandler(config=config, metrics_source=MagicMock())
        with pytest.raises(ValueError, match="Unknown product type"):
            handler.get_allocator("UNKNOWN_TYPE")
