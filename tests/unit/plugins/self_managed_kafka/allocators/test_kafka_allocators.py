"""Tests for self-managed Kafka cost allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

from core.engine.allocation import AllocationContext
from core.models import (
    BillingLineItem,
    CoreIdentity,
    Identity,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)
from core.models.billing import CoreBillingLineItem


@pytest.fixture
def base_billing_line() -> BillingLineItem:
    return CoreBillingLineItem(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        resource_id="kafka-cluster-001",
        product_category="kafka",
        product_type="SELF_KAFKA_COMPUTE",
        quantity=Decimal("72"),
        unit_price=Decimal("0.10"),
        total_cost=Decimal("7.20"),
    )


def make_identity(identity_id: str) -> Identity:
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


def make_ctx(
    billing_line: BillingLineItem,
    resolution: IdentityResolution,
    metrics_data: dict | None = None,
    amount: Decimal = Decimal("100"),
) -> AllocationContext:
    return AllocationContext(
        timeslice=billing_line.timestamp,
        billing_line=billing_line,
        identities=resolution,
        split_amount=amount,
        metrics_data=metrics_data,
        params={},
    )


def make_network_row(key: str, principal: str, value: float) -> MetricRow:
    return MetricRow(
        timestamp=datetime(2026, 2, 1, tzinfo=UTC),
        metric_key=key,
        value=value,
        labels={"principal": principal},
    )


class TestNetworkIngressAllocator:
    def test_usage_ratio_with_bytes_in_only(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 700.0),
                make_network_row("bytes_in_per_principal", "User:bob", 300.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == Decimal("70")
        assert bob_amount == Decimal("30")

    def test_fallback_to_even_split_when_no_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = SMK_INGRESS_MODEL(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_fallback_to_even_split_when_zero_usage(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 0.0),
                make_network_row("bytes_in_per_principal", "User:bob", 0.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = SMK_INGRESS_MODEL(ctx)

        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        resolution = make_resolution()
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = SMK_INGRESS_MODEL(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_directional_allocation_ingress_vs_egress(self, base_billing_line):
        """Principal A sends 100 bytes in / 0 out; B sends 0 in / 100 out.
        Ingress allocator → 100% A.
        """
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "A", 100.0),
                make_network_row("bytes_in_per_principal", "B", 0.0),
            ],
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "A", 0.0),
                make_network_row("bytes_out_per_principal", "B", 100.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("A", "B"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        ingress_result = SMK_INGRESS_MODEL(ctx)
        ingress_a = sum(r.amount for r in ingress_result.rows if r.identity_id == "A")
        ingress_b = sum(r.amount for r in ingress_result.rows if r.identity_id == "B")

        assert ingress_a == Decimal("100")
        assert ingress_b == Decimal("0")


class TestNetworkEgressAllocator:
    def test_usage_ratio_with_bytes_out_only(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        metrics_data = {
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "User:alice", 700.0),
                make_network_row("bytes_out_per_principal", "User:bob", 300.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = SMK_EGRESS_MODEL(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == Decimal("70")
        assert bob_amount == Decimal("30")

    def test_fallback_to_even_split_when_no_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = SMK_EGRESS_MODEL(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_fallback_to_even_split_when_zero_usage(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        metrics_data = {
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "User:alice", 0.0),
                make_network_row("bytes_out_per_principal", "User:bob", 0.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = SMK_EGRESS_MODEL(ctx)

        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        resolution = make_resolution()
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = SMK_EGRESS_MODEL(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_directional_allocation_ingress_vs_egress(self, base_billing_line):
        """Principal A sends 100 bytes in / 0 out; B sends 0 in / 100 out.
        Egress allocator → 100% B.
        """
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "A", 100.0),
                make_network_row("bytes_in_per_principal", "B", 0.0),
            ],
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "A", 0.0),
                make_network_row("bytes_out_per_principal", "B", 100.0),
            ],
        }
        resolution = make_resolution(resource_active=make_identity_set("A", "B"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        egress_result = SMK_EGRESS_MODEL(ctx)
        egress_a = sum(r.amount for r in egress_result.rows if r.identity_id == "A")
        egress_b = sum(r.amount for r in egress_result.rows if r.identity_id == "B")

        assert egress_a == Decimal("0")
        assert egress_b == Decimal("100")


class TestTask024AllocatorRemoval:
    """TASK-024: self_kafka_compute_allocator and self_kafka_storage_allocator must not be importable after fix."""

    def test_self_kafka_compute_allocator_not_importable(self) -> None:
        with pytest.raises(ImportError):
            from plugins.self_managed_kafka.allocators.kafka_allocators import (  # noqa: F401
                self_kafka_compute_allocator,
            )

    def test_self_kafka_storage_allocator_not_importable(self) -> None:
        with pytest.raises(ImportError):
            from plugins.self_managed_kafka.allocators.kafka_allocators import (  # noqa: F401
                self_kafka_storage_allocator,
            )


@pytest.fixture
def smk_config() -> SelfManagedKafkaConfig:
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

    return SelfManagedKafkaConfig.model_validate(
        {
            "cluster_id": "test-cluster",
            "broker_count": 3,
            "cost_model": {
                "compute_hourly_rate": "1.00",
                "storage_per_gib_hourly": "0.01",
                "network_ingress_per_gib": "0.05",
                "network_egress_per_gib": "0.05",
            },
            "metrics": {"url": "http://prom:9090"},
        }
    )


class TestTask024HandlerAllocatorIdentity:
    """TASK-024: handler.get_allocator must return allocate_evenly_with_fallback directly for COMPUTE/STORAGE."""

    def test_compute_allocator_is_allocate_evenly_with_fallback(self, smk_config) -> None:
        from unittest.mock import MagicMock

        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(config=smk_config, metrics_source=MagicMock())
        assert handler.get_allocator("SELF_KAFKA_COMPUTE") is allocate_evenly_with_fallback

    def test_storage_allocator_is_allocate_evenly_with_fallback(self, smk_config) -> None:
        from unittest.mock import MagicMock

        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler

        handler = SelfManagedKafkaHandler(config=smk_config, metrics_source=MagicMock())
        assert handler.get_allocator("SELF_KAFKA_STORAGE") is allocate_evenly_with_fallback


class TestTask024NetworkFallbackParity:
    """TASK-024: Network allocators fallback to even split when metrics absent."""

    def test_network_ingress_fallback_matches_allocate_evenly_with_fallback(self, base_billing_line) -> None:
        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        network_result = SMK_INGRESS_MODEL(ctx)
        fallback_result = allocate_evenly_with_fallback(ctx)

        assert {r.identity_id for r in network_result.rows} == {r.identity_id for r in fallback_result.rows}
        assert sum(r.amount for r in network_result.rows) == sum(r.amount for r in fallback_result.rows)

    def test_network_egress_fallback_matches_allocate_evenly_with_fallback(self, base_billing_line) -> None:
        from core.engine.helpers import allocate_evenly_with_fallback
        from plugins.self_managed_kafka.allocation_models import SMK_EGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"}
        )
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        network_result = SMK_EGRESS_MODEL(ctx)
        fallback_result = allocate_evenly_with_fallback(ctx)

        assert {r.identity_id for r in network_result.rows} == {r.identity_id for r in fallback_result.rows}
        assert sum(r.amount for r in network_result.rows) == sum(r.amount for r in fallback_result.rows)


class TestGap23TenantPeriodFallback:
    """GAP-23: resource_active fallback splits evenly across static identities."""

    def test_network_fallback_splits_resource_active_identities(self, base_billing_line: BillingLineItem) -> None:
        """Network fallback: splits evenly across resource_active when no metrics available.

        SMK Tier 1 uses resource_active (static config identities), not tenant_period.
        """
        from plugins.self_managed_kafka.allocation_models import SMK_INGRESS_MODEL

        billing_line = CoreBillingLineItem(
            **{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"}
        )
        ra = make_identity_set("User:alice", "User:bob")
        # No metrics_data, resource_active set → Tier 1 even split
        resolution = make_resolution(resource_active=ra)
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = SMK_INGRESS_MODEL(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert recipient_ids == {"User:alice", "User:bob"}
        assert len(result.rows) == 2
