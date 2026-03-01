"""Tests for self-managed Kafka cost allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import (
    BillingLineItem,
    Identity,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)


@pytest.fixture
def base_billing_line() -> BillingLineItem:
    return BillingLineItem(
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
    return Identity(
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


class TestComputeAllocator:
    def test_even_split_two_identities(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_compute_allocator

        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(base_billing_line, resolution)

        result = self_kafka_compute_allocator(ctx)

        assert isinstance(result, AllocationResult)
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        assert len(result.rows) == 2

    def test_single_identity_gets_full_amount(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_compute_allocator

        resolution = make_resolution(resource_active=make_identity_set("User:alice"))
        ctx = make_ctx(base_billing_line, resolution)

        result = self_kafka_compute_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].amount == Decimal("100")
        assert result.rows[0].identity_id == "User:alice"

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_compute_allocator

        resolution = make_resolution()
        ctx = make_ctx(base_billing_line, resolution)

        result = self_kafka_compute_allocator(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_uses_tenant_period_fallback(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_compute_allocator

        resolution = make_resolution(tenant_period=make_identity_set("User:charlie"))
        ctx = make_ctx(base_billing_line, resolution)

        result = self_kafka_compute_allocator(ctx)

        assert result.rows[0].identity_id == "User:charlie"


class TestStorageAllocator:
    def test_even_split_two_identities(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_storage_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_STORAGE"})
        resolution = make_resolution(resource_active=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution)

        result = self_kafka_storage_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        assert len(result.rows) == 2

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_storage_allocator

        resolution = make_resolution()
        ctx = make_ctx(base_billing_line, resolution)

        result = self_kafka_storage_allocator(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"


class TestNetworkIngressAllocator:
    def test_usage_ratio_with_bytes_in_only(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 700.0),
                make_network_row("bytes_in_per_principal", "User:bob", 300.0),
            ],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_ingress_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == Decimal("70")
        assert bob_amount == Decimal("30")

    def test_fallback_to_even_split_when_no_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = self_kafka_network_ingress_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_fallback_to_even_split_when_zero_usage(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 0.0),
                make_network_row("bytes_in_per_principal", "User:bob", 0.0),
            ],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_ingress_allocator(ctx)

        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
        resolution = make_resolution()
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = self_kafka_network_ingress_allocator(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_directional_allocation_ingress_vs_egress(self, base_billing_line):
        """Principal A sends 100 bytes in / 0 out; B sends 0 in / 100 out.
        Ingress allocator → 100% A.
        """
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_ingress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
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
        resolution = make_resolution(metrics_derived=make_identity_set("A", "B"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        ingress_result = self_kafka_network_ingress_allocator(ctx)
        ingress_a = sum(r.amount for r in ingress_result.rows if r.identity_id == "A")
        ingress_b = sum(r.amount for r in ingress_result.rows if r.identity_id == "B")

        assert ingress_a == Decimal("100")
        assert ingress_b == Decimal("0")


class TestNetworkEgressAllocator:
    def test_usage_ratio_with_bytes_out_only(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_egress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"})
        metrics_data = {
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "User:alice", 700.0),
                make_network_row("bytes_out_per_principal", "User:bob", 300.0),
            ],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_egress_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == Decimal("70")
        assert bob_amount == Decimal("30")

    def test_fallback_to_even_split_when_no_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_egress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"})
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = self_kafka_network_egress_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_fallback_to_even_split_when_zero_usage(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_egress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"})
        metrics_data = {
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "User:alice", 0.0),
                make_network_row("bytes_out_per_principal", "User:bob", 0.0),
            ],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_egress_allocator(ctx)

        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_egress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"})
        resolution = make_resolution()
        ctx = make_ctx(billing_line, resolution, metrics_data=None)

        result = self_kafka_network_egress_allocator(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_directional_allocation_ingress_vs_egress(self, base_billing_line):
        """Principal A sends 100 bytes in / 0 out; B sends 0 in / 100 out.
        Egress allocator → 100% B.
        """
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_egress_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_EGRESS"})
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
        resolution = make_resolution(metrics_derived=make_identity_set("A", "B"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        egress_result = self_kafka_network_egress_allocator(ctx)
        egress_a = sum(r.amount for r in egress_result.rows if r.identity_id == "A")
        egress_b = sum(r.amount for r in egress_result.rows if r.identity_id == "B")

        assert egress_a == Decimal("0")
        assert egress_b == Decimal("100")
