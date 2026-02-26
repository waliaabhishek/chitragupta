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


class TestNetworkAllocator:
    def test_usage_ratio_with_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        billing_line = BillingLineItem(**{**base_billing_line.__dict__, "product_type": "SELF_KAFKA_NETWORK_INGRESS"})
        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 700.0),
                make_network_row("bytes_in_per_principal", "User:bob", 300.0),
            ],
            "bytes_out_per_principal": [],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount > bob_amount

    def test_fallback_to_even_split_when_no_metrics(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(base_billing_line, resolution, metrics_data=None)

        result = self_kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_fallback_to_even_split_when_zero_usage(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 0.0),
            ],
            "bytes_out_per_principal": [],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(base_billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_allocator(ctx)

        # Falls back to even split since no non-zero usage
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        bob_amount = sum(r.amount for r in result.rows if r.identity_id == "User:bob")
        assert alice_amount == bob_amount

    def test_no_identities_goes_to_unallocated(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        resolution = make_resolution()
        ctx = make_ctx(base_billing_line, resolution, metrics_data=None)

        result = self_kafka_network_allocator(ctx)

        assert result.rows[0].identity_id == "UNALLOCATED"

    def test_total_preserved_with_remainder(self, base_billing_line):
        """Remainder distribution ensures total is always preserved."""
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 333.0),
                make_network_row("bytes_in_per_principal", "User:bob", 333.0),
                make_network_row("bytes_in_per_principal", "User:charlie", 334.0),
            ],
            "bytes_out_per_principal": [],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob", "User:charlie"))
        ctx = make_ctx(base_billing_line, resolution, metrics_data=metrics_data, amount=Decimal("10"))

        result = self_kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("10")

    def test_bytes_in_and_out_summed_per_principal(self, base_billing_line):
        from plugins.self_managed_kafka.allocators.kafka_allocators import self_kafka_network_allocator

        metrics_data = {
            "bytes_in_per_principal": [
                make_network_row("bytes_in_per_principal", "User:alice", 400.0),
                make_network_row("bytes_in_per_principal", "User:bob", 100.0),
            ],
            "bytes_out_per_principal": [
                make_network_row("bytes_out_per_principal", "User:alice", 400.0),
                make_network_row("bytes_out_per_principal", "User:bob", 100.0),
            ],
        }
        resolution = make_resolution(metrics_derived=make_identity_set("User:alice", "User:bob"))
        ctx = make_ctx(base_billing_line, resolution, metrics_data=metrics_data)

        result = self_kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        alice_amount = sum(r.amount for r in result.rows if r.identity_id == "User:alice")
        # alice has 80% of total (800/1000), bob has 20%
        assert alice_amount == Decimal("80")
