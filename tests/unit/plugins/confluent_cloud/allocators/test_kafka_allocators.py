"""Tests for Kafka allocators."""

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
    """Standard Kafka billing line for tests."""
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


@pytest.fixture
def identity_set_two() -> IdentitySet:
    """IdentitySet with two service accounts."""
    iset = IdentitySet()
    iset.add(
        Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )
    )
    iset.add(
        Identity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-2",
            identity_type="service_account",
        )
    )
    return iset


class TestKafkaNumCkuAllocator:
    """Tests for kafka_num_cku_allocator (hybrid usage/shared)."""

    def test_default_ratios_with_usage(self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet) -> None:
        """Default 70/30 split: 70% usage-based, 30% shared."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        # sa-1 has 70% usage, sa-2 has 30%
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    700.0,
                    {"principal_id": "sa-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    300.0,
                    {"principal_id": "sa-2"},
                ),
            ],
            "bytes_out": [],
        }
        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_num_cku_allocator(ctx)

        assert isinstance(result, AllocationResult)
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # Hybrid 70/30: 2 usage rows (sa-1, sa-2) + 2 shared rows (sa-1, sa-2) = 4
        assert len(result.rows) == 4

    def test_custom_ratios(self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet) -> None:
        """Custom ratios from params override defaults."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    1000.0,
                    {"principal_id": "sa-1"},
                ),
            ],
            "bytes_out": [],
        }
        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={"kafka_cku_usage_ratio": 0.50, "kafka_cku_shared_ratio": 0.50},
        )

        result = kafka_num_cku_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")

    def test_no_metrics_fallback_to_even_split(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Without metrics, falls back to even split for usage portion."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_num_cku_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # Should still allocate to both identities
        identity_ids = {row.identity_id for row in result.rows}
        assert "sa-1" in identity_ids
        assert "sa-2" in identity_ids

    def test_no_identities_unallocated(self, base_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_num_cku_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # All rows should be UNALLOCATED
        assert all(row.identity_id == "UNALLOCATED" for row in result.rows)

    def test_single_identity_gets_full_amount(self, base_billing_line: BillingLineItem) -> None:
        """Single identity gets 100% of allocation."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        single_identity = IdentitySet()
        single_identity.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-solo",
                identity_type="service_account",
            )
        )

        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    1000.0,
                    {"principal_id": "sa-solo"},
                ),
            ],
            "bytes_out": [],
        }
        resolution = IdentityResolution(
            resource_active=single_identity,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_num_cku_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # All rows to sa-solo
        assert all(row.identity_id == "sa-solo" for row in result.rows)


class TestKafkaNetworkAllocator:
    """Tests for kafka_network_allocator (pure usage-based)."""

    def test_with_metrics_usage_ratio(self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet) -> None:
        """Allocates by bytes in/out ratio."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )

        # sa-1: 80%, sa-2: 20%
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    400.0,
                    {"principal_id": "sa-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    100.0,
                    {"principal_id": "sa-2"},
                ),
            ],
            "bytes_out": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_out",
                    400.0,
                    {"principal_id": "sa-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_out",
                    100.0,
                    {"principal_id": "sa-2"},
                ),
            ],
        }
        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # sa-1 should get more
        sa1_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-1")
        sa2_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-2")
        assert sa1_amount > sa2_amount

    def test_no_metrics_even_split_fallback(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Without metrics, falls back to even split."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )

        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_network_allocator(ctx)

        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")
        # Even split
        sa1_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-1")
        sa2_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-2")
        assert sa1_amount == sa2_amount == Decimal("50")

    def test_no_identities_unallocated(self, base_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert all(row.identity_id == "UNALLOCATED" for row in result.rows)

    def test_zero_usage_even_split_fallback(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Metrics present but all zero usage -> even split."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_network_allocator,
        )

        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    0.0,
                    {"principal_id": "sa-1"},
                ),
            ],
            "bytes_out": [],
        }
        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        # Should fall back to even split
        sa1_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-1")
        sa2_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-2")
        assert sa1_amount == sa2_amount


class TestKafkaBaseAllocator:
    """Tests for kafka_base_allocator (even split)."""

    def test_even_split_two_identities(self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet) -> None:
        """Even split across two identities."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )

        resolution = IdentityResolution(
            resource_active=identity_set_two,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_base_allocator(ctx)

        assert len(result.rows) == 2
        assert all(row.amount == Decimal("50") for row in result.rows)
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")

    def test_no_identities_unallocated(self, base_billing_line: BillingLineItem) -> None:
        """Without identities, allocates to UNALLOCATED."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )

        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_base_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "UNALLOCATED"
        assert result.rows[0].amount == Decimal("100")

    def test_uses_tenant_period_fallback(self, base_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period when merged_active is empty."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )

        tenant_period = IdentitySet()
        tenant_period.add(
            Identity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-tenant",
                identity_type="service_account",
            )
        )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tenant_period,
        )
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=None,
            params={},
        )

        result = kafka_base_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-tenant"
        assert result.rows[0].amount == Decimal("100")
