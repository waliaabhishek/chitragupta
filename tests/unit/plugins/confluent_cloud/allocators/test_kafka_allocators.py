"""Tests for Kafka allocators."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import (
    BillingLineItem,
    CoreIdentity,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)
from core.models.billing import CoreBillingLineItem


@pytest.fixture
def base_billing_line() -> BillingLineItem:
    """Standard Kafka billing line for tests."""
    return CoreBillingLineItem(
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
        CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-1",
            identity_type="service_account",
        )
    )
    iset.add(
        CoreIdentity(
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
        """Without identities, allocates to the billing resource."""
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
        # All rows go to the resource (no identities found)
        assert all(row.identity_id == base_billing_line.resource_id for row in result.rows)

    def test_single_identity_gets_full_amount(self, base_billing_line: BillingLineItem) -> None:
        """Single identity gets 100% of allocation."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_num_cku_allocator,
        )

        single_identity = IdentitySet()
        single_identity.add(
            CoreIdentity(
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
        """Without identities, allocates to the billing resource."""
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

        assert sum(row.amount for row in result.rows) == Decimal("100")
        assert all(row.identity_id == base_billing_line.resource_id for row in result.rows)

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
        assert sum(row.amount for row in result.rows) == Decimal("100")
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
        """Without identities, allocates to the billing resource."""
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
        assert result.rows[0].identity_id == base_billing_line.resource_id
        assert result.rows[0].amount == Decimal("100")

    def test_uses_tenant_period_fallback(self, base_billing_line: BillingLineItem) -> None:
        """Falls back to tenant_period when merged_active is empty."""
        from plugins.confluent_cloud.allocators.kafka_allocators import (
            kafka_base_allocator,
        )

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
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

    def test_tenant_period_splits_evenly_across_real_identities(self, base_billing_line: BillingLineItem) -> None:
        """GAP-23: tenant_period fallback splits evenly across all real identities when no metrics."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_base_allocator

        tp = IdentitySet()
        for sa_id in ("sa-1", "sa-2"):
            tp.add(
                CoreIdentity(
                    ecosystem="confluent_cloud",
                    tenant_id="org-123",
                    identity_id=sa_id,
                    identity_type="service_account",
                )
            )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tp,
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

        recipient_ids = {r.identity_id for r in result.rows}
        assert recipient_ids == {"sa-1", "sa-2"}
        assert len(result.rows) == 2


class TestKafkaNetworkAllocatorTieredFallback:
    """Tests for tiered fallback branches in kafka_network_allocator and related allocators.

    Verifies that each decision branch produces the correct distinct allocation_detail
    value and routes to the right allocation method.
    """

    def test_t1_usage_ratio_with_positive_bytes(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """T1: bytes_in/bytes_out with value > 0 → USAGE_RATIO_ALLOCATION, proportional amounts."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 600.0, {"principal_id": "sa-1"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 200.0, {"principal_id": "sa-2"}),
            ],
            "bytes_out": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 150.0, {"principal_id": "sa-1"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 50.0, {"principal_id": "sa-2"}),
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

        assert len(result.rows) == 2
        assert all(r.allocation_detail == AllocationDetail.USAGE_RATIO_ALLOCATION for r in result.rows)
        assert all(r.allocation_method == "usage_ratio" for r in result.rows)
        sa1_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-1")
        sa2_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-2")
        assert sa1_amount > sa2_amount
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_t2_b5_no_metrics_merged_active_nonempty(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """T2-B5: metrics_data=None, merged_active non-empty → NO_METRICS_LOCATED, even split."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

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

        assert len(result.rows) == 2
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_t2_b6_no_metrics_merged_empty_tenant_nonempty(self, base_billing_line: BillingLineItem) -> None:
        """T2-B6: metrics_data=None, merged_active empty, tenant_period non-empty.

        → NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED.
        """
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
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

        result = kafka_network_allocator(ctx)

        assert len(result.rows) == 1
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.identity_id != "UNALLOCATED" for r in result.rows)

    def test_t2_b7_no_metrics_all_empty_allocates_to_resource(self, base_billing_line: BillingLineItem) -> None:
        """T2-B7: metrics_data=None, all identity sets empty → identity_id == resource_id, NO_IDENTITIES_LOCATED."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

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

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == base_billing_line.resource_id  # "lkc-abc", NOT "UNALLOCATED"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].allocation_method == "to_resource"
        assert result.rows[0].amount == Decimal("100")

    def test_t3_b9_zero_usage_merged_active_nonempty(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """T3-B9: metrics present but value=0, merged_active non-empty.

        → NO_METRICS_PRESENT_MERGED_IDENTITIES_LOCATED.
        """
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 0.0, {"principal_id": "sa-1"}),
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

        assert len(result.rows) == 2
        assert all(
            r.allocation_detail == AllocationDetail.NO_METRICS_PRESENT_MERGED_IDENTITIES_LOCATED for r in result.rows
        )
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_t3_b10_zero_usage_merged_empty_tenant_nonempty(self, base_billing_line: BillingLineItem) -> None:
        """T3-B10: zero-usage metrics, merged_active empty, tenant_period non-empty.

        → NO_METRICS_PRESENT_PENALTY_ALLOCATION_FOR_EVERYONE.
        """
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-tenant",
                identity_type="service_account",
            )
        )
        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 0.0, {"principal_id": "sa-1"}),
            ],
            "bytes_out": [],
        }
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
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert len(result.rows) == 1
        assert all(
            r.allocation_detail == AllocationDetail.NO_METRICS_PRESENT_PENALTY_ALLOCATION_FOR_EVERYONE
            for r in result.rows
        )
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")
        assert all(r.identity_id != "UNALLOCATED" for r in result.rows)

    def test_t3_b11_zero_usage_all_empty_allocates_to_resource(self, base_billing_line: BillingLineItem) -> None:
        """T3-B11: zero-usage metrics, all identity sets empty → identity_id == resource_id, NO_IDENTITIES_LOCATED."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 0.0, {"principal_id": "sa-1"}),
            ],
            "bytes_out": [],
        }
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
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == base_billing_line.resource_id  # NOT "UNALLOCATED"
        assert result.rows[0].allocation_detail == AllocationDetail.NO_IDENTITIES_LOCATED
        assert result.rows[0].allocation_method == "to_resource"
        assert result.rows[0].amount == Decimal("100")

    def test_edge_empty_dict_metrics_routes_to_fallback_no_metrics(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Edge: metrics_data={} (falsy) → routes to _fallback_no_metrics, same as T2-B5."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

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
            metrics_data={},
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_edge_empty_lists_metrics_routes_to_fallback_no_metrics(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Edge: metrics_data with empty lists (truthy dict, no rows) → _fallback_no_metrics."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

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
            metrics_data={"bytes_in": [], "bytes_out": []},
            params={},
        )

        result = kafka_network_allocator(ctx)

        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_edge_no_principal_label_routes_to_fallback_zero_usage(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """Edge: metrics rows present but no principal_id label → _fallback_zero_usage (T3-B9 path)."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 500.0, {}),  # no principal_id
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

        # No principal_id → identity_bytes empty → zero-usage branch (T3-B9)
        assert all(
            r.allocation_detail == AllocationDetail.NO_METRICS_PRESENT_MERGED_IDENTITIES_LOCATED for r in result.rows
        )
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_kafka_base_allocator_routes_through_fallback_no_metrics(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """kafka_base_allocator: routes through _fallback_no_metrics (NO_METRICS_LOCATED, not EVEN_SPLIT_ALLOCATION)."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_base_allocator

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
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in result.rows)
        assert all(r.allocation_method == "even_split" for r in result.rows)
        assert sum(r.amount for r in result.rows) == Decimal("100")

    def test_kafka_num_cku_allocator_shared_half_uses_fallback_no_metrics(
        self, base_billing_line: BillingLineItem, identity_set_two: IdentitySet
    ) -> None:
        """kafka_num_cku_allocator: shared half uses _fallback_no_metrics, not _kafka_shared_allocation."""
        from core.models.chargeback import AllocationDetail
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_num_cku_allocator

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

        # Both halves go through _fallback_no_metrics when metrics=None:
        # all even-split rows must carry NO_METRICS_LOCATED (not EVEN_SPLIT_ALLOCATION)
        even_split_rows = [r for r in result.rows if r.allocation_method == "even_split"]
        assert len(even_split_rows) > 0
        assert all(r.allocation_detail == AllocationDetail.NO_METRICS_LOCATED for r in even_split_rows)


class TestKafkaNetworkDirectionAllocation:
    """GAP-14: Network READ/WRITE allocators must use direction-specific bytes.

    Principal A = 90% write (bytes_in), 10% read (bytes_out).
    Principal B = 10% write, 90% read.
    KAFKA_NETWORK_WRITE should allocate 90% to A, 10% to B (bytes_in only).
    KAFKA_NETWORK_READ should allocate 10% to A, 90% to B (bytes_out only).
    KAFKA_NUM_CKU should blend both → 50/50 when total bytes are equal.
    """

    @pytest.fixture
    def asymmetric_billing_line(self) -> BillingLineItem:
        """Billing line for direction tests (KAFKA_NETWORK_WRITE as base; overridden per test)."""
        return CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            resource_id="lkc-abc",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            quantity=Decimal("1"),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )

    @pytest.fixture
    def asymmetric_identity_set(self) -> IdentitySet:
        """Two service accounts: sa-A (heavy writer) and sa-B (heavy reader)."""
        iset = IdentitySet()
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-A",
                identity_type="service_account",
            )
        )
        iset.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-B",
                identity_type="service_account",
            )
        )
        return iset

    def test_network_write_uses_bytes_in_only(
        self, asymmetric_billing_line: BillingLineItem, asymmetric_identity_set: IdentitySet
    ) -> None:
        """KAFKA_NETWORK_WRITE with only bytes_in: sa-A (90%) gets 90%, sa-B (10%) gets 10%."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        # Only bytes_in is provided (as the fixed handler will supply for WRITE)
        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 900.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 100.0, {"principal_id": "sa-B"}),
            ],
        }
        resolution = IdentityResolution(
            resource_active=asymmetric_identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=asymmetric_billing_line.timestamp,
            billing_line=asymmetric_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")
        sa_a_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-A")
        sa_b_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-B")
        # sa-A wrote 90% → gets 90%
        assert sa_a_amount == Decimal("90")
        assert sa_b_amount == Decimal("10")

    def test_network_read_uses_bytes_out_only(
        self, asymmetric_billing_line: BillingLineItem, asymmetric_identity_set: IdentitySet
    ) -> None:
        """KAFKA_NETWORK_READ with only bytes_out: sa-A (10%) gets 10%, sa-B (90%) gets 90%."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        # Only bytes_out is provided (as the fixed handler will supply for READ)
        metrics_data = {
            "bytes_out": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 100.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 900.0, {"principal_id": "sa-B"}),
            ],
        }
        resolution = IdentityResolution(
            resource_active=asymmetric_identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=asymmetric_billing_line.timestamp,
            billing_line=asymmetric_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")
        sa_a_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-A")
        sa_b_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-B")
        # sa-A read 10% → gets 10%
        assert sa_a_amount == Decimal("10")
        assert sa_b_amount == Decimal("90")

    def test_network_write_read_ratios_are_inverted(
        self, asymmetric_billing_line: BillingLineItem, asymmetric_identity_set: IdentitySet
    ) -> None:
        """With asymmetric traffic, WRITE and READ allocations are directional inverses."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        write_metrics = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 900.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 100.0, {"principal_id": "sa-B"}),
            ],
        }
        read_metrics = {
            "bytes_out": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 100.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 900.0, {"principal_id": "sa-B"}),
            ],
        }
        resolution = IdentityResolution(
            resource_active=asymmetric_identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        write_ctx = AllocationContext(
            timeslice=asymmetric_billing_line.timestamp,
            billing_line=asymmetric_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=write_metrics,
            params={},
        )
        read_ctx = AllocationContext(
            timeslice=asymmetric_billing_line.timestamp,
            billing_line=asymmetric_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=read_metrics,
            params={},
        )

        write_result = kafka_network_allocator(write_ctx)
        read_result = kafka_network_allocator(read_ctx)

        write_a = sum(r.amount for r in write_result.rows if r.identity_id == "sa-A")
        write_b = sum(r.amount for r in write_result.rows if r.identity_id == "sa-B")
        read_a = sum(r.amount for r in read_result.rows if r.identity_id == "sa-A")
        read_b = sum(r.amount for r in read_result.rows if r.identity_id == "sa-B")

        # Ratios are inverted: WRITE sa-A > WRITE sa-B, READ sa-A < READ sa-B
        assert write_a > write_b
        assert read_a < read_b
        # sa-A write share + sa-A read share != sa-B write share + sa-B read share
        # (asymmetric, not equal)
        assert write_a == read_b  # 90% in both cases (mirror)
        assert write_b == read_a  # 10% in both cases (mirror)

    def test_cku_blends_both_directions_equal_split(
        self, asymmetric_billing_line: BillingLineItem, asymmetric_identity_set: IdentitySet
    ) -> None:
        """KAFKA_NUM_CKU blends bytes_in + bytes_out: equal totals → 50/50 split."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_num_cku_allocator

        cku_line = CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=asymmetric_billing_line.timestamp,
            resource_id="lkc-abc",
            product_category="KAFKA",
            product_type="KAFKA_NUM_CKU",
            quantity=Decimal("1"),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )
        # sa-A: 90% write + 10% read = 100 total
        # sa-B: 10% write + 90% read = 100 total
        # Combined bytes: sa-A = 100, sa-B = 100 → 50/50
        metrics_data = {
            "bytes_in": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 900.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_in", 100.0, {"principal_id": "sa-B"}),
            ],
            "bytes_out": [
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 100.0, {"principal_id": "sa-A"}),
                MetricRow(datetime(2026, 2, 1, tzinfo=UTC), "bytes_out", 900.0, {"principal_id": "sa-B"}),
            ],
        }
        resolution = IdentityResolution(
            resource_active=asymmetric_identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )
        ctx = AllocationContext(
            timeslice=cku_line.timestamp,
            billing_line=cku_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_num_cku_allocator(ctx)

        total = sum(r.amount for r in result.rows)
        assert total == Decimal("100")
        sa_a_total = sum(r.amount for r in result.rows if r.identity_id == "sa-A")
        sa_b_total = sum(r.amount for r in result.rows if r.identity_id == "sa-B")
        # Both principals have equal total bytes → equal allocation
        assert sa_a_total == sa_b_total


class TestKafkaNetworkDirectionRegressionFixture:
    """GAP-14 regression: direction split must be exact for clear-cut asymmetric cases."""

    def test_asymmetric_direction_regression_fixture(self) -> None:
        """Regression fixture: A writes 100 bytes, B reads 100 bytes.

        KAFKA_NETWORK_WRITE (bytes_in only): 100% to A, 0% to B.
        KAFKA_NETWORK_READ (bytes_out only): 0% to A, 100% to B.
        """
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        ts = datetime(2026, 2, 1, tzinfo=UTC)

        identity_set = IdentitySet()
        identity_set.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-writer",
                identity_type="service_account",
            )
        )
        identity_set.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-reader",
                identity_type="service_account",
            )
        )

        write_line = CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=ts,
            resource_id="lkc-abc",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_WRITE",
            quantity=Decimal("1"),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )
        read_line = CoreBillingLineItem(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            timestamp=ts,
            resource_id="lkc-abc",
            product_category="KAFKA",
            product_type="KAFKA_NETWORK_READ",
            quantity=Decimal("1"),
            unit_price=Decimal("100"),
            total_cost=Decimal("100"),
        )

        resolution = IdentityResolution(
            resource_active=identity_set,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
        )

        # WRITE: only bytes_in — sa-writer sent 100, sa-reader sent 0
        write_ctx = AllocationContext(
            timeslice=ts,
            billing_line=write_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data={
                "bytes_in": [
                    MetricRow(ts, "bytes_in", 100.0, {"principal_id": "sa-writer"}),
                    MetricRow(ts, "bytes_in", 0.0, {"principal_id": "sa-reader"}),
                ],
            },
            params={},
        )

        # READ: only bytes_out — sa-writer received 0, sa-reader received 100
        read_ctx = AllocationContext(
            timeslice=ts,
            billing_line=read_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data={
                "bytes_out": [
                    MetricRow(ts, "bytes_out", 0.0, {"principal_id": "sa-writer"}),
                    MetricRow(ts, "bytes_out", 100.0, {"principal_id": "sa-reader"}),
                ],
            },
            params={},
        )

        write_result = kafka_network_allocator(write_ctx)
        read_result = kafka_network_allocator(read_ctx)

        # WRITE: sa-writer gets 100%, sa-reader gets 0%
        write_writer = sum(r.amount for r in write_result.rows if r.identity_id == "sa-writer")
        write_reader = sum(r.amount for r in write_result.rows if r.identity_id == "sa-reader")
        assert write_writer == Decimal("100")
        assert write_reader == Decimal("0")

        # READ: sa-writer gets 0%, sa-reader gets 100%
        read_writer = sum(r.amount for r in read_result.rows if r.identity_id == "sa-writer")
        read_reader = sum(r.amount for r in read_result.rows if r.identity_id == "sa-reader")
        assert read_writer == Decimal("0")
        assert read_reader == Decimal("100")


class TestKafkaApiKeyResolution:
    """Tests for API key → owner translation in usage allocation and fallback filters."""

    def test_kafka_usage_allocation_routes_api_key_bytes_to_owner(
        self, base_billing_line: BillingLineItem
    ) -> None:
        """Bytes attributed to api key principal are mapped to owner before allocation."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        sa_abc = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-abc",
            identity_type="service_account",
        )
        resource_active = IdentitySet()
        resource_active.add(sa_abc)

        resolution = IdentityResolution(
            resource_active=resource_active,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={"api_key_to_owner": {"key-xyz": "sa-abc"}},
        )
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    500.0,
                    {"principal_id": "key-xyz"},
                ),
            ],
        }
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert "sa-abc" in recipient_ids
        assert "key-xyz" not in recipient_ids

    def test_multiple_api_keys_same_owner_aggregate(
        self, base_billing_line: BillingLineItem
    ) -> None:
        """Multiple api keys with same owner aggregate their bytes before ratio allocation."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        sa_abc = CoreIdentity(
            ecosystem="confluent_cloud",
            tenant_id="org-123",
            identity_id="sa-abc",
            identity_type="service_account",
        )
        resource_active = IdentitySet()
        resource_active.add(sa_abc)

        resolution = IdentityResolution(
            resource_active=resource_active,
            metrics_derived=IdentitySet(),
            tenant_period=IdentitySet(),
            context={"api_key_to_owner": {"key-1": "sa-abc", "key-2": "sa-abc"}},
        )
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    200.0,
                    {"principal_id": "key-1"},
                ),
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    300.0,
                    {"principal_id": "key-2"},
                ),
            ],
        }
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        # sa-abc should receive all allocation (sole owner of aggregated 500 bytes)
        recipient_ids = {r.identity_id for r in result.rows}
        assert "sa-abc" in recipient_ids
        assert "key-1" not in recipient_ids
        assert "key-2" not in recipient_ids
        sa_abc_amount = sum(r.amount for r in result.rows if r.identity_id == "sa-abc")
        assert sa_abc_amount == Decimal("100")

    def test_fallback_no_metrics_filters_to_owner_types(
        self, base_billing_line: BillingLineItem
    ) -> None:
        """_fallback_no_metrics even split excludes api_key identities from tenant_period."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-1",
                identity_type="service_account",
            )
        )
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="key-xyz",
                identity_type="api_key",
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

        result = kafka_network_allocator(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert "sa-1" in recipient_ids
        assert "key-xyz" not in recipient_ids

    def test_fallback_zero_usage_filters_to_owner_types(
        self, base_billing_line: BillingLineItem
    ) -> None:
        """_fallback_zero_usage even split excludes api_key identities from tenant_period."""
        from plugins.confluent_cloud.allocators.kafka_allocators import kafka_network_allocator

        tenant_period = IdentitySet()
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-1",
                identity_type="service_account",
            )
        )
        tenant_period.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="key-xyz",
                identity_type="api_key",
            )
        )
        resolution = IdentityResolution(
            resource_active=IdentitySet(),
            metrics_derived=IdentitySet(),
            tenant_period=tenant_period,
        )
        # Metrics exist but value=0 → zero-usage branch
        metrics_data = {
            "bytes_in": [
                MetricRow(
                    datetime(2026, 2, 1, tzinfo=UTC),
                    "bytes_in",
                    0.0,
                    {"principal_id": "key-xyz"},
                ),
            ],
        }
        ctx = AllocationContext(
            timeslice=base_billing_line.timestamp,
            billing_line=base_billing_line,
            identities=resolution,
            split_amount=Decimal("100"),
            metrics_data=metrics_data,
            params={},
        )

        result = kafka_network_allocator(ctx)

        recipient_ids = {r.identity_id for r in result.rows}
        assert "sa-1" in recipient_ids
        assert "key-xyz" not in recipient_ids
