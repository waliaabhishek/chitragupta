"""Tests for GAP-116: kafka_usage_allocation distinguishes metrics fetch failure.

Verification cases 5 and 6 from design doc:
5. _kafka_usage_allocation with metrics_fetch_failed=True → METRICS_FETCH_FAILED
6. _kafka_usage_allocation with metrics_fetch_failed=False and metrics_data={} → NO_METRICS_LOCATED (unchanged)
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext
from core.models import CoreIdentity, IdentityResolution, IdentitySet
from core.models.billing import CoreBillingLineItem
from core.models.chargeback import AllocationDetail
from plugins.confluent_cloud.allocators.kafka_allocators import _kafka_usage_allocation

_NOW = datetime(2026, 2, 22, 12, 0, 0, tzinfo=UTC)


def _make_kafka_ctx(
    metrics_data: dict | None = None,
    metrics_fetch_failed: bool = False,
    resource_active: IdentitySet | None = None,
) -> AllocationContext:
    billing_line = CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=_NOW,
        resource_id="lkc-abc",
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_READ",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )
    sa_set = resource_active if resource_active is not None else IdentitySet()
    if sa_set is None or len(list(sa_set.ids())) == 0:
        sa_set = IdentitySet()
        sa_set.add(
            CoreIdentity(
                ecosystem="confluent_cloud",
                tenant_id="org-123",
                identity_id="sa-1",
                identity_type="service_account",
            )
        )
    identities = IdentityResolution(
        resource_active=sa_set,
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
        context={"api_key_to_owner": {}},
    )
    return AllocationContext(
        timeslice=_NOW,
        billing_line=billing_line,
        identities=identities,
        split_amount=Decimal("100.00"),
        metrics_data=metrics_data,
        metrics_fetch_failed=metrics_fetch_failed,
    )


class TestKafkaUsageAllocationMetricsFetchFailed:
    def test_metrics_fetch_failed_true_produces_metrics_fetch_failed_detail(self) -> None:
        """metrics_fetch_failed=True → METRICS_FETCH_FAILED (not NO_METRICS_LOCATED)."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=True)
        result = _kafka_usage_allocation(ctx)

        assert len(result.rows) == 1
        row = result.rows[0]
        assert row.allocation_detail == AllocationDetail.METRICS_FETCH_FAILED, (
            f"Expected METRICS_FETCH_FAILED, got {row.allocation_detail!r}"
        )
        assert row.identity_id == "UNALLOCATED"

    def test_metrics_fetch_failed_true_does_not_produce_no_metrics_located(self) -> None:
        """metrics_fetch_failed=True must NOT produce NO_METRICS_LOCATED."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=True)
        result = _kafka_usage_allocation(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].allocation_detail != AllocationDetail.NO_METRICS_LOCATED

    def test_metrics_fetch_failed_false_empty_metrics_produces_no_metrics_located(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + metrics_data={} → NO_METRICS_LOCATED."""
        ctx = _make_kafka_ctx(metrics_data={}, metrics_fetch_failed=False)
        result = _kafka_usage_allocation(ctx)

        # Falls through to _fallback_no_metrics → NO_METRICS_LOCATED or
        # NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED (depending on identity resolution)
        details = {row.allocation_detail for row in result.rows}
        assert AllocationDetail.METRICS_FETCH_FAILED not in details, (
            "Empty metrics (not a failure) must not produce METRICS_FETCH_FAILED"
        )
        # Must be one of the legitimate no-metrics detail codes
        no_metrics_details = {
            AllocationDetail.NO_METRICS_LOCATED,
            AllocationDetail.NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED,
            AllocationDetail.NO_IDENTITIES_LOCATED,
        }
        assert details & no_metrics_details, f"Expected a no-metrics detail code, got {details}"

    def test_metrics_fetch_failed_false_none_metrics_unchanged_behavior(self) -> None:
        """Existing behavior: metrics_fetch_failed=False + metrics_data=None → no METRICS_FETCH_FAILED."""
        ctx = _make_kafka_ctx(metrics_data=None, metrics_fetch_failed=False)
        result = _kafka_usage_allocation(ctx)

        details = {row.allocation_detail for row in result.rows}
        assert AllocationDetail.METRICS_FETCH_FAILED not in details

    def test_metrics_fetch_failed_false_with_usage_data_allocates_normally(self) -> None:
        """Existing behavior: metrics_fetch_failed=False with real metrics → normal allocation."""
        from core.models.metrics import MetricRow

        byte_row = MetricRow(
            timestamp=_NOW,
            metric_key="bytes_in",
            value=200.0,
            labels={"principal_id": "sa-1"},
        )
        ctx = _make_kafka_ctx(
            metrics_data={"bytes_in": [byte_row]},
            metrics_fetch_failed=False,
        )
        result = _kafka_usage_allocation(ctx)

        details = {row.allocation_detail for row in result.rows}
        assert AllocationDetail.METRICS_FETCH_FAILED not in details
        identities = {row.identity_id for row in result.rows}
        assert "UNALLOCATED" not in identities
