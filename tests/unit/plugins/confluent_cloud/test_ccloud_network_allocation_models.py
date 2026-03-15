"""Tests for CCloud Kafka network allocation models (TDD red phase).

Covers BYTES_IN_MODEL, BYTES_OUT_MODEL, PARTITION_MODEL constants and
make_network_model factory. Tests verify 4-tier fallback chain behavior.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import (
    CoreIdentity,
    IdentityResolution,
    IdentitySet,
    MetricRow,
)
from core.models.billing import CoreBillingLineItem
from plugins.confluent_cloud.allocation_models import (
    BYTES_IN_MODEL,
    BYTES_OUT_MODEL,
    PARTITION_MODEL,
    make_network_model,
)

_NOW = datetime(2026, 2, 1, tzinfo=UTC)
_RESOURCE_ID = "lkc-abc"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_billing_line(resource_id: str = _RESOURCE_ID) -> CoreBillingLineItem:
    return CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        timestamp=_NOW,
        resource_id=resource_id,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_READ",
        quantity=Decimal("1"),
        unit_price=Decimal("100"),
        total_cost=Decimal("100"),
    )


def _make_identity(identity_id: str, identity_type: str = "service_account") -> CoreIdentity:
    return CoreIdentity(
        ecosystem="confluent_cloud",
        tenant_id="org-123",
        identity_id=identity_id,
        identity_type=identity_type,
    )


def _make_iset(*identity_ids: str, identity_type: str = "service_account") -> IdentitySet:
    iset = IdentitySet()
    for iid in identity_ids:
        iset.add(_make_identity(iid, identity_type=identity_type))
    return iset


def _make_resolution(
    resource_active: IdentitySet | None = None,
    metrics_derived: IdentitySet | None = None,
    tenant_period: IdentitySet | None = None,
    context: dict | None = None,
) -> IdentityResolution:
    return IdentityResolution(
        resource_active=resource_active or IdentitySet(),
        metrics_derived=metrics_derived or IdentitySet(),
        tenant_period=tenant_period or IdentitySet(),
        context=context or {},
    )


def _make_ctx(
    identities: IdentityResolution | None = None,
    metrics_data: dict | None = None,
    split_amount: Decimal = Decimal("100"),
    resource_id: str = _RESOURCE_ID,
) -> AllocationContext:
    return AllocationContext(
        timeslice=_NOW,
        billing_line=_make_billing_line(resource_id=resource_id),
        identities=identities or _make_resolution(),
        split_amount=split_amount,
        metrics_data=metrics_data,
        params={},
    )


def _metric_row(key: str, value: float, principal_id: str) -> MetricRow:
    return MetricRow(_NOW, key, value, {"principal_id": principal_id})


# ---------------------------------------------------------------------------
# Tier 0: usage ratio allocation
# ---------------------------------------------------------------------------


class TestTier0UsageRatio:
    """BYTES_OUT_MODEL — Tier 0: ratio split by bytes_out per principal."""

    def test_bytes_out_usage_ratio_split(self) -> None:
        """Two principals with 100/300 bytes → 25%/75% amounts, chain_tier=0."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1", "sa-2"),
            ),
            metrics_data={
                "bytes_out": [
                    _metric_row("bytes_out", 100.0, "sa-1"),
                    _metric_row("bytes_out", 300.0, "sa-2"),
                ]
            },
        )

        result = BYTES_OUT_MODEL(ctx)

        assert isinstance(result, AllocationResult)
        assert len(result.rows) == 2
        amounts = {row.identity_id: row.amount for row in result.rows}
        assert amounts["sa-1"] == Decimal("25")
        assert amounts["sa-2"] == Decimal("75")
        for row in result.rows:
            assert row.allocation_detail == "usage_ratio_allocation"
            assert row.metadata["chain_tier"] == 0

    def test_api_key_resolved_to_owner(self) -> None:
        """api_key ak-1 is resolved to sa-10 via api_key_to_owner map."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-10"),
                context={"api_key_to_owner": {"ak-1": "sa-10"}},
            ),
            metrics_data={
                "bytes_out": [
                    _metric_row("bytes_out", 500.0, "ak-1"),
                ]
            },
        )

        result = BYTES_OUT_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-10"
        assert "ak-1" not in {row.identity_id for row in result.rows}
        assert result.rows[0].metadata["chain_tier"] == 0


# ---------------------------------------------------------------------------
# Tier 1: no metrics or zero usage — even split over merged_active
# ---------------------------------------------------------------------------


class TestTier1MergedActive:
    """BYTES_OUT_MODEL — Tier 1: even split when no usable metrics."""

    def test_no_metrics_falls_to_merged_active(self) -> None:
        """Empty metrics_data → Tier 1 even split over resource_active."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1", "sa-2"),
                metrics_derived=IdentitySet(),
            ),
            metrics_data={},
        )

        result = BYTES_OUT_MODEL(ctx)

        assert len(result.rows) == 2
        identity_ids = {row.identity_id for row in result.rows}
        assert identity_ids == {"sa-1", "sa-2"}
        for row in result.rows:
            assert row.allocation_detail == "no_metrics_located"
            assert row.metadata["chain_tier"] == 1
        total = sum(row.amount for row in result.rows)
        assert total == Decimal("100")

    def test_zero_usage_falls_to_merged_active(self) -> None:
        """bytes_out row with value=0 → Tier 0 skipped → Tier 1 over merged_active."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-2"),
                metrics_derived=IdentitySet(),
            ),
            metrics_data={
                "bytes_out": [
                    _metric_row("bytes_out", 0.0, "sa-1"),
                ]
            },
        )

        result = BYTES_OUT_MODEL(ctx)

        identity_ids = {row.identity_id for row in result.rows}
        assert identity_ids == {"sa-2"}
        for row in result.rows:
            assert row.allocation_detail == "no_metrics_located"
            assert row.metadata["chain_tier"] == 1


# ---------------------------------------------------------------------------
# Tier 2: no active identities — even split over tenant_period owners
# ---------------------------------------------------------------------------


class TestTier2TenantPeriod:
    """BYTES_OUT_MODEL — Tier 2: even split over tenant_period when merged_active empty."""

    def test_no_active_identities_uses_tenant_period(self) -> None:
        """No metrics + empty resource_active/metrics_derived → Tier 2 over tenant_period."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=IdentitySet(),
                metrics_derived=IdentitySet(),
                tenant_period=_make_iset("sa-3"),
            ),
            metrics_data=None,
        )

        result = BYTES_OUT_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-3"
        assert result.rows[0].allocation_detail == "no_active_identities_located"
        assert result.rows[0].metadata["chain_tier"] == 2


# ---------------------------------------------------------------------------
# Tier 3: no identities at all — terminal to resource_id
# ---------------------------------------------------------------------------


class TestTier3Terminal:
    """BYTES_OUT_MODEL — Tier 3: terminal allocation to resource_id."""

    def test_no_identities_terminal_to_resource_id(self) -> None:
        """All identity sets empty, no metrics → resource_id terminal row."""
        resource_id = "lkc-terminal"
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=IdentitySet(),
                metrics_derived=IdentitySet(),
                tenant_period=IdentitySet(),
            ),
            metrics_data=None,
            resource_id=resource_id,
        )

        result = BYTES_OUT_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == resource_id
        assert result.rows[0].allocation_detail == "no_identities_located"
        assert result.rows[0].metadata["chain_tier"] == 3


# ---------------------------------------------------------------------------
# Direction isolation
# ---------------------------------------------------------------------------


class TestDirectionIsolation:
    """BYTES_IN_MODEL must only read bytes_in; bytes_out data must not trigger Tier 0."""

    def test_bytes_in_model_ignores_bytes_out_data(self) -> None:
        """bytes_out metric data → BYTES_IN_MODEL cannot fire Tier 0 → falls to Tier 1."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1"),
            ),
            metrics_data={
                "bytes_out": [
                    _metric_row("bytes_out", 500.0, "sa-1"),
                ]
            },
        )

        result = BYTES_IN_MODEL(ctx)

        # Must NOT have fired Tier 0 (bytes_out data irrelevant to bytes_in model)
        for row in result.rows:
            assert row.metadata["chain_tier"] == 1
            assert row.allocation_detail == "no_metrics_located"

    def test_bytes_out_model_ignores_bytes_in_data(self) -> None:
        """bytes_in metric data → BYTES_OUT_MODEL cannot fire Tier 0 → falls to Tier 1."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1"),
            ),
            metrics_data={
                "bytes_in": [
                    _metric_row("bytes_in", 500.0, "sa-1"),
                ]
            },
        )

        result = BYTES_OUT_MODEL(ctx)

        for row in result.rows:
            assert row.metadata["chain_tier"] == 1
            assert row.allocation_detail == "no_metrics_located"


# ---------------------------------------------------------------------------
# PARTITION_MODEL always falls through Tier 0
# ---------------------------------------------------------------------------


class TestPartitionModel:
    """PARTITION_MODEL — no metric configured, always starts at Tier 1."""

    def test_partition_model_empty_metric_list_falls_to_tier1(self) -> None:
        """partition_count key with empty list → Tier 0 skipped → Tier 1 over resource_active."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1"),
            ),
            metrics_data={"partition_count": []},
        )

        result = PARTITION_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].identity_id == "sa-1"
        assert result.rows[0].allocation_detail == "no_metrics_located"
        assert result.rows[0].metadata["chain_tier"] == 1


# ---------------------------------------------------------------------------
# Factory independence
# ---------------------------------------------------------------------------


class TestFactoryIndependence:
    """make_network_model creates independent model instances."""

    def test_bytes_in_and_bytes_out_models_are_distinct_objects(self) -> None:
        """BYTES_IN_MODEL is not BYTES_OUT_MODEL."""
        assert BYTES_IN_MODEL is not BYTES_OUT_MODEL

    def test_bytes_in_model_fires_tier0_with_bytes_in_data(self) -> None:
        """BYTES_IN_MODEL fires Tier 0 when bytes_in data is present."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-1"),
            ),
            metrics_data={
                "bytes_in": [
                    _metric_row("bytes_in", 800.0, "sa-1"),
                ]
            },
        )

        result = BYTES_IN_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].metadata["chain_tier"] == 0
        assert result.rows[0].allocation_detail == "usage_ratio_allocation"

    def test_bytes_out_model_fires_tier0_with_bytes_out_data(self) -> None:
        """BYTES_OUT_MODEL fires Tier 0 when bytes_out data is present."""
        ctx = _make_ctx(
            identities=_make_resolution(
                resource_active=_make_iset("sa-2"),
            ),
            metrics_data={
                "bytes_out": [
                    _metric_row("bytes_out", 600.0, "sa-2"),
                ]
            },
        )

        result = BYTES_OUT_MODEL(ctx)

        assert len(result.rows) == 1
        assert result.rows[0].metadata["chain_tier"] == 0
        assert result.rows[0].allocation_detail == "usage_ratio_allocation"

    def test_make_network_model_returns_new_instance_each_call(self) -> None:
        """make_network_model() creates independent instances, not cached."""
        model_a = make_network_model(metric_key="bytes_in", principal_label="principal_id")
        model_b = make_network_model(metric_key="bytes_in", principal_label="principal_id")
        assert model_a is not model_b
