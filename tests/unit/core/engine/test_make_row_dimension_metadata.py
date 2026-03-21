from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from core.engine.allocation import AllocationContext
from core.engine.helpers import make_row
from core.models import CoreBillingLineItem, CostType
from core.models.identity import IdentityResolution, IdentitySet

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_billing_line(**overrides: Any) -> CoreBillingLineItem:
    defaults: dict[str, Any] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "t-001",
        "timestamp": _NOW,
        "resource_id": "lkc-abc123",
        "product_category": "kafka",
        "product_type": "kafka_num_ckus",
        "quantity": Decimal("100"),
        "unit_price": Decimal("0.01"),
        "total_cost": Decimal("1.00"),
    }
    defaults.update(overrides)
    return CoreBillingLineItem(**defaults)


def _make_identities() -> IdentityResolution:
    return IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
    )


class TestAllocationContextDimensionMetadataDefault:
    """Verification item 8 — AllocationContext.dimension_metadata default."""

    def test_allocation_context_dimension_metadata_default_is_empty_dict(self) -> None:
        """AllocationContext() without dimension_metadata kwarg has dimension_metadata={}."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
        )
        assert ctx.dimension_metadata == {}

    def test_allocation_context_dimension_metadata_accepts_env_id(self) -> None:
        """dimension_metadata can be explicitly set with env_id."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            dimension_metadata={"env_id": "env-abc123"},
        )
        assert ctx.dimension_metadata == {"env_id": "env-abc123"}


class TestMakeRowDimensionMetadataMerge:
    """Verification item 8 — make_row() merges ctx.dimension_metadata with caller metadata."""

    def test_make_row_no_caller_metadata_no_ctx_dimension_metadata_yields_empty_metadata(self) -> None:
        """make_row() without metadata= and empty ctx.dimension_metadata → row.metadata={}."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("10.00"),
            dimension_metadata={},
        )
        row = make_row(ctx, "user-1", CostType.USAGE, Decimal("10.00"), "direct")
        assert row.metadata == {}

    def test_make_row_ctx_env_id_without_caller_metadata_preserves_env_id(self) -> None:
        """make_row() with ctx.dimension_metadata={"env_id":"env-abc"} and no metadata= → env_id preserved."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("10.00"),
            dimension_metadata={"env_id": "env-abc"},
        )
        row = make_row(ctx, "user-1", CostType.USAGE, Decimal("10.00"), "direct")
        assert row.metadata.get("env_id") == "env-abc"

    def test_make_row_caller_metadata_ratio_merged_with_ctx_env_id(self) -> None:
        """metadata={"ratio": 0.4} merged with ctx.dimension_metadata → both keys present."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("10.00"),
            dimension_metadata={"env_id": "env-abc"},
        )
        row = make_row(ctx, "user-1", CostType.USAGE, Decimal("10.00"), "usage_ratio", metadata={"ratio": 0.4})
        assert row.metadata.get("env_id") == "env-abc"
        assert row.metadata.get("ratio") == pytest.approx(0.4)

    def test_make_row_caller_metadata_without_env_id_does_not_drop_env_id(self) -> None:
        """Caller metadata without env_id key does not clobber env_id from ctx.dimension_metadata."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("5.00"),
            dimension_metadata={"env_id": "env-xyz"},
        )
        row = make_row(ctx, "u-1", CostType.SHARED, Decimal("5.00"), "even_split", metadata={"detail": "split"})
        assert row.metadata.get("env_id") == "env-xyz"
        assert row.metadata.get("detail") == "split"

    def test_make_row_existing_allocators_without_metadata_kwarg_unaffected(self) -> None:
        """Existing allocators that omit metadata= are unaffected: no env_id leakage when dimension_metadata={}."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("10.00"),
            # dimension_metadata not passed — defaults to {}
        )
        row = make_row(ctx, "user-1", CostType.USAGE, Decimal("10.00"), "direct")
        assert isinstance(row.metadata, dict)
        assert "env_id" not in row.metadata

    def test_make_row_caller_metadata_can_override_ctx_dimension_metadata_key(self) -> None:
        """Caller metadata can override a key set by ctx.dimension_metadata (caller wins)."""
        ctx = AllocationContext(
            timeslice=_NOW,
            billing_line=_make_billing_line(),
            identities=_make_identities(),
            split_amount=Decimal("10.00"),
            dimension_metadata={"env_id": "env-original"},
        )
        row = make_row(ctx, "user-1", CostType.USAGE, Decimal("10.00"), "direct", metadata={"env_id": "env-override"})
        assert row.metadata.get("env_id") == "env-override"
