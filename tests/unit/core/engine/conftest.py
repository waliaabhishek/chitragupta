from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from core.engine.allocation import AllocationContext, AllocationResult
from core.models import BillingLineItem, CoreBillingLineItem, IdentityResolution
from core.models.identity import IdentitySet

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def make_billing_line(**overrides: Any) -> BillingLineItem:
    defaults: dict[str, Any] = {
        "ecosystem": "confluent",
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


def make_identity_resolution() -> IdentityResolution:
    return IdentityResolution(
        resource_active=IdentitySet(),
        metrics_derived=IdentitySet(),
        tenant_period=IdentitySet(),
    )


def make_ctx(
    split_amount: Decimal = Decimal("10.00"),
    **overrides: Any,
) -> AllocationContext:
    defaults: dict[str, Any] = {
        "timeslice": _NOW,
        "billing_line": make_billing_line(),
        "identities": make_identity_resolution(),
        "split_amount": split_amount,
    }
    defaults.update(overrides)
    return AllocationContext(**defaults)


def stub_allocator(ctx: AllocationContext) -> AllocationResult:
    return AllocationResult()


def stub_allocator_2(ctx: AllocationContext) -> AllocationResult:
    return AllocationResult()
