from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models import (
        BillingLineItem,
        ChargebackRow,
        IdentityResolution,
        MetricRow,
    )
    from core.plugin.protocols import CostAllocator
logger = logging.getLogger(__name__)


@dataclass
class AllocationContext:
    """Immutable context passed to cost allocators."""

    timeslice: datetime
    billing_line: BillingLineItem
    identities: IdentityResolution
    split_amount: Decimal = Decimal(0)
    metrics_data: dict[str, list[MetricRow]] | None = None
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class AllocationResult:
    """Container for chargeback rows produced by an allocator."""

    rows: list[ChargebackRow] = field(default_factory=list)


class AllocatorRegistry:
    """Two-tier registry: overrides take precedence over base registrations."""

    def __init__(self) -> None:
        self._base: dict[str, CostAllocator] = {}
        self._overrides: dict[str, CostAllocator] = {}

    def register(self, product_type: str, allocator: CostAllocator) -> None:
        """Register a base allocator. Raises ValueError on duplicate."""
        if product_type in self._base:
            msg = f"Duplicate base registration for product_type {product_type!r}"
            raise ValueError(msg)
        self._base[product_type] = allocator

    def register_override(self, product_type: str, allocator: CostAllocator) -> None:
        """Register an override allocator. Last-write-wins."""
        self._overrides[product_type] = allocator

    def get(self, product_type: str) -> CostAllocator:
        """Return override if present, else base. Raises KeyError if neither."""
        if product_type in self._overrides:
            return self._overrides[product_type]
        if product_type in self._base:
            return self._base[product_type]
        msg = f"No allocator registered for product_type {product_type!r}"
        raise KeyError(msg)

    def list_product_types(self) -> list[str]:
        """Return all product types with registrations (base or override)."""
        return sorted({*self._base, *self._overrides})

    def list_overrides(self) -> list[str]:
        """Return product types that have overrides."""
        return sorted(self._overrides)
