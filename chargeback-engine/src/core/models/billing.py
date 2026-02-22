from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class BillingLineItem:
    """An immutable billing line item from an ecosystem's billing API."""

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    resource_id: str
    product_category: str
    product_type: str
    quantity: Decimal
    unit_price: Decimal
    total_cost: Decimal
    currency: str = "USD"
    granularity: str = "daily"
    metadata: dict[str, Any] = field(default_factory=dict)
