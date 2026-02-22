from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class CostType(StrEnum):
    """Classification of a chargeback cost."""

    USAGE = "usage"
    SHARED = "shared"


@dataclass
class ChargebackRow:
    """A single row of chargeback output."""

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    resource_id: str | None
    product_category: str
    product_type: str
    identity_id: str
    cost_type: CostType
    amount: Decimal = Decimal(0)
    allocation_method: str | None = None
    allocation_detail: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
