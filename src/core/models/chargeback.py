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
    dimension_id: int | None = None


@dataclass
class CustomTag:
    """A user-defined tag attached to a chargeback dimension."""

    tag_id: int | None
    dimension_id: int
    tag_key: str
    tag_value: str
    display_name: str
    created_by: str
    created_at: datetime | None = None


@dataclass
class ChargebackDimensionInfo:
    """Dimension row with ownership info for tenant isolation checks."""

    dimension_id: int
    ecosystem: str
    tenant_id: str
    resource_id: str | None
    product_category: str
    product_type: str
    identity_id: str
    cost_type: str
    allocation_method: str | None
    allocation_detail: str | None


@dataclass
class AggregationRow:
    """A single bucket from a server-side aggregation query."""

    dimensions: dict[str, str]
    time_bucket: str
    total_amount: Decimal
    row_count: int
