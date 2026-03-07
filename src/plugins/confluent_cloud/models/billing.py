from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CCloudBillingLineItem:
    """Confluent Cloud billing line item.

    Extends the core BillingLineItem Protocol with env_id, which is required
    to correctly identify billing rows (same cluster in different envs would
    otherwise collide on the 6-field core PK).
    """

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    env_id: str  # CCloud-specific; part of billing PK
    resource_id: str
    product_category: str
    product_type: str
    quantity: Decimal
    unit_price: Decimal
    total_cost: Decimal
    currency: str = "USD"
    granularity: str = "daily"
    metadata: dict[str, Any] = field(default_factory=dict)
