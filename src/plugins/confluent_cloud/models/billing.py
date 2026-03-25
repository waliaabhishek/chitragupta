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


def billing_natural_key(
    item: CCloudBillingLineItem,
) -> tuple[str, str, datetime, str, str, str, str]:
    """Return the 7-field natural key that uniquely identifies a billing line item.

    Used by ``cost_input._fetch_window()`` for tier grouping.
    ``repositories._billing_pk()`` encodes the same fields independently;
    a follow-up task can unify them.
    """
    return (
        item.ecosystem,
        item.tenant_id,
        item.timestamp,
        item.env_id,
        item.resource_id,
        item.product_type,
        item.product_category,
    )
