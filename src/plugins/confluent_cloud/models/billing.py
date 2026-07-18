from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CCloudCostSourceRecord:
    """One native Confluent Cost record before allocation aggregation."""

    ecosystem: str
    tenant_id: str
    source_record_id: str
    identity_scheme: str
    provider_cost_id: str | None
    source_period_start: datetime | None
    source_period_end: datetime | None
    collection_window_start: datetime
    collection_window_end: datetime
    evidence_scope_start: datetime
    evidence_scope_end: datetime
    allocation_timestamp: datetime
    retention_timestamp: datetime
    granularity: str | None
    product: str | None
    line_type: str | None
    amount: Decimal | None
    original_amount: Decimal | None
    discount_amount: Decimal | None
    price: Decimal | None
    quantity: Decimal | None
    unit: str | None
    description: str | None
    network_access_type: str | None
    resource_id: str | None
    resource_name: str | None
    environment_id: str | None
    tier_dimensions: dict[str, str]
    malformed: bool
    diagnostics: tuple[str, ...]
    raw_payload: dict[str, Any]


@runtime_checkable
class CCloudSourceWindowWriter(Protocol):
    """Production-consumed persistence seam for native Cost evidence."""

    def replace_source_window(
        self,
        ecosystem: str,
        tenant_id: str,
        refresh_window_start: datetime,
        refresh_window_end: datetime,
        records: Sequence[CCloudCostSourceRecord],
    ) -> None: ...


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
