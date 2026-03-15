from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class BillingLineItem(Protocol):
    """Protocol satisfied by any billing line item regardless of ecosystem."""

    @property
    def ecosystem(self) -> str: ...

    @property
    def tenant_id(self) -> str: ...

    @property
    def timestamp(self) -> datetime: ...

    @property
    def resource_id(self) -> str: ...

    @property
    def product_category(self) -> str: ...

    @property
    def product_type(self) -> str: ...

    @property
    def quantity(self) -> Decimal: ...

    @property
    def unit_price(self) -> Decimal: ...

    @property
    def total_cost(self) -> Decimal: ...

    @property
    def currency(self) -> str: ...

    @property
    def granularity(self) -> str: ...

    @property
    def metadata(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class CoreBillingLineItem:
    """Concrete billing line item for core/generic ecosystems.

    Satisfies the BillingLineItem Protocol. Use this when no plugin-specific
    fields (e.g. env_id) are needed.
    """

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
