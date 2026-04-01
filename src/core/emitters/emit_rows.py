from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar

from core.models.emit_descriptors import MetricDescriptor  # models layer — no inversion

if TYPE_CHECKING:
    from core.models.billing import BillingLineItem
    from core.models.identity import Identity
    from core.models.resource import Resource

logger = logging.getLogger(__name__)


@dataclass
class BillingEmitRow:
    """Emit-layer wrapper for BillingLineItem — maps total_cost → amount for Prometheus."""

    tenant_id: str
    ecosystem: str
    resource_id: str
    product_type: str
    product_category: str
    amount: Decimal
    timestamp: datetime

    __csv_fields__: ClassVar[tuple[str, ...]] = ()  # Prometheus-only, no CSV export
    __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
        MetricDescriptor(
            name="chitragupta_billing_amount",
            value_field="amount",
            label_fields=("tenant_id", "ecosystem", "resource_id", "product_type", "product_category"),
            documentation="Billing total cost per resource/product combination",
        ),
    )

    @classmethod
    def from_line(cls, line: BillingLineItem, tenant_id: str, timestamp: datetime) -> BillingEmitRow:
        return cls(
            tenant_id=tenant_id,
            ecosystem=line.ecosystem,
            resource_id=line.resource_id,
            product_type=line.product_type,
            product_category=line.product_category,
            amount=line.total_cost,
            timestamp=timestamp,
        )


@dataclass
class ResourceEmitRow:
    """Emit-layer wrapper for Resource — amount=1 (active indicator)."""

    tenant_id: str
    ecosystem: str
    resource_id: str
    resource_type: str
    amount: Decimal
    timestamp: datetime

    __csv_fields__: ClassVar[tuple[str, ...]] = ()
    __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
        MetricDescriptor(
            name="chitragupta_resource_active",
            value_field="amount",
            label_fields=("tenant_id", "ecosystem", "resource_id", "resource_type"),
            documentation="Active resources at billing date (1 = active)",
        ),
    )

    @classmethod
    def from_resource(cls, r: Resource, tenant_id: str, timestamp: datetime) -> ResourceEmitRow:
        return cls(
            tenant_id=tenant_id,
            ecosystem=r.ecosystem,
            resource_id=r.resource_id,
            resource_type=r.resource_type,
            amount=Decimal(1),
            timestamp=timestamp,
        )


@dataclass
class IdentityEmitRow:
    """Emit-layer wrapper for Identity — amount=1 (active indicator)."""

    tenant_id: str
    ecosystem: str
    identity_id: str
    identity_type: str
    amount: Decimal
    timestamp: datetime

    __csv_fields__: ClassVar[tuple[str, ...]] = ()
    __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
        MetricDescriptor(
            name="chitragupta_identity_active",
            value_field="amount",
            label_fields=("tenant_id", "ecosystem", "identity_id", "identity_type"),
            documentation="Active identities at billing date (1 = active)",
        ),
    )

    @classmethod
    def from_identity(cls, i: Identity, tenant_id: str, timestamp: datetime) -> IdentityEmitRow:
        return cls(
            tenant_id=tenant_id,
            ecosystem=i.ecosystem,
            identity_id=i.identity_id,
            identity_type=i.identity_type,
            amount=Decimal(1),
            timestamp=timestamp,
        )
