from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, ClassVar

from core.models.emit_descriptors import MetricDescriptor

logger = logging.getLogger(__name__)


class CostType(StrEnum):
    """Classification of a chargeback cost."""

    USAGE = "usage"
    SHARED = "shared"


class AllocationDetail(StrEnum):
    """Standardized reason codes for allocation decisions.

    Values are stored as VARCHAR in the DB; existing free-form strings
    from older data remain valid (they just won't match enum values).

    """

    # Default/fallback allocators
    USING_DEFAULT_ALLOCATOR = "using_default_allocator"
    USING_UNKNOWN_ALLOCATOR = "using_unknown_allocator"

    # Identity resolution failures
    NO_IDENTITIES_LOCATED = "no_identities_located"
    NO_ACTIVE_IDENTITIES_LOCATED = "no_active_identities_located"

    # Metrics failures
    NO_METRICS_LOCATED = "no_metrics_located"
    NO_USAGE_FOR_ACTIVE_IDENTITIES = "no_usage_for_active_identities"
    METRICS_FETCH_FAILED = "metrics_fetch_failed"

    # Combined failures
    NO_METRICS_NO_ACTIVE_IDENTITIES = "no_metrics_no_active_identities"
    METRICS_PRESENT_NO_ACTIVE_IDENTITIES = "metrics_present_no_active_identities"

    # Usage-based allocation (success)
    USAGE_RATIO_ALLOCATION = "usage_ratio_allocation"

    # Even split (success)
    EVEN_SPLIT_ALLOCATION = "even_split_allocation"

    # Network allocator — Tier 2 (no metrics, active identities absent)
    NO_METRICS_NO_ACTIVE_IDENTITIES_LOCATED = "no_metrics_no_active_identities_located"

    # Network allocator — Tier 3 (metrics present but zero usage)
    NO_METRICS_PRESENT_MERGED_IDENTITIES_LOCATED = "no_metrics_present_merged_identities_located"
    NO_METRICS_PRESENT_PENALTY_ALLOCATION_FOR_EVERYONE = "no_metrics_present_penalty_allocation_for_everyone"


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
    tags: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    dimension_id: int | None = None

    __csv_fields__: ClassVar[tuple[str, ...]] = (
        "ecosystem",
        "tenant_id",
        "timestamp",
        "resource_id",
        "product_category",
        "product_type",
        "identity_id",
        "cost_type",
        "amount",
        "allocation_method",
        "allocation_detail",
    )
    __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
        MetricDescriptor(
            name="chitragupta_chargeback_amount",
            value_field="amount",
            label_fields=(
                "tenant_id",
                "ecosystem",
                "identity_id",
                "resource_id",
                "product_type",
                "cost_type",
                "allocation_method",
            ),
            documentation="Chargeback cost amount per identity/resource/product combination",
        ),
    )


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
    env_id: str = ""


@dataclass
class AggregationRow:
    """A single bucket from a server-side aggregation query."""

    dimensions: dict[str, str]
    time_bucket: str
    total_amount: Decimal
    usage_amount: Decimal
    shared_amount: Decimal
    row_count: int


@dataclass
class AllocationIssueRow:
    """Aggregated row of a failed allocation, grouped by dimension key + error code."""

    ecosystem: str
    env_id: str
    resource_id: str | None
    product_type: str
    identity_id: str
    allocation_detail: str
    row_count: int
    usage_cost: Decimal
    shared_cost: Decimal
    total_cost: Decimal
