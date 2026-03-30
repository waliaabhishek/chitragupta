from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TopicAttributionRow:
    """A single row of topic-level cost attribution.

    Analogous to ChargebackRow but for topic-scoped cost overlay.
    """

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    env_id: str
    cluster_resource_id: str  # "lkc-jj231m"
    topic_name: str  # "orders-events"
    product_category: str  # "KAFKA" — same as ccloud_billing
    product_type: str  # "KAFKA_NETWORK_WRITE" — same as ccloud_billing
    attribution_method: str  # "bytes_ratio", "even_split", etc.
    amount: Decimal = Decimal(0)  # attributed cost
    metadata: dict[str, Any] = field(default_factory=dict)
    dimension_id: int | None = None


@dataclass
class TopicAttributionAggregationBucket:
    """Domain-level aggregation bucket — one group-by key × time bucket combination."""

    dimensions: dict[str, str]
    time_bucket: str
    total_amount: Decimal
    row_count: int


@dataclass
class TopicAttributionAggregationResult:
    """Domain-level aggregation result returned by the repository.

    The API route converts this to TopicAttributionAggregationResponse (Pydantic).
    Storage layer must NOT import from core.api.* — this keeps the dependency direction correct.
    """

    buckets: list[TopicAttributionAggregationBucket]
    total_amount: Decimal
    total_rows: int
