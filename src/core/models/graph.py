from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

logger = logging.getLogger(__name__)


class EdgeType(StrEnum):
    parent = "parent"
    charge = "charge"
    attribution = "attribution"  # reserved for TASK-221 topic attribution flow


@dataclass
class GraphNodeData:
    id: str
    resource_type: str  # "environment" | "kafka_cluster" | "kafka_topic" | "identity" | ...
    display_name: str | None
    cost: Decimal  # sum of chargeback_facts.amount in billing period
    created_at: datetime | None
    deleted_at: datetime | None
    tags: dict[str, str]  # resolved from entity_tags
    parent_id: str | None
    cloud: str | None
    region: str | None
    status: str
    cross_references: list[str] = field(default_factory=list)  # other resource_ids this identity is charged in


@dataclass
class GraphEdgeData:
    source: str  # entity_id of source (parent) node
    target: str  # entity_id of target (child) node
    relationship_type: EdgeType
    cost: Decimal | None = None  # populated for charge edges


@dataclass
class GraphNeighborhood:
    nodes: list[GraphNodeData]
    edges: list[GraphEdgeData]


@dataclass
class GraphSearchResultData:
    id: str
    resource_type: str  # resource_type for resources; identity_type for identities
    display_name: str | None
    parent_id: str | None  # None for identities (no parent_id on IdentityTable)
    status: str  # "active" | "deleted"


@dataclass
class GraphDiffNodeData:
    id: str
    resource_type: str
    display_name: str | None
    parent_id: str | None
    cost_before: Decimal
    cost_after: Decimal
    cost_delta: Decimal
    pct_change: Decimal | None  # None when cost_before == 0 (new entity)
    status: str  # "new" | "deleted" | "changed" | "unchanged"


@dataclass
class GraphTimelineData:
    date: date
    cost: Decimal
