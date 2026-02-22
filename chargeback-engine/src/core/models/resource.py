from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class ResourceStatus(StrEnum):
    """Status of a tracked resource."""

    ACTIVE = "active"
    DELETED = "deleted"


@dataclass
class Resource:
    """A billable resource within an ecosystem."""

    ecosystem: str
    tenant_id: str
    resource_id: str
    resource_type: str
    display_name: str | None = None
    parent_id: str | None = None
    owner_id: str | None = None
    status: ResourceStatus = ResourceStatus.ACTIVE
    created_at: datetime | None = None
    deleted_at: datetime | None = None
    last_seen_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
