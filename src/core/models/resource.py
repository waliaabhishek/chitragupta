from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class ResourceStatus(StrEnum):
    """Status of a tracked resource."""

    ACTIVE = "active"
    DELETED = "deleted"


@runtime_checkable
class Resource(Protocol):
    """Protocol for a billable resource within an ecosystem."""

    @property
    def ecosystem(self) -> str: ...

    @property
    def tenant_id(self) -> str: ...

    @property
    def resource_id(self) -> str: ...

    @property
    def resource_type(self) -> str: ...

    @property
    def display_name(self) -> str | None: ...

    @property
    def parent_id(self) -> str | None: ...

    @property
    def owner_id(self) -> str | None: ...

    @property
    def status(self) -> ResourceStatus: ...

    @property
    def created_at(self) -> datetime | None: ...

    @property
    def deleted_at(self) -> datetime | None: ...

    @property
    def last_seen_at(self) -> datetime | None: ...

    @property
    def metadata(self) -> dict[str, Any]: ...


@dataclass
class CoreResource:
    """Core implementation of the Resource Protocol."""

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
