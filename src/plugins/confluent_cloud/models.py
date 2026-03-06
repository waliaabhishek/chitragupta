from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models import Resource

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CCloudFlinkStatement:
    """Typed view of a Flink statement resource."""

    resource_id: str
    statement_name: str
    compute_pool_id: str
    environment_id: str
    owner_id: str
    is_stopped: bool

    @classmethod
    def from_resource(cls, r: Resource) -> CCloudFlinkStatement:
        """Project core Resource into typed Flink statement view."""
        return cls(
            resource_id=r.resource_id,
            statement_name=r.metadata["statement_name"],
            compute_pool_id=r.metadata["compute_pool_id"],
            environment_id=r.parent_id or "",
            owner_id=r.owner_id or "",
            is_stopped=r.metadata.get("is_stopped", False),
        )


@dataclass(frozen=True)
class CCloudFlinkPool:
    """Typed view of a Flink compute pool resource."""

    resource_id: str
    pool_name: str
    environment_id: str
    cloud: str
    region: str
    max_cfu: int

    @classmethod
    def from_resource(cls, r: Resource) -> CCloudFlinkPool:
        return cls(
            resource_id=r.resource_id,
            pool_name=r.display_name or r.resource_id,
            environment_id=r.parent_id or "",
            cloud=r.metadata.get("cloud", ""),
            region=r.metadata.get("region", ""),
            max_cfu=r.metadata.get("max_cfu", 0),
        )


@dataclass(frozen=True)
class CCloudConnector:
    """Typed view of a connector resource."""

    resource_id: str
    connector_name: str
    connector_class: str
    cluster_id: str
    environment_id: str
    owner_id: str
    is_deleted: bool

    @classmethod
    def from_resource(cls, r: Resource) -> CCloudConnector:
        from core.models import ResourceStatus

        return cls(
            resource_id=r.resource_id,
            connector_name=r.display_name or r.resource_id,
            connector_class=r.metadata.get("connector_class", ""),
            cluster_id=r.metadata.get("cluster_id", ""),
            environment_id=r.parent_id or "",
            owner_id=r.owner_id or "",
            is_deleted=r.status == ResourceStatus.DELETED,
        )
