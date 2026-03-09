from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, TypedDict, runtime_checkable

logger = logging.getLogger(__name__)

# Types that represent "owners" (not API keys or system identities)
OWNER_IDENTITY_TYPES: tuple[str, ...] = ("service_account", "user", "identity_pool", "principal")


class FlinkContextDict(TypedDict, total=False):
    """TypedDict for Flink handler's IdentityResolution.context.

    Used by flink_cfu_allocator to distribute costs by statement owner.
    """

    stmt_owner_cfu: dict[str, float]
    """Maps owner identity_id to total CFU usage."""


@runtime_checkable
class Identity(Protocol):
    """Protocol for an identity (user, service account, etc.) within an ecosystem."""

    @property
    def ecosystem(self) -> str: ...

    @property
    def tenant_id(self) -> str: ...

    @property
    def identity_id(self) -> str: ...

    @property
    def identity_type(self) -> str: ...

    @property
    def display_name(self) -> str | None: ...

    @property
    def created_at(self) -> datetime | None: ...

    @property
    def deleted_at(self) -> datetime | None: ...

    @property
    def last_seen_at(self) -> datetime | None: ...

    @property
    def metadata(self) -> dict[str, Any]: ...


@dataclass
class CoreIdentity:
    """Core implementation of the Identity Protocol."""

    ecosystem: str
    tenant_id: str
    identity_id: str
    identity_type: str
    display_name: str | None = None
    created_at: datetime | None = None
    deleted_at: datetime | None = None
    last_seen_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class IdentitySet:
    """Dict-backed set of identities keyed by identity_id."""

    def __init__(self) -> None:
        self._entries: dict[str, Identity] = {}

    def add(self, identity: Identity) -> None:
        """Add an identity. Last-write-wins on duplicate identity_id."""
        self._entries[identity.identity_id] = identity

    def get(self, identity_id: str) -> Identity | None:
        """Get an identity by ID, or None if not present."""
        return self._entries.get(identity_id)

    def ids(self) -> frozenset[str]:
        """Return all identity IDs as a frozenset."""
        return frozenset(self._entries.keys())

    def ids_by_type(self, *types: str) -> frozenset[str]:
        """Return identity IDs matching the given types."""
        return frozenset(i.identity_id for i in self._entries.values() if i.identity_type in types)

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[Identity]:
        return iter(self._entries.values())

    def __bool__(self) -> bool:
        return len(self._entries) > 0

    def __contains__(self, identity_id: str) -> bool:
        return identity_id in self._entries


@dataclass
class IdentityResolution:
    """Three-scope identity resolution result."""

    resource_active: IdentitySet
    metrics_derived: IdentitySet
    tenant_period: IdentitySet
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def merged_active(self) -> IdentitySet:
        """resource_active union metrics_derived. Metrics_derived wins on overlap."""
        merged = IdentitySet()
        for identity in self.resource_active:
            merged.add(identity)
        for identity in self.metrics_derived:
            merged.add(identity)
        return merged
