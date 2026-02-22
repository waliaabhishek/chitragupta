from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class Identity:
    """An identity (user, service account, etc.) within an ecosystem."""

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
