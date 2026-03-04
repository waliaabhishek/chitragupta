from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.models import Resource


@dataclass(frozen=True)
class CCloudSharedContext:
    """Pre-gathered shared context for a single gather cycle.

    Created once per gather cycle by ConfluentCloudPlugin.build_shared_context()
    and passed to every handler's gather_resources() call. Eliminates implicit
    UoW-mediated dependencies between handlers.

    Uses tuple fields (not list) to enforce true immutability — frozen=True
    only prevents field reassignment, not mutation of mutable containers.
    Derived properties are precomputed in __post_init__ to avoid repeated list
    comprehensions on every access.
    """

    environment_resources: tuple[Resource, ...]
    kafka_cluster_resources: tuple[Resource, ...]
    # Precomputed derived fields — populated by __post_init__
    _env_ids: tuple[str, ...] = field(init=False, compare=False, hash=False)
    _kafka_cluster_pairs: tuple[tuple[str, str], ...] = field(init=False, compare=False, hash=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_env_ids", tuple(r.resource_id for r in self.environment_resources))
        object.__setattr__(
            self,
            "_kafka_cluster_pairs",
            tuple((r.parent_id or "", r.resource_id) for r in self.kafka_cluster_resources),
        )

    @property
    def env_ids(self) -> list[str]:
        return list(self._env_ids)

    @property
    def kafka_cluster_pairs(self) -> list[tuple[str, str]]:
        """Return (env_id, cluster_id) tuples for gather_connectors."""
        return list(self._kafka_cluster_pairs)
